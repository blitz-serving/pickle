#pragma once

#include <cuda_runtime.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <map>
#include <memory>

#include "executor_common.h"
#include "spsc.h"

namespace pickle {

// ==================== NVLink 侧控制面数据结构 ====================

struct NvlinkHandle {
    cudaIpcMemHandle_t handle;
    uint64_t addr;
    uint64_t length;
};

// Sender -> Receiver：描述一次 NVLink 发送的 ticket。
// 注意：必须是 trivially copyable，才能放进 ipc::SPSCQueue。
struct alignas(64) NvlinkSendTicket {
    cudaIpcMemHandle_t handle;
    uint64_t offset;
    uint32_t unique_id;
    uint32_t length;
};

// Receiver -> Sender：copy 完成之后的 ACK，同样要求 trivially copyable。
struct alignas(8) NvlinkAck {
    uint32_t unique_id;
};

// 本地 recv() 请求的描述，生命周期仅限本进程。
struct NvlinkRecvRequest {
    uint32_t unique_id;
    uint32_t length;
    uint64_t addr;
    int32_t device;
};

// 应用侧 recv() 排队到运行时的 command（带 Event）
struct NvlinkRecvCommand {
    NvlinkRecvRequest request;
    std::shared_ptr<Event> event;
};

// ==================== NVLink Sender：发送方执行器 ====================
//
// 角色：
// - 应用调用 send() -> 把 (unique_id, src_ptr, length, handle) 推入跨进程 SPSCQueue
//   (handle 由应用通过 cudaIpcGetMemHandle 预先获取) -> 等待 ACK。
// - 后台 poll() 负责从 ack_queue_ 中取出 NvlinkAck，找到对应 Event 并 notify()。

class NvlinkSender {
private:
    int device_;

    // Sender -> Receiver：发送 ticket 的 SPSCQueue（本进程为 producer）
    ipc::SPSCQueue<NvlinkSendTicket> send_ticket_queue_;
    // Receiver -> Sender：发送 ACK 的 SPSCQueue（本进程为 consumer）
    ipc::SPSCQueue<NvlinkAck> ack_queue_;

    // 以 unique_id 为 key 的 pending event
    std::map<uint32_t, std::shared_ptr<Event>> pending_send_event_map_;

    NvlinkSender(
        int device,
        ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
        ipc::SPSCQueue<NvlinkAck>&& ack_queue
    ) noexcept;

public:
    NvlinkSender() = delete;
    NvlinkSender(const NvlinkSender&) = delete;
    NvlinkSender& operator=(const NvlinkSender&) = delete;
    NvlinkSender(NvlinkSender&&) = delete;
    NvlinkSender& operator=(NvlinkSender&&) = delete;
    ~NvlinkSender() = default;

    // 工厂函数：由上层在完成 SPSCQueue 建立后调用。
    static std::shared_ptr<NvlinkSender> create(
        int src_device,
        ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
        ipc::SPSCQueue<NvlinkAck>&& ack_queue
    ) noexcept;

    [[nodiscard]] std::shared_ptr<Event>
    send(uint32_t unique_id, uint64_t offset, uint32_t length, cudaIpcMemHandle_t handle);

    // 后台执行器：从 ACK 队列中取出完成的 unique_id，并通知 Event。
    // SAFETY: !! Not thread-safe, 单线程调用 !!
    void poll() noexcept;
};

// ==================== NVLink Receiver：接收方执行器 ====================
//
// 角色：
// - 应用调用 recv() -> 将 NvlinkRecvCommand 投入本地 ConcurrentQueue；
// - poll():
//   1) 从 SPSCQueue 中取出 NvlinkSendTicket （remote send）；
//   2) 从本地 recv_request_command_queue_ 中取出 NvlinkRecvCommand （local recv）；
//   3) 基于 unique_id 做匹配，生成 NvlinkCopyTask 放入 copy_task_queue_；
// - 内部持有一个 NvlinkCopyExecutor，使用本 GPU 的 copy engine 做 cudaMemcpyAsync。

class NvlinkRecver {
private:
    int device_;

    // Sender -> Receiver：来自远端的 send ticket（本进程为 consumer）
    ipc::SPSCQueue<NvlinkSendTicket> recv_ticket_queue_;
    // Receiver -> Sender：发送 ACK 的 SPSCQueue（本进程为 producer）
    ipc::SPSCQueue<NvlinkAck> ack_queue_;

    // 应用调用 recv() 时将命令投递到这里
    Queue<NvlinkRecvCommand> recv_request_command_queue_;

    // pending 队列 / map：用于匹配 (remote send, local recv)
    MultiMap<NvlinkSendTicket> pending_remote_send_map_;
    MultiMap<NvlinkRecvCommand> pending_local_recv_map_;

    struct InflightCopy {
        cudaEvent_t cuda_event {nullptr};
        std::shared_ptr<Event> pickle_event;
        void* remote_base_ptr {nullptr};
        uint32_t unique_id {0};
    };

    cudaStream_t stream_ {nullptr};
    std::deque<InflightCopy> inflight_copies_;

    void start_copy(const NvlinkSendTicket& ticket, NvlinkRecvCommand&& command);
    void check_inflight_copies();
    void ensure_stream();
    void cleanup_inflight();

    NvlinkRecver(int device, ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue, ipc::SPSCQueue<NvlinkAck>&& ack_queue);

public:
    NvlinkRecver() = delete;
    NvlinkRecver(const NvlinkRecver&) = delete;
    NvlinkRecver& operator=(const NvlinkRecver&) = delete;
    NvlinkRecver(NvlinkRecver&&) = delete;
    NvlinkRecver& operator=(NvlinkRecver&&) = delete;

    ~NvlinkRecver();

    static std::shared_ptr<NvlinkRecver>
    create(int dst_device, ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue, ipc::SPSCQueue<NvlinkAck>&& ack_queue);

    // 应用接口：发起一次 NVLink 接收
    [[nodiscard]] std::shared_ptr<Event> recv(uint32_t unique_id, uint64_t addr, uint32_t length);

    // 后台执行器：从 SPSCQueue 和本地队列取数据并做匹配，生成 copy task。
    // SAFETY: !! Not thread-safe, 单线程调用 !!
    void poll() noexcept;
};

}  // namespace pickle
