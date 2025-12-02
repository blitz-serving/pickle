#pragma once

#include <cuda_runtime.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "executor_common.h"
#include "spsc.h"

#define CUDA_CHECK(expr)                                                                               \
    do {                                                                                               \
        cudaError_t err = expr;                                                                        \
        if (err != cudaSuccess) {                                                                      \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__, cudaGetErrorString(err)); \
            exit(1);                                                                                   \
        }                                                                                              \
    } while (0)

namespace pickle {

// ==================== NVLink 侧控制面数据结构 ====================

// Sender -> Receiver：描述一次 NVLink 发送的 ticket。
// 注意：必须是 trivially copyable，才能放进 ipc::SPSCQueue。
struct alignas(64) NvlinkSendTicket {
    uint32_t unique_id;
    uint32_t length;  // 以字节为单位
    int src_device;  // 源 GPU id
    int dst_device;  // 目标 GPU id
    cudaIpcMemHandle_t src_handle;  // 源进程导出的 cudaIpcMemHandle

    std::string to_string() const {
        return std::format(
            "{{ unique_id: {}, length: 0x{:x}, src_device: {}, dst_device: {} }}",
            unique_id,
            length,
            src_device,
            dst_device
        );
    }
};

// Receiver -> Sender：copy 完成之后的 ACK，同样要求 trivially copyable。
struct alignas(8) NvlinkAck {
    uint32_t unique_id;
};

// 本地 recv() 请求的描述，生命周期仅限本进程。
struct NvlinkRecvRequest {
    uint32_t unique_id;
    uint32_t length;
    void* dst_ptr;  // 目标 GPU buffer 指针（本进程可见）
    int dst_device;  // 目标 GPU id
};

// 应用侧 recv() 排队到运行时的 command（带 Event）
struct NvlinkRecvCommand {
    NvlinkRecvRequest request;
    std::shared_ptr<Event> event;
};

// 由 Receiver 的 poll() 生成的 copy task，投递到后台 copy 线程。
// 这里只保存本进程需要的信息：remote ticket + local request + 本地 Event。
struct NvlinkCopyTask {
    NvlinkSendTicket remote_ticket;
    NvlinkRecvRequest local_request;
    std::shared_ptr<Event> local_event;
};

// ==================== NVLink Sender：发送方执行器 ====================
//
// 角色：
// - 应用调用 send() -> 把 (unique_id, src_ptr, length) 注册成 cudaIpcMemHandle
//   -> 推入跨进程 SPSCQueue (send_ticket_queue_) -> 等待 ACK。
// - 后台 poll() 负责从 ack_queue_ 中取出 NvlinkAck，找到对应 Event 并 notify()。

class NvlinkSender {
private:
    int src_device_;
    int dst_device_;

    // Sender -> Receiver：发送 ticket 的 SPSCQueue（本进程为 producer）
    ipc::SPSCQueue<NvlinkSendTicket> send_ticket_queue_;
    // Receiver -> Sender：发送 ACK 的 SPSCQueue（本进程为 consumer）
    ipc::SPSCQueue<NvlinkAck> ack_queue_;

    // 以 unique_id 为 key 的 pending event
    std::map<uint32_t, std::shared_ptr<Event>> pending_send_event_map_;

    NvlinkSender(
        int src_device,
        int dst_device,
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
        int dst_device,
        ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
        ipc::SPSCQueue<NvlinkAck>&& ack_queue
    ) noexcept;

    // 应用接口：发起一次 NVLink 发送
    [[nodiscard]] std::shared_ptr<Event> send(uint32_t unique_id, void* src_ptr, uint32_t length);

    // 后台执行器：从 ACK 队列中取出完成的 unique_id，并通知 Event。
    // SAFETY: !! Not thread-safe, 单线程调用 !!
    void poll() noexcept;
};

// ==================== NVLink Copy Executor：后台拷贝线程 ====================
//
// 角色：
// - 绑定到某个 GPU（local_device_） 的 CUDA context；
// - 从 copy_task_queue_ 中取出 NvlinkCopyTask；
// - 调用 cudaIpcOpenMemHandle 将 remote_ticket.src_handle 映射为当前进程可见的 ptr；
// - 使用 local_device_ 上的 copy engine 做 cudaMemcpyAsync(local_dst <- remote_src)；
// - 等待 copy 完成后，通知本地 Event，并通过 ack_queue_ 发送 ACK。

class NvlinkCopyExecutor {
private:
    int local_device_;
    Queue<NvlinkCopyTask>& copy_task_queue_;
    ipc::SPSCQueue<NvlinkAck>& ack_queue_;

    std::atomic<bool> stop_flag_ {false};
    std::thread worker_;
    cudaStream_t stream_ {nullptr};

    void run() noexcept;

public:
    NvlinkCopyExecutor(int local_device, Queue<NvlinkCopyTask>& copy_task_queue, ipc::SPSCQueue<NvlinkAck>& ack_queue);

    NvlinkCopyExecutor(const NvlinkCopyExecutor&) = delete;
    NvlinkCopyExecutor& operator=(const NvlinkCopyExecutor&) = delete;
    NvlinkCopyExecutor(NvlinkCopyExecutor&&) = delete;
    NvlinkCopyExecutor& operator=(NvlinkCopyExecutor&&) = delete;

    ~NvlinkCopyExecutor();
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
    int dst_device_;

    // Sender -> Receiver：来自远端的 send ticket（本进程为 consumer）
    ipc::SPSCQueue<NvlinkSendTicket> recv_ticket_queue_;
    // Receiver -> Sender：发送 ACK 的 SPSCQueue（本进程为 producer）
    ipc::SPSCQueue<NvlinkAck> ack_queue_;

    // 应用调用 recv() 时将命令投递到这里
    Queue<NvlinkRecvCommand> recv_request_command_queue_;

    // pending 队列 / map：用于匹配 (remote send, local recv)
    MultiMap<NvlinkSendTicket> pending_remote_send_map_;
    MultiMap<NvlinkRecvCommand> pending_local_recv_map_;

    // copy task 队列 & 后台拷贝执行器
    Queue<NvlinkCopyTask> copy_task_queue_;
    NvlinkCopyExecutor copy_executor_;

    NvlinkRecver(int dst_device, ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue, ipc::SPSCQueue<NvlinkAck>&& ack_queue);

public:
    NvlinkRecver() = delete;
    NvlinkRecver(const NvlinkRecver&) = delete;
    NvlinkRecver& operator=(const NvlinkRecver&) = delete;
    NvlinkRecver(NvlinkRecver&&) = delete;
    NvlinkRecver& operator=(NvlinkRecver&&) = delete;

    ~NvlinkRecver() = default;

    static std::shared_ptr<NvlinkRecver>
    create(int dst_device, ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue, ipc::SPSCQueue<NvlinkAck>&& ack_queue);

    // 应用接口：发起一次 NVLink 接收
    [[nodiscard]] std::shared_ptr<Event> recv(uint32_t unique_id, void* dst_ptr, uint32_t length);

    // 后台执行器：从 SPSCQueue 和本地队列取数据并做匹配，生成 copy task。
    // SAFETY: !! Not thread-safe, 单线程调用 !!
    void poll() noexcept;
};

}  // namespace pickle
