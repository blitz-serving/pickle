#include "executor_nvlink.h"

#include <emmintrin.h>

#include <array>
#include <thread>
#include <utility>

#include "pickle_logger.h"

namespace pickle {

using namespace std;
using namespace std::chrono_literals;

NvlinkSender::NvlinkSender(
    int src_device,
    int dst_device,
    ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) noexcept :
    src_device_(src_device),
    dst_device_(dst_device),
    send_ticket_queue_(std::move(send_ticket_queue)),
    ack_queue_(std::move(ack_queue)) {}

std::shared_ptr<NvlinkSender> NvlinkSender::create(
    int src_device,
    int dst_device,
    ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) noexcept {
    return std::shared_ptr<NvlinkSender>(
        new NvlinkSender(src_device, dst_device, std::move(send_ticket_queue), std::move(ack_queue))
    );
}

std::shared_ptr<Event> NvlinkSender::send(uint32_t unique_id, void* src_ptr, uint32_t length) {
    auto event = Event::create();

    NvlinkSendTicket ticket {};
    ticket.unique_id = unique_id;
    ticket.length = length;
    ticket.src_device = this->src_device_;
    ticket.dst_device = this->dst_device_;

    // 假设调用方已经在正确的 src_device 上设置了 CUDA context。
    // 若需要更强的封装，可以在外面绑定线程或手动调用 cudaSetDevice。
    CUDA_CHECK(cudaIpcGetMemHandle(&ticket.src_handle, src_ptr));

    // TODO: what if the queue is full ?
    this->send_ticket_queue_.try_push(ticket).unwrap();

    // 记录 pending event，用于之后 ACK 时唤醒
    PICKLE_ASSERT(
        this->pending_send_event_map_.emplace(unique_id, event).second,
        "NvlinkSender: duplicated unique_id={}",
        unique_id
    );

    return event;
}

void NvlinkSender::poll() noexcept {
    while (true) {
        auto res = this->ack_queue_.try_pop();
        if (res.is_err()) {
            break;
        }

        NvlinkAck ack = std::move(res.unwrap());
        auto iter = this->pending_send_event_map_.find(ack.unique_id);
        PICKLE_ASSERT(
            iter != this->pending_send_event_map_.end(),
            "NvlinkSender::poll() received unknown ACK unique_id={}",
            ack.unique_id
        );

        iter->second->notify();
        this->pending_send_event_map_.erase(iter);
    }
}

// ==================== NvlinkCopyExecutor ====================

NvlinkCopyExecutor::NvlinkCopyExecutor(
    int local_device,
    Queue<NvlinkCopyTask>& copy_task_queue,
    ipc::SPSCQueue<NvlinkAck>& ack_queue
) :
    local_device_(local_device),
    copy_task_queue_(copy_task_queue),
    ack_queue_(ack_queue) {
    // 后台线程启动
    this->worker_ = std::thread([this]() { this->run(); });
}

NvlinkCopyExecutor::~NvlinkCopyExecutor() {
    this->stop_flag_.store(true, std::memory_order_release);
    if (this->worker_.joinable()) {
        this->worker_.join();
    }
}

void NvlinkCopyExecutor::run() noexcept {
    // 绑定本线程的 CUDA context
    CUDA_CHECK(cudaSetDevice(this->local_device_));
    CUDA_CHECK(cudaStreamCreateWithFlags(&this->stream_, cudaStreamNonBlocking));

    cudaEvent_t completion_event;
    CUDA_CHECK(cudaEventCreateWithFlags(&completion_event, cudaEventDisableTiming));

    while (!this->stop_flag_.load(std::memory_order_acquire)) {
        NvlinkCopyTask task;
        if (!this->copy_task_queue_.try_dequeue(task)) {
            // 没有任务，稍微睡一下，避免忙等
            _mm_pause();
            continue;
        }

        const auto& ticket = task.remote_ticket;
        const auto& req = task.local_request;

        // 当前 copy 线程只负责 local_device_ 这块 GPU。
        // 按设计，req.dst_device 应该等于 local_device_。
        PICKLE_ASSERT(
            req.dst_device == this->local_device_,
            "NvlinkCopyExecutor::run() dst_device mismatch: req.dst_device={}, local_device_={}",
            req.dst_device,
            this->local_device_
        );

        // 打开远端的 cudaIpcMemHandle，获得本进程可见的 device pointer。
        void* remote_ptr = nullptr;
        CUDA_CHECK(cudaIpcOpenMemHandle(&remote_ptr, ticket.src_handle, cudaIpcMemLazyEnablePeerAccess));

        // 在本 GPU 的 copy engine 上做 DeviceToDevice 复制（SM-free）
        CUDA_CHECK(cudaMemcpyAsync(req.dst_ptr, remote_ptr, ticket.length, cudaMemcpyDeviceToDevice, this->stream_));

        // 通过 cudaEvent 等待这一批 copy 完成。
        CUDA_CHECK(cudaEventRecord(completion_event, this->stream_));
        CUDA_CHECK(cudaEventSynchronize(completion_event));

        // 关闭 Ipc handle
        CUDA_CHECK(cudaIpcCloseMemHandle(remote_ptr));

        // 通知本地应用：对应的 recv() 已经完成
        if (task.local_event) {
            task.local_event->notify();
        }

        // 通过 ACK 队列告诉远端 Sender：unique_id 已经完成
        NvlinkAck ack {ticket.unique_id};
        this->ack_queue_.try_push(ack).unwrap();
    }

    CUDA_CHECK(cudaEventDestroy(completion_event));
    CUDA_CHECK(cudaStreamDestroy(this->stream_));
}

// ==================== NvlinkRecver ====================

NvlinkRecver::NvlinkRecver(
    int dst_device,
    ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) :
    dst_device_(dst_device),
    recv_ticket_queue_(std::move(recv_queue)),
    ack_queue_(std::move(ack_queue)),
    copy_task_queue_(),
    copy_executor_(dst_device_, copy_task_queue_, this->ack_queue_) {}

std::shared_ptr<NvlinkRecver> NvlinkRecver::create(
    int dst_device,
    ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) {
    return std::shared_ptr<NvlinkRecver>(new NvlinkRecver(dst_device, std::move(recv_queue), std::move(ack_queue)));
}

std::shared_ptr<Event> NvlinkRecver::recv(uint32_t unique_id, void* dst_ptr, uint32_t length) {
    auto event = Event::create();

    NvlinkRecvRequest req {};
    req.unique_id = unique_id;
    req.length = length;
    req.dst_ptr = dst_ptr;
    req.dst_device = this->dst_device_;

    NvlinkRecvCommand cmd {.request = req, .event = event};
    PICKLE_ASSERT(this->recv_request_command_queue_.enqueue(std::move(cmd)));

    return event;
}

void NvlinkRecver::poll() noexcept {
    // 1. 从 SPSCQueue 中取出 remote send ticket
    while (true) {
        auto res = this->recv_ticket_queue_.try_pop();
        if (res.is_err()) {
            break;
        }

        auto ticket = std::move(res.unwrap());
        TRACE("NvlinkRecver::poll() got remote ticket: {}", ticket.to_string());
        this->pending_remote_send_map_[ticket.unique_id].push(std::move(ticket));
    }

    // 2. 从本地 ConcurrentQueue 中取出 recv() 命令
    std::array<NvlinkRecvCommand, kMagic> commands;
    uint64_t count_dequeued = this->recv_request_command_queue_.try_dequeue_bulk(commands.begin(), kMagic);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        auto& cmd = commands[i];
        uint32_t unique_id = cmd.request.unique_id;
        this->pending_local_recv_map_[unique_id].emplace(std::move(cmd));
    }

    // 3. 做匹配：对于每个 unique_id，把 remote send 和 local recv 成对匹配，生成 copy task
    for (auto it = this->pending_remote_send_map_.begin(); it != this->pending_remote_send_map_.end();) {
        uint32_t unique_id = it->first;
        auto& remote_queue = it->second;

        auto local_it = this->pending_local_recv_map_.find(unique_id);
        if (local_it == this->pending_local_recv_map_.end()) {
            ++it;
            continue;
        }

        auto& local_queue = local_it->second;

        while (!remote_queue.empty() && !local_queue.empty()) {
            NvlinkSendTicket ticket = std::move(remote_queue.front());
            remote_queue.pop();

            NvlinkRecvCommand cmd = std::move(local_queue.front());
            local_queue.pop();

            NvlinkCopyTask task {
                .remote_ticket = ticket,
                .local_request = cmd.request,
                .local_event = cmd.event,
            };

            TRACE(
                "NvlinkRecver::poll() enqueue copy task: unique_id={}, length=0x{:x}",
                ticket.unique_id,
                ticket.length
            );
            PICKLE_ASSERT(this->copy_task_queue_.enqueue(std::move(task)));
        }

        // 清理空队列，避免 map 无限增长
        if (remote_queue.empty()) {
            it = this->pending_remote_send_map_.erase(it);
        } else {
            ++it;
        }

        if (local_queue.empty()) {
            this->pending_local_recv_map_.erase(local_it);
        }
    }
}

}  // namespace pickle

#undef CUDA_CHECK