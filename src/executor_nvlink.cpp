#include "executor_nvlink.h"

#include <emmintrin.h>

#include <array>
#include <utility>

#include "cuda_util.h"
#include "pickle_logger.h"

namespace pickle {

using namespace std::chrono_literals;

NvlinkSender::NvlinkSender(
    int device,
    ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) noexcept :
    device_(device),
    send_ticket_queue_(std::move(send_ticket_queue)),
    ack_queue_(std::move(ack_queue)) {}

std::shared_ptr<NvlinkSender> NvlinkSender::create(
    int device,
    ipc::SPSCQueue<NvlinkSendTicket>&& send_ticket_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) noexcept {
    return std::shared_ptr<NvlinkSender>(new NvlinkSender(device, std::move(send_ticket_queue), std::move(ack_queue)));
}

std::shared_ptr<Event>
NvlinkSender::send(uint32_t unique_id, uint64_t offset, uint32_t length, cudaIpcMemHandle_t handle) {
    auto event = Event::create();

    NvlinkSendTicket ticket {};
    ticket.handle = handle;
    ticket.offset = offset;
    ticket.unique_id = unique_id;
    ticket.length = length;

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

// ==================== NvlinkRecver ====================

NvlinkRecver::NvlinkRecver(
    int device,
    ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue,
    ipc::SPSCQueue<NvlinkAck>&& ack_queue
) :
    device_(device),
    recv_ticket_queue_(std::move(recv_queue)),
    ack_queue_(std::move(ack_queue)) {}

NvlinkRecver::~NvlinkRecver() {
    if (this->stream_ != nullptr) {
        this->cleanup_inflight();
        CUDA_CHECK(cudaStreamDestroy(this->stream_));
    }
}

std::shared_ptr<NvlinkRecver>
NvlinkRecver::create(int device, ipc::SPSCQueue<NvlinkSendTicket>&& recv_queue, ipc::SPSCQueue<NvlinkAck>&& ack_queue) {
    return std::shared_ptr<NvlinkRecver>(new NvlinkRecver(device, std::move(recv_queue), std::move(ack_queue)));
}

std::shared_ptr<Event> NvlinkRecver::recv(uint32_t unique_id, uint64_t addr, uint32_t length) {
    auto event = Event::create();

    NvlinkRecvRequest req {};
    req.unique_id = unique_id;
    req.length = length;
    req.addr = addr;
    req.device = this->device_;

    NvlinkRecvCommand cmd {.request = req, .event = event};
    PICKLE_ASSERT(this->recv_request_command_queue_.enqueue(std::move(cmd)));

    return event;
}

void NvlinkRecver::ensure_stream() {
    if (this->stream_ != nullptr) {
        return;
    }
    CUDA_CHECK(cudaSetDevice(this->device_));
    CUDA_CHECK(cudaStreamCreateWithFlags(&this->stream_, cudaStreamNonBlocking));
}

void NvlinkRecver::start_copy(const NvlinkSendTicket& ticket, NvlinkRecvCommand&& cmd) {
    this->ensure_stream();

    PICKLE_ASSERT(cmd.request.device == this->device_);

    // 打开远端的 cudaIpcMemHandle，获得本进程可见的 device pointer。
    void* remote_base_ptr = nullptr;
    CUDA_CHECK(cudaIpcOpenMemHandle(&remote_base_ptr, ticket.handle, cudaIpcMemLazyEnablePeerAccess));

    uint64_t raddr = reinterpret_cast<uint64_t>(remote_base_ptr) + ticket.offset;
    uint64_t laddr = reinterpret_cast<uint64_t>(cmd.request.addr);
    CUDA_CHECK(cudaMemcpyAsync(
        reinterpret_cast<void*>(laddr),
        reinterpret_cast<void*>(raddr),
        ticket.length,
        cudaMemcpyDeviceToDevice,
        this->stream_
    ));

    cudaEvent_t completion_event;
    CUDA_CHECK(cudaEventCreateWithFlags(&completion_event, cudaEventDisableTiming));
    CUDA_CHECK(cudaEventRecord(completion_event, this->stream_));

    this->inflight_copies_.push_back(
        InflightCopy {
            .cuda_event = completion_event,
            .pickle_event = cmd.event,
            .remote_base_ptr = remote_base_ptr,
            .unique_id = ticket.unique_id,
        }
    );
}

void NvlinkRecver::check_inflight_copies() {
    while (!this->inflight_copies_.empty()) {
        auto& front = this->inflight_copies_.front();
        auto status = cudaEventQuery(front.cuda_event);
        if (status == cudaErrorNotReady) {
            break;
        }

        CUDA_CHECK(status);
        CUDA_CHECK(cudaEventDestroy(front.cuda_event));
        CUDA_CHECK(cudaIpcCloseMemHandle(front.remote_base_ptr));

        if (front.pickle_event) {
            front.pickle_event->notify();
        }

        NvlinkAck ack {front.unique_id};
        this->ack_queue_.try_push(ack).unwrap();
        this->inflight_copies_.pop_front();
    }
}

void NvlinkRecver::cleanup_inflight() {
    for (auto& inflight : this->inflight_copies_) {
        cudaEventDestroy(inflight.cuda_event);
        cudaIpcCloseMemHandle(inflight.remote_base_ptr);
    }
    this->inflight_copies_.clear();
}

void NvlinkRecver::poll() noexcept {
    // 1. 从 SPSCQueue 中取出 remote send ticket
    while (true) {
        auto res = this->recv_ticket_queue_.try_pop();
        if (res.is_err()) {
            break;
        }
        auto ticket = std::move(res.unwrap());
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
            NvlinkRecvCommand command = std::move(local_queue.front());
            local_queue.pop();

            PICKLE_ASSERT(
                ticket.length == command.request.length,
                "NvlinkRecver::poll() length mismatch: send_length={}, recv_length={}",
                ticket.length,
                command.request.length
            );

            this->start_copy(ticket, std::move(command));
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

    // 4. 检查 in-flight copy 的完成状态
    this->check_inflight_copies();
}

}  // namespace pickle
