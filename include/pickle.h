#pragma once

#include <infiniband/verbs.h>
#include <x86intrin.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "concurrentqueue.h"
#include "pickle_logger.h"
#include "rdma_util.h"

namespace pickle {

using ::rdma_util::CompletionQueue;
using ::rdma_util::MemoryRegion;
using ::rdma_util::ProtectionDomain;
using ::rdma_util::RcQueuePair;

static const uint64_t kMagic = 32;

template<typename T>
using Queue = moodycamel::ConcurrentQueue<T>;

struct alignas(32) Ticket {
    uint32_t stream_id;
    uint32_t length;
    uint32_t key;
    uint64_t addr;

    std::string to_string() const {
        return std::format("{{ stream_id: {}, key: {}, length: 0x{:x}, addr: 0x{:x} }}", stream_id, key, length, addr);
    }
};

template<typename T>
using MultiMap = std::map<uint32_t, std::queue<T>>;

class Event {
private:
    std::atomic<bool> finished_ = false;

public:
    Event() = default;
    Event(const Event&) = delete;
    Event& operator=(const Event&) = delete;
    Event(Event&&) = delete;
    Event& operator=(Event&&) = delete;
    ~Event() = default;

    static std::shared_ptr<Event> create() {
        return std::make_shared<Event>();
    }

    bool is_notified() const {
        return this->finished_.load(std::memory_order_acquire);
    }

    void notify() {
        this->finished_.store(true, std::memory_order_release);
#ifndef BUSY_WAIT
        this->finished_.notify_all();
#endif
    }

    void wait() {
#ifndef BUSY_WAIT
        this->finished_.wait(false, std::memory_order_acquire);
#else
        while (!this->finished_.load(std::memory_order_acquire)) {
            _mm_pause();
        }
#endif
    }
};

struct Command {
    Ticket ticket;
    std::shared_ptr<Event> event;
};

class PickleSender {
private:
    uint32_t packet_size_;

    std::queue<Ticket> remote_recv_request_queue_;
    Queue<Command> send_request_command_queue_;
    MultiMap<Ticket> pending_remote_recv_request_map_;
    MultiMap<Ticket> pending_local_send_request_map_;
    MultiMap<std::shared_ptr<Event>> pending_local_send_event_map_;

    std::shared_ptr<RcQueuePair> qp_;
    uint64_t wr_occupied_;
    std::vector<ibv_send_wr> send_wr_list_;
    std::vector<ibv_sge> send_sge_list_;

    std::vector<ibv_wc> polled_send_wcs_;
    std::vector<ibv_wc> polled_recv_wcs_;

    std::unique_ptr<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    PickleSender(std::unique_ptr<RcQueuePair> qp, uint64_t packet_size) noexcept(false);

public:
    PickleSender() = delete;
    PickleSender(const PickleSender&) = delete;
    PickleSender& operator=(const PickleSender&) = delete;
    PickleSender(PickleSender&&) = delete;
    PickleSender& operator=(PickleSender&&) = delete;
    ~PickleSender() = default;

    static std::shared_ptr<PickleSender>
    create(std::unique_ptr<RcQueuePair> qp, uint64_t packet_size = 256 * 1024) noexcept(false);

    [[nodiscard]] std::shared_ptr<Event> send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey);

    /**
     * @brief The executor of the PickleSender.
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);
};

struct FlushInfo {
    uint32_t rkey;
    uint64_t raddr;
    std::shared_ptr<Event> event;
};

class Flusher {
private:
    std::unique_ptr<RcQueuePair> loopback_qp_;
    std::unique_ptr<MemoryRegion> loopback_buffer_;
    std::vector<ibv_wc> polled_wcs_;

    uint64_t pending_flushes_;
    std::queue<std::shared_ptr<Event>> flushing_queue_;
    std::vector<FlushInfo> flush_infos_;
    Queue<FlushInfo> info_queue_;

    Flusher(std::shared_ptr<ProtectionDomain>& pd) noexcept(false);

public:
    Flusher() = delete;
    Flusher(const Flusher&) = delete;
    Flusher& operator=(const Flusher&) = delete;
    Flusher(Flusher&&) = delete;
    Flusher& operator=(Flusher&&) = delete;
    ~Flusher() = default;

    static std::unique_ptr<Flusher> create(std::shared_ptr<ProtectionDomain> pd) noexcept(false);

    void append(uint32_t rkey, uint64_t raddr, std::shared_ptr<Event> event) {
        TRACE("pickle::Flusher::append() append FlushInfo: rkey={}, raddr={}", rkey, raddr);
        PICKLE_ASSERT(this->info_queue_.enqueue(FlushInfo {.rkey = rkey, .raddr = raddr, .event = std::move(event)}));
    }

    /**
     * @brief The executor of the Flusher.
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);
};

class PickleRecver {
private:
    uint64_t count_pending_requests_;
    std::queue<Ticket> pending_local_recv_request_queue_;
    std::queue<uint64_t> free_slots;
    Queue<Command> recv_request_command_queue_;
    MultiMap<Command> pending_local_recv_request_map_;

    std::shared_ptr<RcQueuePair> qp_;
    std::vector<ibv_wc> polled_send_wcs_;
    std::vector<ibv_wc> polled_recv_wcs_;

    std::unique_ptr<MemoryRegion> host_send_buffer_;
    uint64_t send_buffer_addr_;
    uint32_t send_buffer_lkey_;

    std::unique_ptr<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    std::shared_ptr<Flusher> flusher_;

    PickleRecver(std::unique_ptr<RcQueuePair> qp, std::shared_ptr<Flusher> flusher) noexcept(false);

public:
    PickleRecver() = delete;
    PickleRecver(const PickleRecver&) = delete;
    PickleRecver& operator=(const PickleRecver&) = delete;
    PickleRecver(PickleRecver&&) = delete;
    PickleRecver& operator=(PickleRecver&&) = delete;
    ~PickleRecver() = default;

    static std::shared_ptr<PickleRecver>
    create(std::unique_ptr<RcQueuePair> qp, std::shared_ptr<Flusher> flusher = nullptr) noexcept(false);

    [[nodiscard]] std::shared_ptr<Event> recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey);

    /**
     * @brief The executor of the PickleRecver
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);
};

}  // namespace pickle
