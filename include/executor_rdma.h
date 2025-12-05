#pragma once

#include <infiniband/verbs.h>
#include <linux/types.h>

#include <cstdint>
#include <cstdlib>
#include <format>
#include <memory>
#include <queue>
#include <vector>

#include "executor_common.h"
#include "pickle_logger.h"
#include "rdma_util.h"

namespace pickle {

using namespace std;
using ::rdma_util::CompletionQueue;
using ::rdma_util::MemoryRegion;
using ::rdma_util::ProtectionDomain;
using ::rdma_util::RcQueuePair;

struct alignas(32) RdmaTicket {
    uint32_t unique_id;
    uint32_t length;
    uint32_t key;
    uint64_t addr;

    string to_string() const {
        return std::format("{{ unique_id: {}, key: {}, length: 0x{:x}, addr: 0x{:x} }}", unique_id, key, length, addr);
    }
};

struct RdmaCommand {
    RdmaTicket ticket;
    shared_ptr<Event> event;
};

class RdmaSender {
private:
    uint64_t packet_size_;

    queue<RdmaTicket> remote_recv_request_queue_;
    Queue<RdmaCommand> send_request_command_queue_;
    MultiMap<RdmaTicket> pending_remote_recv_request_map_;
    MultiMap<RdmaTicket> pending_local_send_request_map_;
    MultiMap<shared_ptr<Event>> pending_local_send_event_map_;

    shared_ptr<RcQueuePair> qp_;
    uint64_t wr_occupied_;
    vector<ibv_send_wr> send_wr_list_;
    vector<ibv_sge> send_sge_list_;

    vector<ibv_wc> polled_send_wcs_;
    vector<ibv_wc> polled_recv_wcs_;

    unique_ptr<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    void drain_remote_recv_requests();
    void drain_local_send_requests();
    void build_and_post();
    void handle_completions();

    RdmaSender(unique_ptr<RcQueuePair> qp, uint64_t packet_size) noexcept(false);

public:
    RdmaSender() = delete;
    RdmaSender(const RdmaSender&) = delete;
    RdmaSender& operator=(const RdmaSender&) = delete;
    RdmaSender(RdmaSender&&) = delete;
    RdmaSender& operator=(RdmaSender&&) = delete;
    ~RdmaSender() = default;

    static shared_ptr<RdmaSender> create(unique_ptr<RcQueuePair> qp, uint64_t packet_size = 256 * 1024) noexcept(false);

    [[nodiscard]] shared_ptr<Event> send(uint32_t unique_id, uint64_t addr, uint32_t length, uint32_t lkey);

    /**
     * @brief The executor of the PickleSender.
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);
};

struct FlushInfo {
    uint32_t rkey;
    uint64_t raddr;
    shared_ptr<Event> event;
};

class RdmaFlusher {
private:
    unique_ptr<RcQueuePair> loopback_qp_;
    unique_ptr<MemoryRegion> loopback_buffer_;
    vector<ibv_wc> polled_wcs_;

    uint64_t pending_flushes_;
    queue<shared_ptr<Event>> flushing_queue_;
    vector<FlushInfo> flush_infos_;
    Queue<FlushInfo> info_queue_;

    RdmaFlusher(shared_ptr<ProtectionDomain>& pd) noexcept(false);

public:
    RdmaFlusher() = delete;
    RdmaFlusher(const RdmaFlusher&) = delete;
    RdmaFlusher& operator=(const RdmaFlusher&) = delete;
    RdmaFlusher(RdmaFlusher&&) = delete;
    RdmaFlusher& operator=(RdmaFlusher&&) = delete;
    ~RdmaFlusher() = default;

    static unique_ptr<RdmaFlusher> create(shared_ptr<ProtectionDomain> pd) noexcept(false);

    void append(uint32_t rkey, uint64_t raddr, shared_ptr<Event> event) {
        TRACE("pickle::Flusher::append() append FlushInfo: rkey={}, raddr={}", rkey, raddr);
        PICKLE_ASSERT(this->info_queue_.enqueue(FlushInfo {.rkey = rkey, .raddr = raddr, .event = std::move(event)}));
    }

    /**
     * @brief The executor of the Flusher.
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);
};

class RdmaRecver {
private:
    uint64_t count_pending_requests_;
    queue<RdmaTicket> pending_local_recv_request_queue_;
    queue<uint64_t> free_slots;
    Queue<RdmaCommand> recv_request_command_queue_;
    MultiMap<RdmaCommand> pending_local_recv_request_map_;

    shared_ptr<RcQueuePair> qp_;
    vector<ibv_wc> polled_send_wcs_;
    vector<ibv_wc> polled_recv_wcs_;

    unique_ptr<MemoryRegion> host_send_buffer_;
    uint64_t send_buffer_addr_;
    uint32_t send_buffer_lkey_;

    unique_ptr<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    shared_ptr<RdmaFlusher> flusher_;

    RdmaRecver(unique_ptr<RcQueuePair> qp, shared_ptr<RdmaFlusher> flusher) noexcept(false);

public:
    RdmaRecver() = delete;
    RdmaRecver(const RdmaRecver&) = delete;
    RdmaRecver& operator=(const RdmaRecver&) = delete;
    RdmaRecver(RdmaRecver&&) = delete;
    RdmaRecver& operator=(RdmaRecver&&) = delete;
    ~RdmaRecver() = default;

    static shared_ptr<RdmaRecver>
    create(unique_ptr<RcQueuePair> qp, shared_ptr<RdmaFlusher> flusher = nullptr) noexcept(false);

    [[nodiscard]] shared_ptr<Event> recv(uint32_t unique_id, uint64_t addr, uint32_t length, uint32_t rkey);

    /**
     * @brief The executor of the PickleRecver
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);
};

}  // namespace pickle
