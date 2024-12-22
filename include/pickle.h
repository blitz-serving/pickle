#ifndef _PICKLE_H_
#define _PICKLE_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <ios>
#include <map>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "concurrentqueue.h"
#include "rdma_util.h"

namespace pickle {

using namespace rdma_util;

template<typename T>
using Queue = moodycamel::ConcurrentQueue<T>;

struct Ticket {
    uint32_t stream_id;
    uint32_t length;
    uint64_t addr;
    uint32_t key;
    uint32_t padding_;

    inline std::string to_string() const {
        std::stringstream ss;
        ss << "stream_id: " << stream_id << std::hex << std::uppercase << ", length: " << length << ", addr: " << addr
           << ", key: " << key << ", padding: " << padding_;
        return ss.str();
    }
};

using Command = std::tuple<Ticket, Arc<std::atomic<bool>>>;

template<typename T>
using MultiMap = std::map<uint32_t, std::queue<T>>;

class Handle {
  private:
    Arc<std::atomic<bool>> finished_;

  public:
    Handle() : finished_(std::make_shared<std::atomic<bool>>(true)) {}

    Handle(const Arc<std::atomic<bool>>& finished) : finished_(finished) {}

    /**
     * @brief Check if the send/recv is finished.
     */
    inline bool is_finished() const {
        return this->finished_->load(std::memory_order_relaxed);
    }

    /**
     * @brief Wait until the operation is finished in a busy looping way.
     */
    inline void wait() const {
        while (!this->is_finished()) {
            std::this_thread::yield();
        }
    }
};

class PickleSender {
  private:
    uint64_t dop_;

    Arc<RcQueuePair> qp_;

    std::vector<ibv_wc> send_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_send_wcs_;

    std::vector<ibv_wc> recv_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_recv_wcs_;

    Queue<Command> send_request_command_queue_;
    Queue<Command> recv_request_command_queue_;

    Box<MemoryRegion> host_send_buffer_;
    uint64_t send_buffer_addr_;
    uint32_t send_buffer_lkey_;

    Box<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    Queue<Ticket> local_recv_request_queue_;
    Queue<Ticket> remote_recv_request_queue_;

    // Used in send_one_round
    std::queue<Ticket> pending_local_recv_request_queue_;
    MultiMap<Ticket> pending_remote_recv_request_map_;
    MultiMap<Ticket> pending_local_send_request_map_;
    MultiMap<Arc<std::atomic<bool>>> pending_local_send_flag_map_;
    std::queue<uint64_t> free_post_send_send_slots_;
    uint64_t post_send_write_slot_available_;
    uint64_t post_send_send_slot_available_;

    // Used in recv_one_round
    uint64_t pending_recv_request_count_;
    MultiMap<Arc<std::atomic<bool>>> pending_local_recv_request_map_;

    // Background polling
    bool background_polling_;
    std::thread polling_thread_;
    std::atomic<bool> polling_stopped_;

    PickleSender(const PickleSender&) = delete;
    PickleSender& operator=(const PickleSender&) = delete;

    void poll_both_inner() noexcept(false);
    void poll_send_one_round_inner() noexcept(false);
    void poll_recv_one_round_inner() noexcept(false);

  public:
    ~PickleSender();

    inline uint64_t get_dop() const {
        return this->dop_;
    }

    /**
     * @brief Poll both send and recv tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_both() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_recv_one_round_inner();
        this->poll_send_one_round_inner();
    }

    /**
     * @brief Poll send tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_send_one_round() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_send_one_round_inner();
    }

    /**
     * @brief Poll recv tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_recv_one_round() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_recv_one_round_inner();
    }

    static Arc<PickleSender>
    create(Box<RcQueuePair> qp, bool spawn_polling_thread = true, uint64_t dop = 16) noexcept(false);
    [[nodiscard]] Handle send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey, uint32_t padding = 0);

  private:
    PickleSender() = default;
    void initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false);
};

class PickleRecver {
  private:
    uint64_t dop_;

    Arc<RcQueuePair> qp_;

    std::vector<ibv_wc> send_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_send_wcs_;

    std::vector<ibv_wc> recv_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_recv_wcs_;

    Queue<Command> send_request_command_queue_;
    Queue<Command> recv_request_command_queue_;

    Box<MemoryRegion> host_send_buffer_;
    uint64_t send_buffer_addr_;
    uint32_t send_buffer_lkey_;

    Box<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    Queue<Ticket> local_recv_request_queue_;
    Queue<Ticket> remote_recv_request_queue_;

    // Used in send_one_round
    std::queue<Ticket> pending_local_recv_request_queue_;
    MultiMap<Ticket> pending_remote_recv_request_map_;
    MultiMap<Ticket> pending_local_send_request_map_;
    MultiMap<Arc<std::atomic<bool>>> pending_local_send_flag_map_;
    std::queue<uint64_t> free_post_send_send_slots_;
    uint64_t post_send_write_slot_available_;
    uint64_t post_send_send_slot_available_;

    // Used in recv_one_round
    uint64_t pending_recv_request_count_;
    MultiMap<Arc<std::atomic<bool>>> pending_local_recv_request_map_;

    // Background polling
    bool background_polling_;
    std::thread polling_thread_;
    std::atomic<bool> polling_stopped_;

    PickleRecver(const PickleRecver&) = delete;
    PickleRecver& operator=(const PickleRecver&) = delete;

    void poll_both_inner() noexcept(false);
    void poll_send_one_round_inner() noexcept(false);
    void poll_recv_one_round_inner() noexcept(false);

  public:
    ~PickleRecver();

    inline uint64_t get_dop() const {
        return this->dop_;
    }

    /**
     * @brief Poll both send and recv tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_both() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_recv_one_round_inner();
        this->poll_send_one_round_inner();
    }

    /**
     * @brief Poll send tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_send_one_round() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_send_one_round_inner();
    }

    /**
     * @brief Poll recv tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_recv_one_round() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_recv_one_round_inner();
    }

    static Arc<PickleRecver>
    create(Box<RcQueuePair> qp, bool spawn_polling_thread = true, uint64_t dop = 16) noexcept(false);
    [[nodiscard]] Handle recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey, uint32_t padding = 0);

  private:
    PickleRecver() = default;
    void initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false);
};

}  // namespace pickle

#endif  // _PICKLE_H_