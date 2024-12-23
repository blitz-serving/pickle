#ifndef _PICKLE_H_
#define _PICKLE_H_

#include <infiniband/verbs.h>

#include <map>
#include <memory>
#include <queue>
#include <sstream>
#include <string>

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

    inline std::string to_string() const {
        std::stringstream ss;
        ss << "stream_id: " << stream_id << std::hex << std::uppercase << ", length: " << length << ", addr: " << addr
           << ", key: " << key;
        return ss.str();
    }
};

using Command = std::tuple<Ticket, std::shared_ptr<std::atomic<bool>>>;

template<typename T>
using MultiMap = std::map<uint32_t, std::queue<T>>;

class Handle {
  private:
    std::shared_ptr<std::atomic<bool>> finished_;

  public:
    Handle() : finished_(std::make_shared<std::atomic<bool>>(true)) {}

    Handle(const std::shared_ptr<std::atomic<bool>>& finished) : finished_(finished) {}

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
    uint64_t chunk_size_;
    uint64_t wr_list_capacity_;
    uint64_t send_wr_list_index_;

    std::queue<Ticket> remote_recv_request_queue_;
    Queue<Command> send_request_command_queue_;
    MultiMap<Ticket> pending_remote_recv_request_map_;
    MultiMap<Ticket> pending_local_send_request_map_;
    MultiMap<std::shared_ptr<std::atomic<bool>>> pending_local_send_flag_map_;

    std::shared_ptr<RcQueuePair> qp_;
    std::vector<ibv_send_wr> send_wr_list_;
    std::vector<ibv_sge> send_sge_list_;

    std::vector<ibv_wc> send_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_send_wcs_;
    std::vector<ibv_wc> recv_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_recv_wcs_;

    std::unique_ptr<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    PickleSender() = delete;
    PickleSender(const PickleSender&) = delete;
    PickleSender& operator=(const PickleSender&) = delete;
    PickleSender(PickleSender&&) = delete;
    PickleSender& operator=(PickleSender&&) = delete;

  public:
    static std::shared_ptr<PickleSender> create(std::unique_ptr<RcQueuePair> qp, uint64_t dop = 16) noexcept(false);

    [[nodiscard]] Handle send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey);

    ~PickleSender() = default;

    /**
     * @brief The executor of the PickleSender.
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);

  private:
    PickleSender(std::unique_ptr<RcQueuePair> qp, uint64_t dop) :
        dop_(dop),
        qp_(std::move(qp)),
        send_ibv_wc_buffer_(dop),
        recv_ibv_wc_buffer_(dop),
        wr_list_capacity_(dop),
        send_wr_list_index_(0),
        send_wr_list_(dop),
        send_sge_list_(dop) {
        this->host_recv_buffer_ = MemoryRegion::create(
            this->qp_->get_pd(),
            std::shared_ptr<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
            sizeof(Ticket) * dop
        );
        this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
        this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();
        for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
            this->qp_->post_recv(
                wr_id,
                this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
                sizeof(Ticket),
                this->recv_buffer_lkey_
            );
        }
    }

    [[deprecated]] void initialize(std::unique_ptr<RcQueuePair> qp, uint64_t dop) noexcept(false);
};

class PickleRecver {
  private:
    uint64_t dop_;
    uint64_t count_pending_requests_;
    std::queue<Ticket> pending_local_recv_request_queue_;
    std::queue<uint64_t> free_slots;
    Queue<Command> recv_request_command_queue_;
    MultiMap<std::shared_ptr<std::atomic<bool>>> pending_local_recv_request_map_;

    std::shared_ptr<RcQueuePair> qp_;
    std::vector<ibv_wc> send_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_send_wcs_;
    std::vector<ibv_wc> recv_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_recv_wcs_;

    std::unique_ptr<MemoryRegion> host_send_buffer_;
    uint64_t send_buffer_addr_;
    uint32_t send_buffer_lkey_;

    std::unique_ptr<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    PickleRecver() = delete;
    PickleRecver(const PickleRecver&) = delete;
    PickleRecver& operator=(const PickleRecver&) = delete;
    PickleRecver(PickleRecver&&) = delete;
    PickleRecver& operator=(PickleRecver&&) = delete;

  public:
    static std::shared_ptr<PickleRecver> create(std::unique_ptr<RcQueuePair> qp, uint64_t dop = 16) noexcept(false);

    [[nodiscard]] Handle recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey);

    ~PickleRecver() = default;

    /**
     * @brief The executor of the PickleRecver
     * SAFETY: !! This function is not thread-safe !!
     */
    void poll() noexcept(false);

  private:
    PickleRecver(std::unique_ptr<RcQueuePair> qp, uint64_t dop) :
        dop_(dop),
        count_pending_requests_(0),
        qp_(std::move(qp)),
        send_ibv_wc_buffer_(dop),
        recv_ibv_wc_buffer_(dop),
        polled_send_wcs_() {
        this->host_send_buffer_ = MemoryRegion::create(
            this->qp_->get_pd(),
            std::shared_ptr<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
            sizeof(Ticket) * dop
        );
        this->send_buffer_addr_ = uint64_t(this->host_send_buffer_->get_addr());
        this->send_buffer_lkey_ = this->host_send_buffer_->get_lkey();

        this->host_recv_buffer_ = MemoryRegion::create(
            this->qp_->get_pd(),
            std::shared_ptr<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
            sizeof(Ticket) * dop
        );
        this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
        this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();

        for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
            this->free_slots.push(wr_id);
        }
    }

    [[deprecated]] void initialize(std::unique_ptr<RcQueuePair> qp, uint64_t dop) noexcept(false);
};

}  // namespace pickle

#endif  // _PICKLE_H_