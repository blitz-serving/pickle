#include "pickle.h"

#include <infiniband/verbs.h>

namespace pickle {

using namespace rdma_util;

#define ASSERT(expr, msg) \
    if (!(expr)) { \
        printf("Assertion failed: %s:%d %s\n", __FILE__, __LINE__, msg); \
        throw std::runtime_error(std::string("Assertion failed: ") + msg); \
    }

Arc<PickleSender> PickleSender::create(Box<RcQueuePair> qp, bool spawn_polling_thread, uint64_t dop) noexcept(false) {
    Arc<PickleSender> tccl_context = Arc<PickleSender>(new PickleSender());
    tccl_context->initialize(std::move(qp), dop);
    if (spawn_polling_thread) {
        tccl_context->background_polling_ = true;
        tccl_context->polling_stopped_.store(false);
        tccl_context->polling_thread_ = std::thread([tccl_context]() {
            while (!tccl_context->polling_stopped_.load(std::memory_order_relaxed)) {
                tccl_context->poll_both_inner();
            }
        });
    } else {
        tccl_context->background_polling_ = false;
        tccl_context->polling_stopped_.store(true);
    }
    return tccl_context;
}

PickleSender::~PickleSender() {
    if (this->background_polling_) {
        this->polling_stopped_.store(true);
        this->polling_thread_.join();
    }
}

void PickleSender::initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false) {
    this->dop_ = dop;

    this->qp_ = std::move(qp);

    this->send_ibv_wc_buffer_ = std::vector<ibv_wc>(dop);
    this->polled_send_wcs_ = std::vector<WorkCompletion>();
    this->polled_send_wcs_.reserve(this->dop_);

    this->recv_ibv_wc_buffer_ = std::vector<ibv_wc>(dop);
    this->polled_recv_wcs_ = std::vector<WorkCompletion>();
    this->polled_recv_wcs_.reserve(this->dop_);

    this->send_request_command_queue_ = Queue<Command>();

    this->host_recv_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        Arc<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * this->dop_
    );
    this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
    this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();

    this->remote_recv_request_queue_ = Queue<Ticket>();

    this->pending_remote_recv_request_map_ = MultiMap<Ticket>();
    this->pending_local_send_request_map_ = MultiMap<Ticket>();
    this->pending_local_send_flag_map_ = MultiMap<Arc<std::atomic<bool>>>();

    this->post_send_write_slot_available_ = this->dop_;

    for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }
}

Handle PickleSender::send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey, uint32_t padding) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = lkey;
    ticket.padding_ = padding;
    Command command = std::make_tuple(ticket, flag);
    this->send_request_command_queue_.enqueue(command);
    return Handle(flag);
}

void PickleSender::poll_both_inner() noexcept(false) {
    this->poll_recv_one_round_inner();
    this->poll_send_one_round_inner();
}

void PickleSender::poll_send_one_round_inner() noexcept(false) {
    ASSERT(this->send_ibv_wc_buffer_.size() > 0, "WC buffer is empty");

    std::vector<Command> commands(this->dop_);
    std::vector<Ticket> tickets(this->dop_);

    uint64_t count_dequeued = 0;

    // Received from send request
    count_dequeued = this->send_request_command_queue_.try_dequeue_bulk(commands.begin(), this->dop_);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        auto ticket = std::get<0>(commands[i]);
        auto flag = std::get<1>(commands[i]);
        this->pending_local_send_request_map_[ticket.stream_id].push(ticket);
        this->pending_local_send_flag_map_[ticket.stream_id].push(flag);
    }

    // Received from thread_post_recv
    count_dequeued = this->remote_recv_request_queue_.try_dequeue_bulk(tickets.begin(), this->dop_);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        this->pending_remote_recv_request_map_[tickets[i].stream_id].push(tickets[i]);
    }

    // Execute remote write args
    for (auto& item : this->pending_remote_recv_request_map_) {
        uint32_t stream_id = item.first;
        std::queue<Ticket>& pending_remote_recv_request_queue = item.second;
        if (this->post_send_write_slot_available_ == 0) {
            break;
        } else if (!pending_remote_recv_request_queue.empty()
                   && !this->pending_local_send_request_map_[stream_id].empty()) {
            auto remote_recv_request = pending_remote_recv_request_queue.front();
            auto local_send_request = this->pending_local_send_request_map_[stream_id].front();
            pending_remote_recv_request_queue.pop();
            this->pending_local_send_request_map_[stream_id].pop();

            ASSERT(local_send_request.length == remote_recv_request.length, "Length mismatch");

            auto wr_id = stream_id;
            auto raddr = remote_recv_request.addr;
            auto rkey = remote_recv_request.key;
            auto laddr = local_send_request.addr;
            auto lkey = local_send_request.key;

            this->qp_
                ->post_send_write_with_imm(wr_id, laddr, raddr, local_send_request.length, stream_id, lkey, rkey, true);
            this->post_send_write_slot_available_--;
        }
    }

    int ret = this->qp_->poll_send_cq_once(
        this->send_ibv_wc_buffer_.size(),
        this->send_ibv_wc_buffer_.data(),
        this->polled_send_wcs_
    );

    ASSERT(ret >= 0, "Failed to poll send CQ");
    for (const auto& wc : this->polled_send_wcs_) {
        ASSERT(wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_RDMA_WRITE, "Op write_with_imm failed");
        this->pending_local_send_flag_map_[wc.wr_id].front()->store(true);
        this->pending_local_send_flag_map_[wc.wr_id].pop();
        this->post_send_write_slot_available_++;
    }
}

void PickleSender::poll_recv_one_round_inner() noexcept(false) {
    ASSERT(this->recv_ibv_wc_buffer_.size() > 0, "WC buffer is empty");

    int ret = this->qp_->poll_recv_cq_once(
        this->recv_ibv_wc_buffer_.size(),
        this->recv_ibv_wc_buffer_.data(),
        this->polled_recv_wcs_
    );

    ASSERT(ret >= 0, "Failed to poll recv CQ");
    for (const auto& wc : this->polled_recv_wcs_) {
        ASSERT(wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_RECV, "Failed to receive data");

        auto wr_id = wc.wr_id;
        Ticket ticket {};
        memcpy(&ticket, reinterpret_cast<void*>(this->recv_buffer_addr_ + wr_id * sizeof(Ticket)), sizeof(Ticket));
        this->remote_recv_request_queue_.enqueue(ticket);
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }
}

Arc<PickleRecver> PickleRecver::create(Box<RcQueuePair> qp, bool spawn_polling_thread, uint64_t dop) noexcept(false) {
    Arc<PickleRecver> tccl_context = Arc<PickleRecver>(new PickleRecver());
    tccl_context->initialize(std::move(qp), dop);
    if (spawn_polling_thread) {
        tccl_context->background_polling_ = true;
        tccl_context->polling_stopped_.store(false);
        tccl_context->polling_thread_ = std::thread([tccl_context]() {
            while (!tccl_context->polling_stopped_.load(std::memory_order_relaxed)) {
                tccl_context->poll_both_inner();
            }
        });
    } else {
        tccl_context->background_polling_ = false;
        tccl_context->polling_stopped_.store(true);
    }
    return tccl_context;
}

PickleRecver::~PickleRecver() {
    if (this->background_polling_) {
        this->polling_stopped_.store(true);
        this->polling_thread_.join();
    }
}

void PickleRecver::initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false) {
    this->dop_ = dop;
    this->qp_ = std::move(qp);

    this->send_ibv_wc_buffer_ = std::vector<ibv_wc>(dop);
    this->polled_send_wcs_ = std::vector<WorkCompletion>();
    this->polled_send_wcs_.reserve(this->dop_);

    this->recv_ibv_wc_buffer_ = std::vector<ibv_wc>(dop);
    this->polled_recv_wcs_ = std::vector<WorkCompletion>();
    this->polled_recv_wcs_.reserve(this->dop_);

    this->recv_request_command_queue_ = Queue<Command>();

    this->host_send_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        Arc<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * this->dop_
    );
    this->send_buffer_addr_ = uint64_t(this->host_send_buffer_->get_addr());
    this->send_buffer_lkey_ = this->host_send_buffer_->get_lkey();

    this->host_recv_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        Arc<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * this->dop_
    );
    this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
    this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();

    this->local_recv_request_queue_ = Queue<Ticket>();

    this->pending_local_recv_request_queue_ = std::queue<Ticket>();
    this->pending_remote_recv_request_map_ = MultiMap<Ticket>();

    this->free_post_send_send_slots_ = std::queue<uint64_t>();
    this->post_send_send_slot_available_ = this->dop_;
    for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
        this->free_post_send_send_slots_.push(wr_id);
    }

    this->pending_recv_request_count_ = 0;
    this->pending_local_recv_request_map_ = MultiMap<Arc<std::atomic<bool>>>();

    // TODO: do not post_recv until recver side post_send_send is called
    for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }
}

Handle PickleRecver::recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey, uint32_t padding) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = rkey;
    ticket.padding_ = padding;
    Command command = std::make_tuple(ticket, flag);
    this->recv_request_command_queue_.enqueue(command);
    return Handle(flag);
}

void PickleRecver::poll_both_inner() noexcept(false) {
    this->poll_recv_one_round_inner();
    this->poll_send_one_round_inner();
}

void PickleRecver::poll_send_one_round_inner() noexcept(false) {
    ASSERT(this->send_ibv_wc_buffer_.size() > 0, "WC buffer is empty");

    std::vector<Ticket> tickets(this->dop_);

    // Received from recv request
    uint64_t count_dequeued = this->local_recv_request_queue_.try_dequeue_bulk(tickets.begin(), this->dop_);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        this->pending_local_recv_request_queue_.push(tickets[i]);
    }

    // Send local write args to remote side
    while (this->post_send_send_slot_available_ > 0 && !this->pending_local_recv_request_queue_.empty()) {
        uint64_t wr_id = this->free_post_send_send_slots_.front();
        Ticket ticket = this->pending_local_recv_request_queue_.front();
        this->free_post_send_send_slots_.pop();
        this->pending_local_recv_request_queue_.pop();
        memcpy(reinterpret_cast<void*>(this->send_buffer_addr_ + wr_id * sizeof(Ticket)), &ticket, sizeof(Ticket));

        this->qp_->post_send_send(
            wr_id,
            this->send_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->send_buffer_lkey_,
            true
        );
        this->post_send_send_slot_available_--;
    }

    int ret = this->qp_->poll_send_cq_once(
        this->send_ibv_wc_buffer_.size(),
        this->send_ibv_wc_buffer_.data(),
        this->polled_send_wcs_
    );

    ASSERT(ret >= 0, "Failed to poll send CQ");
    for (const auto& wc : this->polled_send_wcs_) {
        ASSERT(wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_SEND, "Op post_send_send failed");
        this->free_post_send_send_slots_.push(wc.wr_id);
        this->post_send_send_slot_available_++;
    }
}

void PickleRecver::poll_recv_one_round_inner() noexcept(false) {
    ASSERT(this->recv_ibv_wc_buffer_.size() > 0, "WC buffer is empty");

    std::vector<Command> commands(this->dop_);
    std::vector<Ticket> tickets(this->dop_);

    const uint64_t dequeued_count = this->recv_request_command_queue_.try_dequeue_bulk(commands.begin(), this->dop_);
    for (uint64_t i = 0; i < dequeued_count; ++i) {
        tickets[i] = std::get<0>(commands[i]);
        this->pending_local_recv_request_map_[tickets[i].stream_id].push(std::get<1>(commands[i]));
        this->pending_recv_request_count_++;
    }
    this->local_recv_request_queue_.enqueue_bulk(tickets.begin(), dequeued_count);

    int ret = this->qp_->poll_recv_cq_once(
        this->recv_ibv_wc_buffer_.size(),
        this->recv_ibv_wc_buffer_.data(),
        this->polled_recv_wcs_
    );

    ASSERT(ret >= 0, "Failed to poll recv CQ");

    for (const auto& wc : this->polled_recv_wcs_) {
        ASSERT(wc.status == IBV_WC_SUCCESS, "Failed to receive data");
        ASSERT(wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM, "Unexpected opcode");

        auto wr_id = wc.wr_id;
        std::queue<Arc<std::atomic<bool>>>& queue = this->pending_local_recv_request_map_[wc.imm_data];
        queue.front()->store(1);
        queue.pop();
        this->pending_recv_request_count_--;
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }
}

}  // namespace pickle