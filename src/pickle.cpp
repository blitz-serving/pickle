#include "pickle.h"

#include <infiniband/verbs.h>
#include <netinet/in.h>

#include <cstdint>
#include <memory>

#include "pickle_logger.h"
#include "rdma_util.h"

namespace pickle {

union wrid_t {
    struct alignas(8) {
        uint16_t padding_ : 15;
        bool request_finished : 1;
        uint16_t doorbell_length : 16;
        uint32_t stream_id : 32;
    } wr_id;

    uint64_t value;
};

struct alignas(64) Cacheline {
    uint8_t data[64];
};

std::shared_ptr<PickleSender> PickleSender::create(std::unique_ptr<RcQueuePair> qp, uint64_t packet_size) noexcept(false
) {
    return std::shared_ptr<PickleSender>(new PickleSender(std::move(qp), packet_size));
}

PickleSender::PickleSender(std::unique_ptr<RcQueuePair> qp, uint64_t packet_size) noexcept(false) :
    qp_(std::move(qp)),
    packet_size_(packet_size),
    wr_occupied_(0) {
    this->send_wr_list_.resize(kMagic);
    this->send_sge_list_.resize(kMagic);

    this->host_recv_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        std::shared_ptr<void>(new Ticket[kMagic], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * kMagic
    );
    this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
    this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();
    for (uint64_t wr_id = 0; wr_id < kMagic; ++wr_id) {
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }
}

PickleSender::~PickleSender() {
    DEBUG("Destroying PickleSender");
}

Handle PickleSender::send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = lkey;
    Command command {};
    command.ticket = ticket;
    command.flag = flag;
    this->send_request_command_queue_.enqueue(command);
    return Handle(flag);
}

void PickleSender::poll() noexcept(false) {
    int ret = this->qp_->poll_recv_cq_once(kMagic, this->polled_recv_wcs_);

    PICKLE_ASSERT(ret >= 0);
    for (const auto& wc : this->polled_recv_wcs_) {
        PICKLE_ASSERT(
            wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RECV,
            "Failed to receive data: status={}, opcode={}",
            int(wc.status),
            int(wc.opcode)
        );
        auto wr_id = wc.wr_id;
        Ticket ticket {};
        memcpy(&ticket, reinterpret_cast<void*>(this->recv_buffer_addr_ + wr_id * sizeof(Ticket)), sizeof(Ticket));
        this->remote_recv_request_queue_.push(ticket);
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }

    std::vector<Command> commands(kMagic);
    std::vector<Ticket> tickets(kMagic);

    // Received from send request
    uint64_t count_dequeued = this->send_request_command_queue_.try_dequeue_bulk(commands.begin(), kMagic);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        auto ticket = commands[i].ticket;
        auto flag = commands[i].flag;
        this->pending_local_send_request_map_[ticket.stream_id].push(ticket);
        this->pending_local_send_flag_map_[ticket.stream_id].push(flag);
    }

    // Received from thread_post_recv
    int count_poped = 0;
    while (!this->remote_recv_request_queue_.empty() && count_poped < kMagic) {
        tickets[count_poped] = this->remote_recv_request_queue_.front();
        this->remote_recv_request_queue_.pop();
        count_poped++;
    }
    for (uint64_t i = 0; i < count_poped; ++i) {
        this->pending_remote_recv_request_map_[tickets[i].stream_id].push(tickets[i]);
    }

    uint64_t wr_list_start = this->wr_occupied_;
    uint16_t doorbell_length = 0;
    // Execute remote write args
    for (auto& item : this->pending_remote_recv_request_map_) {
        if (this->wr_occupied_ >= this->send_wr_list_.size()) {
            // Fast path
            break;
        }

        uint32_t stream_id = item.first;
        std::queue<Ticket>& pending_remote_recv_request_queue = item.second;
        std::queue<Ticket>& pending_local_send_request_queue = this->pending_local_send_request_map_[stream_id];

        while (this->wr_occupied_ < this->send_wr_list_.size() && !pending_remote_recv_request_queue.empty()
               && !pending_local_send_request_queue.empty()) {
            doorbell_length++;

            auto& remote_recv_request = pending_remote_recv_request_queue.front();
            auto& local_send_request = pending_local_send_request_queue.front();

            uint64_t wr_list_index = this->wr_occupied_;
            uint64_t raddr = remote_recv_request.addr;
            uint32_t rlength = remote_recv_request.length;
            uint32_t rkey = remote_recv_request.key;

            uint64_t laddr = local_send_request.addr;
            uint32_t llength = local_send_request.length;
            uint32_t lkey = local_send_request.key;

            PICKLE_ASSERT(rlength == llength, "Length mismatch");

            uint32_t write_size;
            ibv_wr_opcode opcode;
            wrid_t wr_id;

            if (llength <= this->packet_size_) {
                // The last packet of the request
                opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
                write_size = llength;
                wr_id.wr_id.doorbell_length = doorbell_length;
                wr_id.wr_id.stream_id = stream_id;
                wr_id.wr_id.request_finished = 1;
                pending_remote_recv_request_queue.pop();
                this->pending_local_send_request_map_[stream_id].pop();
            } else {
                // The intermediate packet of the request
                opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                write_size = this->packet_size_;
                wr_id.wr_id.doorbell_length = doorbell_length;
                wr_id.wr_id.stream_id = stream_id;
                wr_id.wr_id.request_finished = 0;
                remote_recv_request.addr += this->packet_size_;
                remote_recv_request.length -= this->packet_size_;
                local_send_request.addr += this->packet_size_;
                local_send_request.length -= this->packet_size_;
            }

            memset(&(this->send_sge_list_[wr_list_index]), 0, sizeof(ibv_sge));
            memset(&(this->send_wr_list_[wr_list_index]), 0, sizeof(ibv_send_wr));

            this->send_sge_list_[wr_list_index].addr = laddr;
            this->send_sge_list_[wr_list_index].lkey = lkey;
            this->send_sge_list_[wr_list_index].length = write_size;

            this->send_wr_list_[wr_list_index].wr_id = wr_id.value;
            this->send_wr_list_[wr_list_index].opcode = opcode;
            this->send_wr_list_[wr_list_index].imm_data = htonl(stream_id);
            this->send_wr_list_[wr_list_index].wr.rdma.remote_addr = raddr;
            this->send_wr_list_[wr_list_index].wr.rdma.rkey = rkey;
            this->send_wr_list_[wr_list_index].sg_list = &(this->send_sge_list_[wr_list_index]);
            this->send_wr_list_[wr_list_index].num_sge = 1;

            if (wr_list_index > 0) {
                this->send_wr_list_[wr_list_index - 1].next = &(this->send_wr_list_[wr_list_index]);
            } else {
                this->send_wr_list_[wr_list_index - 1].next = nullptr;
            }

            if (wr_list_index + 1 == this->send_wr_list_.size()
                || opcode == ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM) {
                this->send_wr_list_[wr_list_index].send_flags = uint32_t(ibv_send_flags::IBV_SEND_SIGNALED);
                doorbell_length = 0;
            } else {
                this->send_wr_list_[wr_list_index].send_flags = 0;
            }

            this->wr_occupied_++;
        }
    }

    if (this->wr_occupied_ > wr_list_start) {
        TRACE("Posting {} work requests", this->wr_occupied_ - wr_list_start);
        this->qp_->post_send_wrs(&(this->send_wr_list_[wr_list_start]));
    }

    ret = this->qp_->poll_send_cq_once(kMagic, this->polled_send_wcs_);

    PICKLE_ASSERT(ret >= 0, "Failed to poll send CQ");
    if (ret > 0) {
        TRACE("Polled {} signaled work completions", ret);
        for (const auto& wc : this->polled_send_wcs_) {
            PICKLE_ASSERT(
                wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RDMA_WRITE,
                "Op write_with_imm failed: status={}, opcode={}",
                int(wc.status),
                int(wc.opcode)
            );

            wrid_t wr_id;
            wr_id.value = wc.wr_id;

            uint16_t doorbell_length = wr_id.wr_id.doorbell_length;
            bool request_finished = wr_id.wr_id.request_finished;
            uint32_t stream_id = wr_id.wr_id.stream_id;

            this->wr_occupied_ -= doorbell_length;
            if (request_finished) {
                this->pending_local_send_flag_map_[stream_id].front()->store(true);
                this->pending_local_send_flag_map_[stream_id].pop();
            }
        }
    }
}

PickleRecver::PickleRecver(std::unique_ptr<RcQueuePair> qp, std::shared_ptr<Flusher> flusher) noexcept(false) :
    count_pending_requests_(0),
    qp_(std::move(qp)),
    polled_send_wcs_(kMagic),
    polled_recv_wcs_(kMagic),
    flusher_(flusher) {
    this->host_send_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        std::shared_ptr<void>(new Ticket[kMagic], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * kMagic
    );
    this->send_buffer_addr_ = uint64_t(this->host_send_buffer_->get_addr());
    this->send_buffer_lkey_ = this->host_send_buffer_->get_lkey();

    this->host_recv_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        std::shared_ptr<void>(new Ticket[kMagic], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * kMagic
    );
    this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
    this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();

    for (uint64_t wr_id = 0; wr_id < kMagic; ++wr_id) {
        this->free_slots.push(wr_id);
    }
}

PickleRecver::~PickleRecver() {
    DEBUG("Destroying PickleRecver");
}

std::shared_ptr<PickleRecver>
PickleRecver::create(std::unique_ptr<RcQueuePair> qp, std::shared_ptr<Flusher> flusher) noexcept(false) {
    return std::shared_ptr<PickleRecver>(new PickleRecver(std::move(qp), flusher));
}

Handle PickleRecver::recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = rkey;
    Command command {};
    command.ticket = ticket;
    command.flag = flag;
    this->recv_request_command_queue_.enqueue(command);
    return Handle(flag);
}

void PickleRecver::poll() noexcept(false) {
    std::vector<Command> commands(kMagic);
    std::vector<Ticket> tickets(kMagic);

    uint64_t count_dequeued = this->recv_request_command_queue_.try_dequeue_bulk(commands.begin(), kMagic);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        this->pending_local_recv_request_map_[commands[i].ticket.stream_id].push(commands[i]);
        this->pending_local_recv_request_queue_.push(commands[i].ticket);
        this->count_pending_requests_++;
    }

    while (!this->free_slots.empty() && !this->pending_local_recv_request_queue_.empty()) {
        uint64_t wr_id = this->free_slots.front();
        Ticket ticket = this->pending_local_recv_request_queue_.front();
        this->free_slots.pop();
        this->pending_local_recv_request_queue_.pop();
        (reinterpret_cast<Ticket*>(this->send_buffer_addr_))[wr_id] = ticket;
        this->qp_->post_recv(wr_id, this->recv_buffer_addr_ + wr_id * sizeof(Ticket), 0, this->recv_buffer_lkey_);
        this->qp_->post_send_send(
            wr_id,
            this->send_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->send_buffer_lkey_,
            true
        );
    }

    int ret;
    ret = this->qp_->poll_send_cq_once(kMagic, this->polled_send_wcs_);

    PICKLE_ASSERT(ret >= 0, "Failed to poll send CQ");
    for (const auto& wc : this->polled_send_wcs_) {
        PICKLE_ASSERT(
            wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_SEND,
            "Op post_send_send failed: status={}, opcode={}",
            int(wc.status),
            int(wc.opcode)
        );
    }

    ret = this->qp_->poll_recv_cq_once(kMagic, this->polled_recv_wcs_);

    PICKLE_ASSERT(ret >= 0, "Failed to poll recv CQ");
    for (const auto& wc : this->polled_recv_wcs_) {
        PICKLE_ASSERT(
            wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RECV_RDMA_WITH_IMM,
            "Failed to receive data: status={}, opcode={}",
            int(wc.status),
            int(wc.opcode)
        );
        uint32_t stream_id = ntohl(wc.imm_data);
        TRACE("Received data from stream {}", stream_id);

        std::queue<Command>& queue = this->pending_local_recv_request_map_[stream_id];
        Command command = queue.front();
        queue.pop();
        this->count_pending_requests_--;
        this->free_slots.push(wc.wr_id);

        // Post-process
        if (this->flusher_ == nullptr) {
            command.flag->store(true);
        } else {
            this->flusher_->append(command.ticket.key, command.ticket.addr, command.flag);
        }
    }
}

Flusher::Flusher(std::shared_ptr<ProtectionDomain>& pd) noexcept(false) : flush_infos_(16) {
    std::shared_ptr<rdma_util::CompletionQueue> cq = CompletionQueue::create(pd->get_context());
    this->loopback_qp_ = RcQueuePair::create(pd, cq, cq);
    this->loopback_qp_->bring_up(this->loopback_qp_->get_handshake_data());
    this->loopback_buffer_ = MemoryRegion::create(
        pd,
        std::shared_ptr<void>(new Cacheline, [](Cacheline* p) { delete p; }),
        sizeof(Cacheline)
    );
}

Flusher::~Flusher() {
    DEBUG("Destroying LoopbackFlusher");
}

std::shared_ptr<Flusher> Flusher::create(std::shared_ptr<ProtectionDomain> pd) noexcept(false) {
    return std::shared_ptr<Flusher>(new Flusher(pd));
}

void Flusher::append(uint32_t rkey, uint64_t raddr, std::shared_ptr<std::atomic<bool>> flag) {
    FlushInfo info {};
    info.rkey = rkey;
    info.raddr = raddr;
    info.flag = flag;
    this->info_queue_.enqueue(info);
}

void Flusher::poll() noexcept(false) {
    if (this->pending_flushes_ > 0) {
        int ret = this->loopback_qp_->poll_send_cq_once(kMagic, this->polled_wcs_);
        PICKLE_ASSERT(ret >= 0);
        for (const ibv_wc& wc : this->polled_wcs_) {
            PICKLE_ASSERT(
                wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RDMA_READ,
                "Failed to flush data: status={}, opcode={}",
                int(wc.status),
                int(wc.opcode)
            );
            this->flushing_queue_.front()->store(true);
            this->flushing_queue_.pop();
            this->pending_flushes_--;
        }
    }

    if (this->pending_flushes_ < 16) {
        uint64_t count_dequeued =
            this->info_queue_.try_dequeue_bulk(this->flush_infos_.begin(), 16 - this->pending_flushes_);
        for (uint64_t i = 0; i < count_dequeued; ++i) {
            this->pending_flushes_++;
            FlushInfo& info = this->flush_infos_[i];
            this->flushing_queue_.push(info.flag);
            this->loopback_qp_->post_send_read(
                0,
                uint64_t(this->loopback_buffer_->get_addr()),
                info.raddr,
                8,
                this->loopback_buffer_->get_lkey(),
                info.rkey,
                true
            );
        }
    }
}

}  // namespace pickle