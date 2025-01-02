#include "pickle.h"

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>

#include "pickle_logger.h"

namespace pickle {

#define ASSERT(expr, msg) \
    if (!(expr)) { \
        ERROR("Assertion failed: %s:%d %s\n", __FILE__, __LINE__, msg); \
        throw std::runtime_error(std::string("Assertion failed: ") + msg); \
    }

std::shared_ptr<PickleSender> PickleSender::create(std::unique_ptr<RcQueuePair> qp, uint64_t packet_size) noexcept(false
) {
    return std::shared_ptr<PickleSender>(new PickleSender(std::move(qp), packet_size));
}

Handle PickleSender::send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = lkey;
    Command command = std::make_tuple(ticket, flag);
    this->send_request_command_queue_.enqueue(command);
    return Handle(flag);
}

void PickleSender::poll() noexcept(false) {
    int ret = this->qp_->poll_recv_cq_once(kMagic, this->polled_recv_wcs_);

    ASSERT(ret >= 0, "Failed to poll recv CQ");
    for (const auto& wc : this->polled_recv_wcs_) {
        ASSERT(
            wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RECV,
            "Failed to receive data"
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
        auto ticket = std::get<0>(commands[i]);
        auto flag = std::get<1>(commands[i]);
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

            ASSERT(rlength == llength, "Length mismatch");

            uint32_t write_size;
            ibv_wr_opcode opcode;
            uint64_t wr_id = 0;

            if (llength <= this->packet_size_) {
                // The last packet of the request
                opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
                write_size = llength;
                wr_id = (uint64_t(1) << 63) | (uint64_t(doorbell_length) << 32) | stream_id;
                pending_remote_recv_request_queue.pop();
                this->pending_local_send_request_map_[stream_id].pop();
            } else {
                // The intermediate packet of the request
                opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                write_size = this->packet_size_;
                wr_id = (uint64_t(doorbell_length) << 32) | stream_id;
                remote_recv_request.addr += this->packet_size_;
                remote_recv_request.length -= this->packet_size_;
                local_send_request.addr += this->packet_size_;
                local_send_request.length -= this->packet_size_;
            }

            this->send_sge_list_[wr_list_index] = {};
            this->send_sge_list_[wr_list_index].addr = laddr;
            this->send_sge_list_[wr_list_index].lkey = lkey;
            this->send_sge_list_[wr_list_index].length = write_size;

            this->send_wr_list_[wr_list_index] = {};
            this->send_wr_list_[wr_list_index].wr_id = wr_id;
            this->send_wr_list_[wr_list_index].opcode = opcode;
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
        DEBUG("Posting {} work requests", this->wr_occupied_ - wr_list_start);
        this->qp_->post_send_wrs(&(this->send_wr_list_[wr_list_start]));
    }

    ret = this->qp_->poll_send_cq_once(kMagic, this->polled_send_wcs_);

    ASSERT(ret >= 0, "Failed to poll send CQ");
    if (ret > 0) {
        DEBUG("Polled {} signaled work completions", ret);
        for (const auto& wc : this->polled_send_wcs_) {
            ASSERT(
                wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RDMA_WRITE,
                "Op write_with_imm failed"
            );
            uint64_t wr_id = wc.wr_id;
            uint16_t doorbell_length = (wr_id >> 32) & 0xFFFF;
            bool request_finished = (wr_id >> 63) & 0x1;
            uint32_t stream_id = wr_id & 0xFFFFFFFF;
            this->wr_occupied_ -= doorbell_length;
            if (request_finished) {
                this->pending_local_send_flag_map_[stream_id].front()->store(true);
                this->pending_local_send_flag_map_[stream_id].pop();
            }
        }
    }
}

std::shared_ptr<PickleRecver> PickleRecver::create(std::unique_ptr<RcQueuePair> qp) noexcept(false) {
    return std::shared_ptr<PickleRecver>(new PickleRecver(std::move(qp)));
}

Handle PickleRecver::recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = rkey;
    Command command = std::make_tuple(ticket, flag);
    this->recv_request_command_queue_.enqueue(command);
    return Handle(flag);
}

void PickleRecver::poll() noexcept(false) {
    std::vector<Command> commands(kMagic);
    std::vector<Ticket> tickets(kMagic);

    uint64_t count_dequeued = this->recv_request_command_queue_.try_dequeue_bulk(commands.begin(), kMagic);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        auto ticket = std::get<0>(commands[i]);
        auto flag = std::get<1>(commands[i]);
        this->pending_local_recv_request_map_[ticket.stream_id].push(flag);
        this->pending_local_recv_request_queue_.push(ticket);
        this->count_pending_requests_++;
    }

    while (!this->free_slots.empty() && !this->pending_local_recv_request_queue_.empty()) {
        uint64_t wr_id = this->free_slots.front();
        Ticket ticket = this->pending_local_recv_request_queue_.front();
        this->free_slots.pop();
        this->pending_local_recv_request_queue_.pop();
        (reinterpret_cast<Ticket*>(this->send_buffer_addr_))[wr_id] = ticket;
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
        this->qp_->post_send_send(
            wr_id,
            this->send_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->send_buffer_lkey_,
            true
        );
    }

    if (this->count_pending_requests_ == 0) {
        return;
    }

    int ret;
    ret = this->qp_->poll_send_cq_once(kMagic, this->polled_send_wcs_);

    ASSERT(ret >= 0, "Failed to poll send CQ");
    for (const auto& wc : this->polled_send_wcs_) {
        ASSERT(
            wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_SEND,
            "Op post_send_send failed"
        );
    }

    ret = this->qp_->poll_recv_cq_once(kMagic, this->polled_recv_wcs_);

    ASSERT(ret >= 0, "Failed to poll recv CQ");
    for (const auto& wc : this->polled_recv_wcs_) {
        ASSERT(
            wc.status == ibv_wc_status::IBV_WC_SUCCESS && wc.opcode == ibv_wc_opcode::IBV_WC_RECV_RDMA_WITH_IMM,
            "Failed to receive data"
        );
        auto wr_id = wc.wr_id;
        std::queue<std::shared_ptr<std::atomic<bool>>>& queue = this->pending_local_recv_request_map_[wc.imm_data];
        queue.front()->store(true);
        queue.pop();
        this->count_pending_requests_--;
        this->free_slots.push(wc.wr_id);
    }
}

}  // namespace pickle