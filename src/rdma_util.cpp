#include "rdma_util.h"

#include <infiniband/verbs.h>
#include <netinet/in.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "concurrentqueue.h"

namespace rdma_util {

constexpr uint64_t kSlotNum = 32;
constexpr uint64_t kHostBufferSize = sizeof(Ticket) * kSlotNum;

Context::Context(const char* dev_name) noexcept(false) {
    auto dev_list = ibv_get_device_list(nullptr);
    if (!dev_list) {
        throw std::runtime_error("Failed to get device list");
    }

    ibv_device* dev = nullptr;
    for (int i = 0; dev_list[i] != nullptr; i++) {
        if (strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
            dev = dev_list[i];
            break;
        }
    }

    if (dev == nullptr) {
        ibv_free_device_list(dev_list);
        throw std::runtime_error("Device not found");
    }

    auto context = ibv_open_device(dev);
    if (context == nullptr) {
        ibv_free_device_list(dev_list);
        throw std::runtime_error("Failed to open device");
    }

    this->inner = context;
}

Context::~Context() {
    if (this->inner) {
        ibv_close_device(this->inner);
    }
}

std::unique_ptr<Context> Context::create(const char* dev_name) noexcept(false) {
    return std::unique_ptr<Context>(new Context(dev_name));
}

ProtectionDomain::ProtectionDomain(std::shared_ptr<Context> context) noexcept(false) {
    this->context_ = context;
    this->inner = ibv_alloc_pd(context->inner);
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to allocate protection domain");
    }
}

std::unique_ptr<ProtectionDomain> ProtectionDomain::create(std::shared_ptr<Context> context) noexcept(false) {
    return std::unique_ptr<ProtectionDomain>(new ProtectionDomain(context));
}

ProtectionDomain::~ProtectionDomain() {
    if (this->inner) {
        ibv_dealloc_pd(this->inner);
    }
}

RcQueuePair::RcQueuePair(std::shared_ptr<ProtectionDomain> pd) noexcept(false) {
    this->pd_ = pd;
    this->context_ = pd->context_;
    auto send_cq = ibv_create_cq(context_->inner, 128, nullptr, nullptr, 0);
    auto recv_cq = ibv_create_cq(context_->inner, 128, nullptr, nullptr, 0);

    if (send_cq == nullptr || recv_cq == nullptr) {
        if (send_cq) {
            ibv_destroy_cq(send_cq);
        }
        if (recv_cq) {
            ibv_destroy_cq(recv_cq);
        }
        throw std::runtime_error("Failed to create completion queue");
    }

    ibv_qp_init_attr init_attr {};
    init_attr.send_cq = send_cq;
    init_attr.recv_cq = recv_cq;
    init_attr.cap.max_send_wr = 128;
    init_attr.cap.max_recv_wr = 1024;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = 64;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.sq_sig_all = 0;

    this->inner = ibv_create_qp(pd->inner, &init_attr);
    if (this->inner == nullptr) {
        ibv_destroy_cq(send_cq);
        ibv_destroy_cq(recv_cq);
        throw std::runtime_error("Failed to create queue pair");
    }
}

std::unique_ptr<RcQueuePair> RcQueuePair::create(const char* dev_name) noexcept(false) {
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(Context::create(dev_name))));
}

std::unique_ptr<RcQueuePair> RcQueuePair::create(std::shared_ptr<Context> context) noexcept(false) {
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context)));
}

std::unique_ptr<RcQueuePair> RcQueuePair::create(std::shared_ptr<ProtectionDomain> pd) noexcept(false) {
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(pd));
}

RcQueuePair::~RcQueuePair() {
    if (this->inner) {
        ibv_destroy_cq(this->inner->send_cq);
        ibv_destroy_cq(this->inner->recv_cq);
        ibv_destroy_qp(this->inner);
    }
}

QueuePairState RcQueuePair::query_qp_state() noexcept(false) {
    ibv_qp_attr attr;
    ibv_qp_init_attr init_attr;
    if (ibv_query_qp(this->inner, &attr, IBV_QP_STATE, &init_attr)) {
        throw std::runtime_error("Failed to query QP state");
    }
    switch (attr.qp_state) {
        case ibv_qp_state::IBV_QPS_RESET:
            return QueuePairState::RESET;
        case ibv_qp_state::IBV_QPS_INIT:
            return QueuePairState::INIT;
        case ibv_qp_state::IBV_QPS_RTR:
            return QueuePairState::RTR;
        case ibv_qp_state::IBV_QPS_RTS:
            return QueuePairState::RTS;
        default:
            return QueuePairState::UNKNOWN;
    }
}

HandshakeData RcQueuePair::get_handshake_data() noexcept(false) {
    ibv_gid gid;
    if (ibv_query_gid(this->context_->inner, 1, 0, &gid)) {
        throw std::runtime_error("Failed to query gid");
    }
    ibv_port_attr attr;
    if (ibv_query_port(this->context_->inner, 1, &attr)) {
        throw std::runtime_error("Failed to query port");
    }

    HandshakeData handshake_data {};
    handshake_data.gid = gid;
    handshake_data.lid = attr.lid;
    handshake_data.qp_num = this->inner->qp_num;
    return handshake_data;
}

void RcQueuePair::bring_up(const HandshakeData& handshake_data) noexcept(false) {
    ibv_gid gid = handshake_data.gid;
    uint16_t lid = handshake_data.lid;
    uint32_t remote_qp_num = handshake_data.qp_num;

    {
        // Check QP state
        int mask = ibv_qp_attr_mask::IBV_QP_STATE;
        ibv_qp_attr attr;
        ibv_qp_init_attr init_attr;
        int ret = ibv_query_qp(this->inner, &attr, mask, &init_attr);
        if (ret) {
            throw std::runtime_error("Failed to query QP state");
        } else if (attr.qp_state == ibv_qp_state::IBV_QPS_RTS) {
            printf("[warn] QP state is already RTS\n");
            return;
        } else if (attr.qp_state != ibv_qp_state::IBV_QPS_RESET) {
            throw std::runtime_error("QP state is not RESET");
        }
    }

    {
        // Modify QP to INIT
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;

        ibv_qp_attr attr {};
        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        attr.pkey_index = 0;
        attr.port_num = 1;

        if (ibv_modify_qp(this->inner, &attr, mask)) {
            throw std::runtime_error("Failed to modify to INIT");
        }
    }

    {
        // Modify QP to ready-to-receive
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_AV | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        ibv_qp_attr attr {};
        attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = ibv_mtu::IBV_MTU_4096;
        attr.rq_psn = remote_qp_num;
        attr.dest_qp_num = remote_qp_num;
        attr.ah_attr.grh.dgid = gid;
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.sgid_index = 0;
        attr.ah_attr.grh.hop_limit = 255;
        attr.ah_attr.dlid = lid;
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 0;

        if (ibv_modify_qp(this->inner, &attr, mask)) {
            throw std::runtime_error("Failed to modify to RTR");
        }
    }

    {
        // Modify QP to ready-to-send
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT | ibv_qp_attr_mask::IBV_QP_RNR_RETRY | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

        ibv_qp_attr attr {};
        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.sq_psn = this->inner->qp_num;
        attr.max_rd_atomic = 16;
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;

        if (ibv_modify_qp(this->inner, &attr, mask)) {
            throw std::runtime_error("Failed to modify to RTS");
        }
    }
}

int RcQueuePair::post_send_send(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey, bool signaled) noexcept {
    ibv_sge sge {};
    sge.addr = addr;
    sge.length = length;
    sge.lkey = lkey;
    ibv_send_wr wr {};
    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;
    ibv_send_wr* bad_wr;
    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_send_read(
    uint64_t wr_id,
    uint64_t laddr,
    uint64_t raddr,
    uint32_t length,
    uint32_t lkey,
    uint32_t rkey,
    bool signaled
) noexcept {
    ibv_sge sge {};
    ibv_send_wr wr {};
    ibv_send_wr* bad_wr = nullptr;

    sge.addr = laddr;
    sge.length = length;
    sge.lkey = lkey;

    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;

    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_send_write(
    uint64_t wr_id,
    uint64_t laddr,
    uint64_t raddr,
    uint32_t length,
    uint32_t lkey,
    uint32_t rkey,
    bool signaled
) noexcept {
    ibv_sge sge {};
    ibv_send_wr wr {};
    ibv_send_wr* bad_wr = nullptr;

    sge.addr = laddr;
    sge.length = length;
    sge.lkey = lkey;

    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;

    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_send_write_with_imm(
    uint64_t wr_id,
    uint64_t laddr,
    uint64_t raddr,
    uint32_t length,
    uint32_t imm,
    uint32_t lkey,
    uint32_t rkey,
    bool signaled
) noexcept {
    ibv_sge sge {};
    ibv_send_wr wr {};

    sge.addr = laddr;
    sge.length = length;
    sge.lkey = lkey;

    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;
    wr.imm_data = htonl(imm);
    ibv_send_wr* bad_wr;
    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_recv(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey) noexcept {
    ibv_sge sge {};
    sge.addr = addr;
    sge.length = length;
    sge.lkey = lkey;
    ibv_recv_wr wr {};
    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    ibv_recv_wr* bad_wr;
    return ibv_post_recv(this->inner, &wr, &bad_wr);
}

int RcQueuePair::wait_until_send_completion(
    const int expected_num_wcs,
    std::vector<WorkCompletion>& polled_wcs
) noexcept {
    const int expected = expected_num_wcs;
    ibv_wc* work_completions = new ibv_wc[expected_num_wcs];
    int ret = 0;
    int num_polled_completions = 0;

    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    while (num_polled_completions < expected_num_wcs) {
        ret = ibv_poll_cq(this->inner->send_cq, expected_num_wcs, work_completions);
        if (ret > 0) {
            for (int i = 0; i < ret; ++i) {
                WorkCompletion work_completion {};
                work_completion.wr_id = work_completions[i].wr_id;
                work_completion.status = work_completions[i].status;
                work_completion.byte_len = work_completions[i].byte_len;
                work_completion.opcode = work_completions[i].opcode;
                work_completion.imm_data = ntohl(work_completions[i].imm_data);
                polled_wcs.push_back(work_completion);
            }
            num_polled_completions += ret;
        } else if (ret < 0) {
            delete[] work_completions;
            return ret;
        }
    }
    delete[] work_completions;
    return 0;
}

int RcQueuePair::wait_until_recv_completion(
    const int expected_num_wcs,
    std::vector<WorkCompletion>& polled_wcs
) noexcept {
    const int expected = expected_num_wcs;
    ibv_wc* work_completions = new ibv_wc[expected_num_wcs];
    int ret = 0;
    int num_polled_completions = 0;

    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    while (num_polled_completions < expected_num_wcs) {
        ret = ibv_poll_cq(this->inner->recv_cq, expected_num_wcs, work_completions);
        if (ret > 0) {
            for (int i = 0; i < ret; ++i) {
                WorkCompletion work_completion {};
                work_completion.wr_id = work_completions[i].wr_id;
                work_completion.status = work_completions[i].status;
                work_completion.byte_len = work_completions[i].byte_len;
                work_completion.opcode = work_completions[i].opcode;
                work_completion.imm_data = ntohl(work_completions[i].imm_data);
                polled_wcs.push_back(work_completion);
            }
            num_polled_completions += ret;
        } else if (ret < 0) {
            delete[] work_completions;
            return ret;
        }
    }
    delete[] work_completions;
    return 0;
}

int RcQueuePair::poll_send_cq_once(const int max_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    const int expected = max_num_wcs;
    ibv_wc* work_completions = new ibv_wc[max_num_wcs];

    int ret = ibv_poll_cq(this->inner->send_cq, max_num_wcs, work_completions);

    if (ret > 0) {
        for (int i = 0; i < ret; ++i) {
            WorkCompletion work_completion {};
            work_completion.wr_id = work_completions[i].wr_id;
            work_completion.status = work_completions[i].status;
            work_completion.byte_len = work_completions[i].byte_len;
            work_completion.opcode = work_completions[i].opcode;
            work_completion.imm_data = ntohl(work_completions[i].imm_data);
            polled_wcs.push_back(work_completion);
        }
    }

    delete[] work_completions;
    return ret;
}

int RcQueuePair::poll_recv_cq_once(const int max_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    const int expected = max_num_wcs;
    ibv_wc* work_completions = new ibv_wc[max_num_wcs];

    int ret = ibv_poll_cq(this->inner->recv_cq, max_num_wcs, work_completions);

    if (ret > 0) {
        for (int i = 0; i < ret; ++i) {
            WorkCompletion work_completion {};
            work_completion.wr_id = work_completions[i].wr_id;
            work_completion.status = work_completions[i].status;
            work_completion.byte_len = work_completions[i].byte_len;
            work_completion.opcode = work_completions[i].opcode;
            work_completion.imm_data = ntohl(work_completions[i].imm_data);
            polled_wcs.push_back(work_completion);
        }
    }

    delete[] work_completions;
    return ret;
}

int RcQueuePair::poll_send_cq_once(const int max_num_wcs, ibv_wc* wc_buffer, std::vector<WorkCompletion>& polled_wcs) {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    const int expected = max_num_wcs;
    ibv_wc* work_completions = wc_buffer;

    int ret = ibv_poll_cq(this->inner->send_cq, max_num_wcs, work_completions);

    if (ret > 0) {
        for (int i = 0; i < ret; ++i) {
            WorkCompletion work_completion {};
            work_completion.wr_id = work_completions[i].wr_id;
            work_completion.status = work_completions[i].status;
            work_completion.byte_len = work_completions[i].byte_len;
            work_completion.opcode = work_completions[i].opcode;
            work_completion.imm_data = ntohl(work_completions[i].imm_data);
            polled_wcs.push_back(work_completion);
        }
    }

    return ret;
}

int RcQueuePair::poll_recv_cq_once(const int max_num_wcs, ibv_wc* wc_buffer, std::vector<WorkCompletion>& polled_wcs) {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    const int expected = max_num_wcs;
    ibv_wc* work_completions = wc_buffer;

    int ret = ibv_poll_cq(this->inner->recv_cq, max_num_wcs, work_completions);

    if (ret > 0) {
        for (int i = 0; i < ret; ++i) {
            WorkCompletion work_completion {};
            work_completion.wr_id = work_completions[i].wr_id;
            work_completion.status = work_completions[i].status;
            work_completion.byte_len = work_completions[i].byte_len;
            work_completion.opcode = work_completions[i].opcode;
            work_completion.imm_data = ntohl(work_completions[i].imm_data);
            polled_wcs.push_back(work_completion);
        }
    }

    return ret;
}

MemoryRegion::MemoryRegion(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
    this->pd_ = pd;
    this->context_ = pd->context_;
    this->inner = ibv_reg_mr(
        pd->inner,
        addr,
        length,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
    );
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to register memory region");
    }
}

MemoryRegion::~MemoryRegion() {
    if (this->inner) {
        ibv_dereg_mr(this->inner);
    }
}

std::unique_ptr<MemoryRegion>
MemoryRegion::create(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
    return std::unique_ptr<MemoryRegion>(new MemoryRegion(pd, addr, length));
}

std::shared_ptr<TcclContext> TcclContext::create(std::unique_ptr<RcQueuePair> qp) noexcept(false) {
    return std::shared_ptr<TcclContext>(new TcclContext(std::move(qp)));
}

void TcclContext::send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey) {
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = lkey;
    this->send_request_command_queue_->enqueue(ticket);
}

void TcclContext::recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey) {
    auto flag = std::make_shared<std::atomic<int>>(0);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = rkey;
    Command command = std::make_tuple(ticket, flag);
    this->recv_request_command_queue_->enqueue(command);
    while (flag->load() == 0) {
        std::this_thread::yield();
    }
}

// local_send_request_queue is received from Send Request like NcclSend
// local_recv_request_queue is received from Recv Request like NcclRecv
// remote_recv_request_queue is received from remote side sender.
void TcclContext::thread_post_send(
    std::shared_ptr<RcQueuePair> qp,
    std::unique_ptr<MemoryRegion> host_send_buffer,
    std::shared_ptr<std::atomic<bool>> finalized,
    Queue<Ticket> local_send_request_queue,
    Queue<Ticket> local_recv_request_queue,
    Queue<Ticket> remote_recv_request_queue
) noexcept(false) {
    auto buffer_addr = uint64_t(host_send_buffer->get_addr());
    constexpr uint64_t chunksize = uint64_t(sizeof(Ticket));
    const uint32_t lkey = host_send_buffer->get_lkey();

    std::queue<Ticket> pending_local_recv_request_queue;
    MultiMap<Ticket> pending_local_send_request_map;
    MultiMap<Ticket> pending_remote_recv_request_map;

    std::vector<WorkCompletion> polled_send_wcs;
    polled_send_wcs.reserve(2 * kSlotNum);
    ibv_wc* wc_buffer = new ibv_wc[2 * kSlotNum];

    Ticket tickets[16];
    uint64_t count_dequeued = 0;

    std::queue<uint64_t> free_post_send_send_slots;

    uint64_t incremented_write_imm_id = kSlotNum;

    uint64_t post_send_write_slot_available = kSlotNum;
    uint64_t post_send_send_slot_available = kSlotNum;
    for (uint64_t wr_id = 0; wr_id < kSlotNum; ++wr_id) {
        free_post_send_send_slots.push(wr_id);
    }

    while (!finalized->load(std::memory_order_relaxed)) {
        // Received from send request
        count_dequeued = local_send_request_queue->try_dequeue_bulk(tickets, 16);
        for (uint64_t i = 0; i < count_dequeued; ++i) {
            pending_local_send_request_map[tickets[i].stream_id].push(tickets[i]);
        }

        // Received from recv request
        count_dequeued = local_recv_request_queue->try_dequeue_bulk(tickets, 16);
        for (uint64_t i = 0; i < count_dequeued; ++i) {
            pending_local_recv_request_queue.push(tickets[i]);
        }

        // Received from thread_post_recv
        count_dequeued = remote_recv_request_queue->try_dequeue_bulk(tickets, 16);
        for (uint64_t i = 0; i < count_dequeued; ++i) {
            pending_remote_recv_request_map[tickets[i].stream_id].push(tickets[i]);
        }

        // Send local write args to remote side
        while (post_send_send_slot_available > 0 && !pending_local_recv_request_queue.empty()) {
            post_send_send_slot_available--;
            uint64_t wr_id = free_post_send_send_slots.front();
            Ticket ticket = pending_local_recv_request_queue.front();
            free_post_send_send_slots.pop();
            pending_local_recv_request_queue.pop();
            memcpy(reinterpret_cast<void*>(buffer_addr + wr_id * chunksize), &ticket, sizeof(Ticket));
            // Tell the remote side (sender) receiver information

            // printf("post_send_send: wr_id %lu stream_id %u\n", wr_id, ticket.stream_id);
            qp->post_send_send(wr_id, buffer_addr + wr_id * chunksize, chunksize, host_send_buffer->get_lkey(), true);
        }

        // Execute remote write args
        for (auto& item : pending_remote_recv_request_map) {
            uint32_t stream_id = item.first;
            std::queue<Ticket>& pending_remote_recv_request_queue = item.second;
            if (post_send_write_slot_available > 0 && !pending_remote_recv_request_queue.empty()
                && !pending_local_send_request_map[stream_id].empty()) {
                auto remote_recv_request = pending_remote_recv_request_queue.front();
                auto local_send_request = pending_local_send_request_map[stream_id].front();
                pending_remote_recv_request_queue.pop();
                pending_local_send_request_map[stream_id].pop();

                if (local_send_request.length != remote_recv_request.length) {
                    throw std::runtime_error("Length mismatch");
                }

                auto wr_id = incremented_write_imm_id;
                auto raddr = remote_recv_request.addr;
                auto rkey = remote_recv_request.key;
                auto laddr = local_send_request.addr;
                auto lkey = local_send_request.key;
                incremented_write_imm_id++;

                // printf("post_send_write: %lu %u %lu %lu %u %u\n", wr_id, stream_id, raddr, laddr, rkey, lkey);

                qp->post_send_write_with_imm(
                    wr_id,
                    laddr,
                    raddr,
                    local_send_request.length,
                    stream_id,
                    lkey,
                    rkey,
                    true
                );
                post_send_write_slot_available--;
            }
        }

        int ret = qp->poll_send_cq_once(2 * kSlotNum, wc_buffer, polled_send_wcs);

        if (ret < 0) {
            throw std::runtime_error("Failed to poll send CQ");
        } else if (ret > 0) {
            for (const auto& wc : polled_send_wcs) {
                if (wc.status != IBV_WC_SUCCESS) {
                    // printf("Failed wc: %s\n", wc.to_string().c_str());
                    throw std::runtime_error("Failed to send data");
                } else if (wc.opcode == IBV_WC_SEND) {
                    // printf("Polled IBV_WC_SEND wc: %s\n", wc.to_string().c_str());
                    free_post_send_send_slots.push(wc.wr_id);
                    post_send_send_slot_available++;
                } else if (wc.opcode == IBV_WC_RDMA_WRITE) {
                    // printf("Polled IBV_WC_RDMA_WRITE wc: %s\n", wc.to_string().c_str());
                    post_send_write_slot_available++;
                }
            }
        }
    }

    delete[] wc_buffer;
}

void TcclContext::thread_post_recv(
    std::shared_ptr<RcQueuePair> qp,
    std::unique_ptr<MemoryRegion> host_recv_buffer,
    std::shared_ptr<std::atomic<bool>> finalized,
    Queue<Command> recv_command_queue,
    Queue<Ticket> local_recv_request_queue,
    Queue<Ticket> remote_recv_request_queue
) noexcept(false) {
    auto buffer_size = host_recv_buffer->get_length();
    auto buffer_addr = uint64_t(host_recv_buffer->get_addr());
    auto chunksize = uint64_t(sizeof(Ticket));

    for (uint64_t wr_id = 0; wr_id < kSlotNum; ++wr_id) {
        qp->post_recv(wr_id, buffer_addr + wr_id * chunksize, chunksize, host_recv_buffer->get_lkey());
    }

    std::vector<WorkCompletion> polled_recv_wcs;
    polled_recv_wcs.reserve(kSlotNum);

    ibv_wc* wc_buffer = new ibv_wc[kSlotNum];

    uint64_t count_dequeued = 0;
    Command command;

    MultiMap<std::shared_ptr<std::atomic<int>>> pending_local_recv_request_map;

    while (!finalized->load(std::memory_order_relaxed)) {
        while (recv_command_queue->try_dequeue(command)) {
            auto ticket = std::get<0>(command);
            auto flag = std::get<1>(command);
            local_recv_request_queue->enqueue(ticket);
            pending_local_recv_request_map[ticket.stream_id].push(flag);
        }

        int ret = qp->poll_recv_cq_once(kSlotNum, wc_buffer, polled_recv_wcs);
        if (ret > 0) {
            for (const auto& wc : polled_recv_wcs) {
                if (wc.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("Failed to receive data");
                } else {
                    auto wr_id = wc.wr_id;
                    if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                        // printf("Polled IBV_WC_RECV_RDMA_WITH_IMM wc: %s\n", wc.to_string().c_str());
                        std::queue<std::shared_ptr<std::atomic<int>>>& queue =
                            pending_local_recv_request_map[wc.imm_data];
                        queue.front()->store(1);
                        queue.pop();
                    } else {
                        // printf("Polled IBV_WC_RECV wc: %s\n", wc.to_string().c_str());
                        Ticket ticket {};
                        memcpy(&ticket, reinterpret_cast<void*>(buffer_addr + wr_id * chunksize), chunksize);
                        // printf("Polled Ticket: stream_id %u\n", ticket.stream_id);
                        remote_recv_request_queue->enqueue(ticket);
                    }
                    qp->post_recv(wr_id, buffer_addr + wr_id * chunksize, chunksize, host_recv_buffer->get_lkey());
                }
            }
        } else if (ret < 0) {
            throw std::runtime_error("Failed to poll recv CQ");
        }
    }

    delete[] wc_buffer;
}

TcclContext::TcclContext(std::unique_ptr<RcQueuePair> qp) noexcept(false) {
    auto host_send_buffer_raw = new uint8_t[kHostBufferSize];
    auto host_recv_buffer_raw = new uint8_t[kHostBufferSize];

    auto host_send_buffer = MemoryRegion::create(qp->get_pd(), host_send_buffer_raw, kHostBufferSize);
    auto host_recv_buffer = MemoryRegion::create(qp->get_pd(), host_recv_buffer_raw, kHostBufferSize);

    std::shared_ptr<std::atomic<bool>> finalized = std::make_shared<std::atomic<bool>>(false);

    auto local_send_request_queue = Queue<Ticket>(new moodycamel::ConcurrentQueue<Ticket>());
    auto local_recv_request_queue = Queue<Ticket>(new moodycamel::ConcurrentQueue<Ticket>());
    auto remote_recv_request_queue = Queue<Ticket>(new moodycamel::ConcurrentQueue<Ticket>());
    auto recv_command_queue = Queue<Command>(new moodycamel::ConcurrentQueue<Command>());

    std::shared_ptr<RcQueuePair> shared_qp = std::move(qp);

    std::thread t1(
        thread_post_send,
        shared_qp,
        std::move(host_send_buffer),
        finalized,
        local_send_request_queue,
        local_recv_request_queue,
        remote_recv_request_queue
    );

    std::thread t2(
        thread_post_recv,
        shared_qp,
        std::move(host_recv_buffer),
        finalized,
        recv_command_queue,
        local_recv_request_queue,
        remote_recv_request_queue
    );

    this->thread_post_send_ = std::move(t1);
    this->thread_post_recv_ = std::move(t2);
    this->recv_request_command_queue_ = std::move(recv_command_queue);
    this->send_request_command_queue_ = std::move(local_send_request_queue);
    this->finalized_ = finalized;
    this->host_send_buffer_raw_ = host_send_buffer_raw;
    this->host_recv_buffer_raw_ = host_recv_buffer_raw;
}

TcclContext::~TcclContext() {
    this->finalized_->store(true);
    this->thread_post_send_.join();
    this->thread_post_recv_.join();
    delete[] this->host_send_buffer_raw_;
    delete[] this->host_recv_buffer_raw_;
}

}  // namespace rdma_util
