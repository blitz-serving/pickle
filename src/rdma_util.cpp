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

#define ASSERT(expr, msg) \
    if (!(expr)) { \
        printf("Assertion failed: %s:%d %s\n", __FILE__, __LINE__, msg); \
        throw std::runtime_error(std::string("Assertion failed: ") + msg); \
    }

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

Box<Context> Context::create(const char* dev_name) noexcept(false) {
    return Box<Context>(new Context(dev_name));
}

ProtectionDomain::ProtectionDomain(rdma_util::Arc<Context> context) noexcept(false) {
    this->context_ = context;
    this->inner = ibv_alloc_pd(context->inner);
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to allocate protection domain");
    }
}

Box<ProtectionDomain> ProtectionDomain::create(rdma_util::Arc<Context> context) noexcept(false) {
    return Box<ProtectionDomain>(new ProtectionDomain(context));
}

ProtectionDomain::~ProtectionDomain() {
    if (this->inner) {
        ibv_dealloc_pd(this->inner);
    }
}

RcQueuePair::RcQueuePair(rdma_util::Arc<ProtectionDomain> pd) noexcept(false) {
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

Box<RcQueuePair> RcQueuePair::create(const char* dev_name) noexcept(false) {
    return Box<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(Context::create(dev_name))));
}

Box<RcQueuePair> RcQueuePair::create(rdma_util::Arc<Context> context) noexcept(false) {
    return Box<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context)));
}

Box<RcQueuePair> RcQueuePair::create(rdma_util::Arc<ProtectionDomain> pd) noexcept(false) {
    return Box<RcQueuePair>(new RcQueuePair(pd));
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

int RcQueuePair::post_send_send(
    uint64_t wr_id,
    uint64_t laddr,
    uint32_t length,
    uint32_t lkey,
    bool signaled
) noexcept {
    ibv_sge sge {};
    ibv_send_wr wr {};
    ibv_send_wr* bad_wr;

    sge.addr = laddr;
    sge.length = length;
    sge.lkey = lkey;

    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;

    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_send_send_with_imm(
    uint64_t wr_id,
    uint64_t laddr,
    uint32_t length,
    uint32_t lkey,
    uint32_t imm,
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
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.imm_data = htonl(imm);
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;

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
    ibv_send_wr* bad_wr = nullptr;

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

    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_recv(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey) noexcept {
    ibv_sge sge {};
    ibv_recv_wr wr {};
    ibv_recv_wr* bad_wr = nullptr;

    sge.addr = addr;
    sge.length = length;
    sge.lkey = lkey;

    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;

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

MemoryRegion::MemoryRegion(
    rdma_util::Arc<ProtectionDomain> pd,
    rdma_util::Arc<void> buffer_with_deleter,
    uint64_t length
) noexcept(false) {
    auto addr = buffer_with_deleter.get();

    this->inner_buffer_with_deleter_ = buffer_with_deleter;
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

MemoryRegion::MemoryRegion(rdma_util::Arc<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
    this->inner_buffer_with_deleter_ = nullptr;
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

Box<MemoryRegion> MemoryRegion::create(
    rdma_util::Arc<ProtectionDomain> pd,
    rdma_util::Arc<void> buffer_with_deleter,
    uint64_t length
) noexcept(false) {
    return Box<MemoryRegion>(new MemoryRegion(pd, buffer_with_deleter, length));
}

Box<MemoryRegion> MemoryRegion::create(rdma_util::Arc<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false
) {
    return Box<MemoryRegion>(new MemoryRegion(pd, addr, length));
}

rdma_util::Arc<TcclContext> TcclContext::create(Box<RcQueuePair> qp, uint64_t dop) noexcept(false) {
    return rdma_util::Arc<TcclContext>(new TcclContext(std::move(qp), dop));
}

void TcclContext::initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false) {
    rdma_util::Arc<std::atomic<bool>> finalized = std::make_shared<std::atomic<bool>>(false);
    auto local_recv_request_queue = Queue<Ticket>(new moodycamel::ConcurrentQueue<Ticket>());
    auto remote_recv_request_queue = Queue<Ticket>(new moodycamel::ConcurrentQueue<Ticket>());
    auto recv_command_queue = Queue<Command>(new moodycamel::ConcurrentQueue<Command>());
    auto send_command_queue = Queue<Command>(new moodycamel::ConcurrentQueue<Command>());

    rdma_util::Arc<RcQueuePair> shared_qp = std::move(qp);

    std::thread t1(
        thread_post_send,
        shared_qp,
        dop,
        finalized,
        send_command_queue,
        local_recv_request_queue,
        remote_recv_request_queue
    );

    std::thread t2(
        thread_post_recv,
        shared_qp,
        dop,
        finalized,
        recv_command_queue,
        local_recv_request_queue,
        remote_recv_request_queue
    );

    this->thread_post_send_ = std::move(t1);
    this->thread_post_recv_ = std::move(t2);
    this->recv_request_command_queue_ = std::move(recv_command_queue);
    this->send_request_command_queue_ = std::move(send_command_queue);
    this->finalized_ = finalized;
    this->dop_ = dop;
}

TcclContext::TcclContext(Box<RcQueuePair> qp, uint64_t dop) noexcept(false) {
    this->initialize(std::move(qp), dop);
}

Handle TcclContext::send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = lkey;
    Command command = std::make_tuple(ticket, flag);
    this->send_request_command_queue_->enqueue(command);
    return Handle(flag);
}

Handle TcclContext::recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey) {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    Ticket ticket {};
    ticket.stream_id = stream_id;
    ticket.addr = addr;
    ticket.length = length;
    ticket.key = rkey;
    Command command = std::make_tuple(ticket, flag);
    this->recv_request_command_queue_->enqueue(command);
    return Handle(flag);
}

// local_send_request_queue is received from Send Request like NcclSend
// local_recv_request_queue is received from Recv Request like NcclRecv
// remote_recv_request_queue is received from remote side sender.
void TcclContext::thread_post_send(
    rdma_util::Arc<RcQueuePair> qp,
    uint64_t dop,
    rdma_util::Arc<std::atomic<bool>> finalized,
    Queue<Command> send_command_queue,
    Queue<Ticket> local_recv_request_queue,
    Queue<Ticket> remote_recv_request_queue
) noexcept(false) {
    auto host_send_buffer = MemoryRegion::create(
        qp->get_pd(),
        rdma_util::Arc<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * dop
    );

    constexpr uint64_t chunksize = uint64_t(sizeof(Ticket));
    const uint64_t buffer_addr = uint64_t(host_send_buffer->get_addr());
    const uint32_t lkey = host_send_buffer->get_lkey();

    std::queue<Ticket> pending_local_recv_request_queue;
    MultiMap<Ticket> pending_remote_recv_request_map;

    MultiMap<Ticket> pending_local_send_request_map;
    MultiMap<rdma_util::Arc<std::atomic<bool>>> pending_local_send_flag_map;

    std::vector<WorkCompletion> polled_send_wcs;
    polled_send_wcs.reserve(2 * dop);
    ibv_wc* wc_buffer = new ibv_wc[2 * dop];

    Ticket tickets[16];
    std::vector<Command> commands(dop);
    uint64_t count_dequeued = 0;

    std::queue<uint64_t> free_post_send_send_slots;

    uint64_t post_send_write_slot_available = dop;
    uint64_t post_send_send_slot_available = dop;

    for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
        free_post_send_send_slots.push(wr_id);
    }

    while (!finalized->load(std::memory_order_relaxed)) {
        // Received from send request
        if (post_send_send_slot_available > 0) {
            count_dequeued = send_command_queue->try_dequeue_bulk(commands.begin(), dop);
            for (uint64_t i = 0; i < count_dequeued; ++i) {
                auto ticket = std::get<0>(commands[i]);
                auto flag = std::get<1>(commands[i]);
                pending_local_send_request_map[ticket.stream_id].push(ticket);
                pending_local_send_flag_map[ticket.stream_id].push(flag);
            }
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
            uint64_t wr_id = free_post_send_send_slots.front();
            Ticket ticket = pending_local_recv_request_queue.front();
            free_post_send_send_slots.pop();
            pending_local_recv_request_queue.pop();
            memcpy(reinterpret_cast<void*>(buffer_addr + wr_id * chunksize), &ticket, sizeof(Ticket));
            // printf("post_send_send: wr_id %lu stream_id %u\n", wr_id, ticket.stream_id);
            qp->post_send_send(wr_id, buffer_addr + wr_id * chunksize, chunksize, lkey, true);
            post_send_send_slot_available--;
        }

        // Execute remote write args
        for (auto& item : pending_remote_recv_request_map) {
            uint32_t stream_id = item.first;
            std::queue<Ticket>& pending_remote_recv_request_queue = item.second;
            if (post_send_write_slot_available == 0) {
                break;
            } else if (!pending_remote_recv_request_queue.empty()
                       && !pending_local_send_request_map[stream_id].empty()) {
                auto remote_recv_request = pending_remote_recv_request_queue.front();
                auto local_send_request = pending_local_send_request_map[stream_id].front();
                pending_remote_recv_request_queue.pop();
                pending_local_send_request_map[stream_id].pop();

                if (local_send_request.length != remote_recv_request.length) {
                    throw std::runtime_error("Length mismatch");
                }

                auto wr_id = stream_id;
                auto raddr = remote_recv_request.addr;
                auto rkey = remote_recv_request.key;
                auto laddr = local_send_request.addr;
                auto lkey = local_send_request.key;

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

        int ret = qp->poll_send_cq_once(2 * dop, wc_buffer, polled_send_wcs);

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
                    pending_local_send_flag_map[wc.wr_id].front()->store(true);
                    pending_local_send_flag_map[wc.wr_id].pop();
                    post_send_write_slot_available++;
                }
            }
        }
    }

    delete[] wc_buffer;
}

void TcclContext::thread_post_recv(
    rdma_util::Arc<RcQueuePair> qp,
    uint64_t dop,
    rdma_util::Arc<std::atomic<bool>> finalized,
    Queue<Command> recv_command_queue,
    Queue<Ticket> local_recv_request_queue,
    Queue<Ticket> remote_recv_request_queue
) noexcept(false) {
    auto host_recv_buffer = MemoryRegion::create(
        qp->get_pd(),
        rdma_util::Arc<void>(new Ticket[2 * dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * 2 * dop
    );

    constexpr uint64_t chunksize = uint64_t(sizeof(Ticket));
    const uint64_t buffer_addr = uint64_t(host_recv_buffer->get_addr());
    const uint32_t lkey = host_recv_buffer->get_lkey();

    for (uint64_t wr_id = 0; wr_id < 2 * dop; ++wr_id) {
        qp->post_recv(wr_id, buffer_addr + wr_id * chunksize, chunksize, lkey);
    }

    std::vector<WorkCompletion> polled_recv_wcs;
    polled_recv_wcs.reserve(2 * dop);

    ibv_wc* wc_buffer = new ibv_wc[2 * dop];

    std::vector<Command> commands(dop);
    std::vector<Ticket> tickets(dop);

    MultiMap<rdma_util::Arc<std::atomic<bool>>> pending_local_recv_request_map;

    while (!finalized->load(std::memory_order_relaxed)) {
        uint64_t dequeued_count = recv_command_queue->try_dequeue_bulk(commands.begin(), dop);
        for (uint64_t i = 0; i < dequeued_count; ++i) {
            tickets[i] = std::get<0>(commands[i]);
            pending_local_recv_request_map[tickets[i].stream_id].push(std::get<1>(commands[i]));
        }
        local_recv_request_queue->enqueue_bulk(tickets.begin(), dequeued_count);

        int ret = qp->poll_recv_cq_once(2 * dop, wc_buffer, polled_recv_wcs);
        if (ret > 0) {
            for (const auto& wc : polled_recv_wcs) {
                if (wc.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("Failed to receive data");
                } else {
                    auto wr_id = wc.wr_id;
                    if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                        // printf("Polled IBV_WC_RECV_RDMA_WITH_IMM wc: %s\n", wc.to_string().c_str());
                        std::queue<rdma_util::Arc<std::atomic<bool>>>& queue =
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

TcclContext::~TcclContext() {
    this->finalized_->store(true);
    this->thread_post_send_.join();
    this->thread_post_recv_.join();
}

}  // namespace rdma_util
