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
#include <tuple>
#include <utility>
#include <vector>

#include "pickle_logger.h"

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
    DEBUG("Creating context for device: {}", dev_name);
    return Box<Context>(new Context(dev_name));
}

ProtectionDomain::ProtectionDomain(Arc<Context> context) noexcept(false) {
    this->context_ = context;
    this->inner = ibv_alloc_pd(context->inner);
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to allocate protection domain");
    }
}

Box<ProtectionDomain> ProtectionDomain::create(Arc<Context> context) noexcept(false) {
    return Box<ProtectionDomain>(new ProtectionDomain(context));
}

ProtectionDomain::~ProtectionDomain() {
    if (this->inner) {
        ibv_dealloc_pd(this->inner);
    }
}

CompletionQueue::CompletionQueue(Arc<Context> context, int cqe) noexcept(false) {
    this->context_ = context;
    this->inner = ibv_create_cq(context->inner, cqe, nullptr, nullptr, 0);
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to create completion queue");
    }
}

CompletionQueue::~CompletionQueue() {
    if (this->inner) {
        ibv_destroy_cq(this->inner);
    }
}

Arc<CompletionQueue> CompletionQueue::create(Arc<Context> context, int cqe) {
    return Arc<CompletionQueue>(new CompletionQueue(context, cqe));
}

RcQueuePair::RcQueuePair(Arc<ProtectionDomain> pd, Arc<CompletionQueue> send_cq, Arc<CompletionQueue> recv_cq) noexcept(
    false
) {
    this->pd_ = pd;
    this->context_ = pd->context_;
    this->send_cq_ = send_cq;
    this->recv_cq_ = recv_cq;

    ibv_qp_init_attr init_attr {};
    init_attr.send_cq = send_cq->inner;
    init_attr.recv_cq = recv_cq->inner;
    init_attr.cap.max_send_wr = 128;
    init_attr.cap.max_recv_wr = 1024;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = 64;
    init_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
    init_attr.sq_sig_all = 0;

    this->inner = ibv_create_qp(pd->inner, &init_attr);
}

Box<RcQueuePair> RcQueuePair::create(const char* dev_name) noexcept(false) {
    Arc<Context> context = Context::create(dev_name);
    auto send_cq = CompletionQueue::create(context);
    auto recv_cq = CompletionQueue::create(context);
    return Box<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context), send_cq, recv_cq));
}

Box<RcQueuePair> RcQueuePair::create(Arc<Context> context) noexcept(false) {
    auto send_cq = CompletionQueue::create(context);
    auto recv_cq = CompletionQueue::create(context);
    return Box<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context), send_cq, recv_cq));
}

Box<RcQueuePair> RcQueuePair::create(Arc<ProtectionDomain> pd) noexcept(false) {
    auto send_cq = CompletionQueue::create(pd->get_context());
    auto recv_cq = CompletionQueue::create(pd->get_context());
    return Box<RcQueuePair>(new RcQueuePair(pd, send_cq, recv_cq));
}

Box<RcQueuePair>
RcQueuePair::create(Arc<ProtectionDomain> pd, Arc<CompletionQueue> send_cq, Arc<CompletionQueue> recv_cq) noexcept(false
) {
    return Box<RcQueuePair>(new RcQueuePair(pd, send_cq, recv_cq));
}

RcQueuePair::~RcQueuePair() {
    if (this->inner) {
        ibv_destroy_qp(this->inner);
    }
}

ibv_qp_state RcQueuePair::query_qp_state() noexcept(false) {
    ibv_qp_attr attr;
    ibv_qp_init_attr init_attr;
    if (ibv_query_qp(this->inner, &attr, ibv_qp_attr_mask::IBV_QP_STATE, &init_attr)) {
        throw std::runtime_error("Failed to query QP state");
    } else {
        return attr.qp_state;
    }
}

HandshakeData RcQueuePair::get_handshake_data() noexcept(false) {
    ibv_qp_attr attr_ {};

    attr_.ah_attr.static_rate = ibv_rate::IBV_RATE_100_GBPS;
    ibv_modify_qp(this->inner, &attr_, ibv_qp_attr_mask::IBV_QP_RATE_LIMIT);

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

void RcQueuePair::bring_up(const HandshakeData& handshake_data, ibv_rate rate) noexcept(false) {
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
        attr.qp_access_flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
        attr.pkey_index = 0;
        attr.port_num = 1;

        if (ibv_modify_qp(this->inner, &attr, mask)) {
            throw std::runtime_error("Failed to modify to INIT");
        }
    }

    {
        // Modify QP to ready-to-receive
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN | ibv_qp_attr_mask::IBV_QP_AV | ibv_qp_attr_mask::IBV_QP_RQ_PSN
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
        attr.ah_attr.static_rate = rate;
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
    wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
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
    wr.opcode = ibv_wr_opcode::IBV_WR_SEND_WITH_IMM;
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
    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
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
    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
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
    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;
    wr.imm_data = htonl(imm);

    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_send_wrs(ibv_send_wr* wr_list) noexcept {
    ibv_send_wr* bad_wr = nullptr;
    return ibv_post_send(this->inner, wr_list, &bad_wr);
}

int RcQueuePair::post_recv_wrs(ibv_recv_wr* wr_list) noexcept {
    ibv_recv_wr* bad_wr = nullptr;
    return ibv_post_recv(this->inner, wr_list, &bad_wr);
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

int RcQueuePair::poll_send_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs) {
    polled_wcs.resize(max_num_wcs);
    int ret = ibv_poll_cq(this->inner->send_cq, max_num_wcs, polled_wcs.data());
    if (ret < 0) {
        polled_wcs.clear();
    } else {
        polled_wcs.resize(ret);
    }
    return ret;
}

int RcQueuePair::poll_recv_cq_once(const int max_num_wcs, ibv_wc* wc_buffer, std::vector<WorkCompletion>& polled_wcs) {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

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

int RcQueuePair::poll_recv_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs) {
    polled_wcs.resize(max_num_wcs);
    int ret = ibv_poll_cq(this->inner->recv_cq, max_num_wcs, polled_wcs.data());
    if (ret < 0) {
        polled_wcs.clear();
    } else {
        polled_wcs.resize(ret);
    }
    return ret;
}

MemoryRegion::MemoryRegion(Arc<ProtectionDomain> pd, Arc<void> buffer_with_deleter, uint64_t length) noexcept(false) {
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

MemoryRegion::MemoryRegion(Arc<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
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

Box<MemoryRegion>
MemoryRegion::create(Arc<ProtectionDomain> pd, Arc<void> buffer_with_deleter, uint64_t length) noexcept(false) {
    return Box<MemoryRegion>(new MemoryRegion(pd, buffer_with_deleter, length));
}

Box<MemoryRegion> MemoryRegion::create(Arc<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
    return Box<MemoryRegion>(new MemoryRegion(pd, addr, length));
}

Arc<TcclContext> TcclContext::create(Box<RcQueuePair> qp, bool spawn_polling_thread, uint64_t dop) noexcept(false) {
    Arc<TcclContext> tccl_context = Arc<TcclContext>(new TcclContext());
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

TcclContext::~TcclContext() {
    if (this->background_polling_) {
        this->polling_stopped_.store(true);
        this->polling_thread_.join();
    }
}

void TcclContext::initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false) {
    this->dop_ = dop;

    this->qp_ = std::move(qp);

    this->send_ibv_wc_buffer_ = std::vector<ibv_wc>(2 * dop);
    this->polled_send_wcs_ = std::vector<WorkCompletion>();
    this->polled_send_wcs_.reserve(2 * this->dop_);

    this->recv_ibv_wc_buffer_ = std::vector<ibv_wc>(2 * dop);
    this->polled_recv_wcs_ = std::vector<WorkCompletion>();
    this->polled_recv_wcs_.reserve(2 * this->dop_);

    this->recv_request_command_queue_ = Queue<Command>();
    this->send_request_command_queue_ = Queue<Command>();

    this->host_send_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        Arc<void>(new Ticket[dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * this->dop_
    );
    this->send_buffer_addr_ = uint64_t(this->host_send_buffer_->get_addr());
    this->send_buffer_lkey_ = this->host_send_buffer_->get_lkey();

    this->host_recv_buffer_ = MemoryRegion::create(
        this->qp_->get_pd(),
        Arc<void>(new Ticket[2 * dop], [](Ticket* p) { delete[] p; }),
        sizeof(Ticket) * 2 * this->dop_
    );
    this->recv_buffer_addr_ = uint64_t(this->host_recv_buffer_->get_addr());
    this->recv_buffer_lkey_ = this->host_recv_buffer_->get_lkey();

    this->local_recv_request_queue_ = Queue<Ticket>();
    this->remote_recv_request_queue_ = Queue<Ticket>();

    this->pending_local_recv_request_queue_ = std::queue<Ticket>();
    this->pending_remote_recv_request_map_ = MultiMap<Ticket>();
    this->pending_local_send_request_map_ = MultiMap<Ticket>();
    this->pending_local_send_flag_map_ = MultiMap<Arc<std::atomic<bool>>>();

    this->free_post_send_send_slots_ = std::queue<uint64_t>();
    this->post_send_write_slot_available_ = this->dop_;
    this->post_send_send_slot_available_ = this->dop_;
    for (uint64_t wr_id = 0; wr_id < dop; ++wr_id) {
        this->free_post_send_send_slots_.push(wr_id);
    }

    this->pending_recv_request_count_ = 0;
    this->pending_local_recv_request_map_ = MultiMap<Arc<std::atomic<bool>>>();
    for (uint64_t wr_id = 0; wr_id < 2 * dop; ++wr_id) {
        this->qp_->post_recv(
            wr_id,
            this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
            sizeof(Ticket),
            this->recv_buffer_lkey_
        );
    }
}

Handle TcclContext::send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey, uint32_t padding) {
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

Handle TcclContext::recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey, uint32_t padding) {
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

void TcclContext::poll_both_inner() noexcept(false) {
    this->poll_recv_one_round_inner();
    this->poll_send_one_round_inner();
}

void TcclContext::poll_send_one_round_inner() noexcept(false) {
    ASSERT(this->send_ibv_wc_buffer_.size() > 0, "WC buffer is empty");

    std::vector<Command> commands(this->dop_);
    std::vector<Ticket> tickets(this->dop_);

    uint64_t count_dequeued = 0;

    // Received from send request
    if (this->post_send_send_slot_available_ > 0) {
        count_dequeued = this->send_request_command_queue_.try_dequeue_bulk(commands.begin(), this->dop_);
        for (uint64_t i = 0; i < count_dequeued; ++i) {
            auto ticket = std::get<0>(commands[i]);
            auto flag = std::get<1>(commands[i]);
            this->pending_local_send_request_map_[ticket.stream_id].push(ticket);
            this->pending_local_send_flag_map_[ticket.stream_id].push(flag);
        }
    }

    // Received from recv request
    count_dequeued = this->local_recv_request_queue_.try_dequeue_bulk(tickets.begin(), this->dop_);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        this->pending_local_recv_request_queue_.push(tickets[i]);
    }

    // Received from thread_post_recv
    count_dequeued = this->remote_recv_request_queue_.try_dequeue_bulk(tickets.begin(), this->dop_);
    for (uint64_t i = 0; i < count_dequeued; ++i) {
        this->pending_remote_recv_request_map_[tickets[i].stream_id].push(tickets[i]);
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

            if (local_send_request.length != remote_recv_request.length) {
                throw std::runtime_error("Length mismatch");
            }

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

    if (ret < 0) {
        throw std::runtime_error("Failed to poll send CQ");
    } else if (ret > 0) {
        for (const auto& wc : this->polled_send_wcs_) {
            if (wc.status != ibv_wc_status::IBV_WC_SUCCESS) {
                throw std::runtime_error("Failed to send data");
            } else if (wc.opcode == ibv_wc_opcode::IBV_WC_SEND) {
                this->free_post_send_send_slots_.push(wc.wr_id);
                this->post_send_send_slot_available_++;
            } else if (wc.opcode == ibv_wc_opcode::IBV_WC_RDMA_WRITE) {
                this->pending_local_send_flag_map_[wc.wr_id].front()->store(true);
                this->pending_local_send_flag_map_[wc.wr_id].pop();
                this->post_send_write_slot_available_++;
            }
        }
    }
}

void TcclContext::poll_recv_one_round_inner() noexcept(false) {
    ASSERT(this->recv_ibv_wc_buffer_.size() > 0, "WC buffer is empty");

    std::vector<Command> commands(this->dop_);
    std::vector<Ticket> tickets(this->dop_);

    if (this->pending_recv_request_count_ < 2 * this->dop_) {
        const uint64_t dequeued_count =
            this->recv_request_command_queue_.try_dequeue_bulk(commands.begin(), this->dop_);
        for (uint64_t i = 0; i < dequeued_count; ++i) {
            tickets[i] = std::get<0>(commands[i]);
            this->pending_local_recv_request_map_[tickets[i].stream_id].push(std::get<1>(commands[i]));
            this->pending_recv_request_count_++;
        }
        this->local_recv_request_queue_.enqueue_bulk(tickets.begin(), dequeued_count);
    }

    int ret = this->qp_->poll_recv_cq_once(
        this->recv_ibv_wc_buffer_.size(),
        this->recv_ibv_wc_buffer_.data(),
        this->polled_recv_wcs_
    );
    if (ret > 0) {
        for (const auto& wc : this->polled_recv_wcs_) {
            if (wc.status != ibv_wc_status::IBV_WC_SUCCESS) {
                throw std::runtime_error("Failed to receive data");
            } else {
                auto wr_id = wc.wr_id;
                if (wc.opcode == ibv_wc_opcode::IBV_WC_RECV_RDMA_WITH_IMM) {
                    std::queue<Arc<std::atomic<bool>>>& queue = this->pending_local_recv_request_map_[wc.imm_data];
                    queue.front()->store(1);
                    queue.pop();
                    this->pending_recv_request_count_--;
                } else {
                    Ticket ticket {};
                    memcpy(
                        &ticket,
                        reinterpret_cast<void*>(this->recv_buffer_addr_ + wr_id * sizeof(Ticket)),
                        sizeof(Ticket)
                    );
                    this->remote_recv_request_queue_.enqueue(ticket);
                }
                this->qp_->post_recv(
                    wr_id,
                    this->recv_buffer_addr_ + wr_id * sizeof(Ticket),
                    sizeof(Ticket),
                    this->recv_buffer_lkey_
                );
            }
        }
    } else if (ret < 0) {
        throw std::runtime_error("Failed to poll recv CQ");
    }
}

}  // namespace rdma_util
