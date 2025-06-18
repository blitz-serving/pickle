#include "rdma_util.h"

#include <infiniband/verbs.h>
#include <netinet/in.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>

#include "pickle_logger.h"

namespace rdma_util {

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
    DEBUG("rdma_util::Context::~Context()");
    if (this->inner) {
        ibv_close_device(this->inner);
    }
}

std::unique_ptr<Context> Context::create(const char* dev_name) noexcept(false) {
    DEBUG("rdma_util::Context::create() creating context using {}", dev_name);
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
    DEBUG("rdma_util::ProtectionDomain::~ProtectionDomain()");
    if (this->inner) {
        ibv_dealloc_pd(this->inner);
    }
}

CompletionQueue::CompletionQueue(std::shared_ptr<Context> context, int cqe) noexcept(false) {
    this->context_ = context;
    this->inner = ibv_create_cq(context->inner, cqe, nullptr, nullptr, 0);
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to create completion queue");
    }
}

CompletionQueue::~CompletionQueue() {
    DEBUG("rdma_util::CompletionQueue::~CompletionQueue()");
    if (this->inner) {
        ibv_destroy_cq(this->inner);
    }
}

std::shared_ptr<CompletionQueue> CompletionQueue::create(std::shared_ptr<Context> context, int cqe) {
    return std::shared_ptr<CompletionQueue>(new CompletionQueue(context, cqe));
}

RcQueuePair::RcQueuePair(
    std::shared_ptr<ProtectionDomain> pd,
    std::shared_ptr<CompletionQueue> send_cq,
    std::shared_ptr<CompletionQueue> recv_cq
) noexcept(false) {
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

std::unique_ptr<RcQueuePair> RcQueuePair::create(const char* dev_name) noexcept(false) {
    std::shared_ptr<Context> context = Context::create(dev_name);
    auto send_cq = CompletionQueue::create(context);
    auto recv_cq = CompletionQueue::create(context);
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context), send_cq, recv_cq));
}

std::unique_ptr<RcQueuePair> RcQueuePair::create(std::shared_ptr<Context> context) noexcept(false) {
    auto send_cq = CompletionQueue::create(context);
    auto recv_cq = CompletionQueue::create(context);
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context), send_cq, recv_cq));
}

std::unique_ptr<RcQueuePair> RcQueuePair::create(std::shared_ptr<ProtectionDomain> pd) noexcept(false) {
    auto send_cq = CompletionQueue::create(pd->get_context());
    auto recv_cq = CompletionQueue::create(pd->get_context());
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(pd, send_cq, recv_cq));
}

std::unique_ptr<RcQueuePair> RcQueuePair::create(
    std::shared_ptr<ProtectionDomain> pd,
    std::shared_ptr<CompletionQueue> send_cq,
    std::shared_ptr<CompletionQueue> recv_cq
) noexcept(false) {
    return std::unique_ptr<RcQueuePair>(new RcQueuePair(pd, send_cq, recv_cq));
}

RcQueuePair::~RcQueuePair() {
    DEBUG("rdma_util::RcQueuePair::~RcQueuePair()");
    if (this->inner) {
        ibv_destroy_qp(this->inner);
    }
}

ibv_qp_state RcQueuePair::query_qp_state() noexcept(false) {
    ibv_qp_attr attr;
    ibv_qp_init_attr init_attr;
    if (ibv_query_qp(this->inner, &attr, ibv_qp_attr_mask::IBV_QP_STATE, &init_attr)) {
        char buf[128];
        sprintf(buf, "Failed to query QP state. Errno(%d): %s", errno, strerror(errno));
        throw std::runtime_error(buf);
    } else {
        return attr.qp_state;
    }
}

HandshakeData RcQueuePair::get_handshake_data(uint32_t gid_index) noexcept(false) {
    ibv_qp_attr attr_ {};

    attr_.ah_attr.static_rate = ibv_rate::IBV_RATE_100_GBPS;
    ibv_modify_qp(this->inner, &attr_, ibv_qp_attr_mask::IBV_QP_RATE_LIMIT);

    ibv_gid gid;
    if (ibv_query_gid(this->context_->inner, 1, gid_index, &gid)) {
        char buf[128];
        sprintf(buf, "Failed to query gid. Errno(%d): %s", errno, strerror(errno));
        throw std::runtime_error(buf);
    }
    ibv_port_attr attr;
    if (ibv_query_port(this->context_->inner, 1, &attr)) {
        char buf[128];
        sprintf(buf, "Failed to query port. Errno(%d): %s", errno, strerror(errno));
        throw std::runtime_error(buf);
    }

    HandshakeData handshake_data {};
    handshake_data.gid = gid;
    handshake_data.lid = attr.lid;
    handshake_data.qp_num = this->inner->qp_num;
    return handshake_data;
}

void RcQueuePair::bring_up(const HandshakeData& handshake_data, uint32_t gid_index, ibv_rate rate) noexcept(false) {
    ibv_gid gid = handshake_data.gid;
    uint16_t lid = handshake_data.lid;
    uint32_t remote_qp_num = handshake_data.qp_num;

    {
        // Check QP state
        int mask = ibv_qp_attr_mask::IBV_QP_STATE;
        ibv_qp_attr attr;
        ibv_qp_init_attr init_attr;
        int ret = ibv_query_qp(this->inner, &attr, mask, &init_attr);
        PICKLE_ASSERT(ret == 0, "Failed to query QP state");
        if (attr.qp_state == ibv_qp_state::IBV_QPS_RTS) {
            WARN("QP state is already RTS");
            return;
        } else {
            PICKLE_ASSERT(attr.qp_state == ibv_qp_state::IBV_QPS_RESET, "QP state is not RESET");
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

        // No need to set sgid_index for Infiband
        // But it is required for RoCE
        attr.ah_attr.grh.sgid_index = gid_index;

        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        attr.ah_attr.grh.dgid = gid;
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 255;
        attr.ah_attr.dlid = lid;
        attr.ah_attr.static_rate = rate;
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 0;

        if (ibv_modify_qp(this->inner, &attr, mask)) {
            char buf[128] {};
            sprintf(buf, "Failed to modify to RTR. Errno(%d): %s", errno, strerror(errno));
            throw std::runtime_error(buf);
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
            char buf[128] {};
            sprintf(buf, "Failed to modify to RTS. Errno(%d): %s", errno, strerror(errno));
            throw std::runtime_error(buf);
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

int RcQueuePair::post_recv(uint64_t wr_id, uint64_t laddr, uint32_t length, uint32_t lkey) noexcept {
    ibv_sge sge {};
    ibv_recv_wr wr {};
    ibv_recv_wr* bad_wr = nullptr;

    sge.addr = laddr;
    sge.length = length;
    sge.lkey = lkey;

    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    return ibv_post_recv(this->inner, &wr, &bad_wr);
}

int RcQueuePair::wait_until_send_completion(const int expected_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    polled_wcs.resize(expected_num_wcs);
    int ret = 0;
    int num_polled_completions = 0;

    while (num_polled_completions < expected_num_wcs) {
        ret = ibv_poll_cq(
            this->inner->send_cq,
            expected_num_wcs - num_polled_completions,
            polled_wcs.data() + num_polled_completions
        );
        if (ret > 0) {
            num_polled_completions += ret;
        } else if (ret < 0) {
            polled_wcs.reserve(num_polled_completions);
            return ret;
        }
    }
    return 0;
}

int RcQueuePair::wait_until_recv_completion(const int expected_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept {
    if (polled_wcs.size() > 0) {
        polled_wcs.clear();
    }

    polled_wcs.resize(expected_num_wcs);
    int ret = 0;
    int num_polled_completions = 0;

    while (num_polled_completions < expected_num_wcs) {
        ret = ibv_poll_cq(
            this->inner->recv_cq,
            expected_num_wcs - num_polled_completions,
            polled_wcs.data() + num_polled_completions
        );
        if (ret > 0) {
            num_polled_completions += ret;
        } else if (ret < 0) {
            polled_wcs.reserve(num_polled_completions);
            return ret;
        }
    }
    return 0;
}

int RcQueuePair::poll_send_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept {
    polled_wcs.resize(max_num_wcs);
    int ret = ibv_poll_cq(this->inner->send_cq, max_num_wcs, polled_wcs.data());
    if (ret < 0) {
        polled_wcs.clear();
    } else {
        polled_wcs.resize(ret);
    }
    return ret;
}

int RcQueuePair::poll_recv_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept {
    polled_wcs.resize(max_num_wcs);
    int ret = ibv_poll_cq(this->inner->recv_cq, max_num_wcs, polled_wcs.data());
    if (ret < 0) {
        polled_wcs.clear();
    } else {
        polled_wcs.resize(ret);
    }
    return ret;
}

MemoryRegion::MemoryRegion(
    std::shared_ptr<ProtectionDomain> pd,
    std::shared_ptr<void> buffer_with_deleter,
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

MemoryRegion::MemoryRegion(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
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
    DEBUG("rdma_util::MemoryRegion::~MemoryRegion()");
    if (this->inner) {
        ibv_dereg_mr(this->inner);
    }
}

std::unique_ptr<MemoryRegion> MemoryRegion::create(
    std::shared_ptr<ProtectionDomain> pd,
    std::shared_ptr<void> buffer_with_deleter,
    uint64_t length
) noexcept(false) {
    return std::unique_ptr<MemoryRegion>(new MemoryRegion(pd, buffer_with_deleter, length));
}

std::unique_ptr<MemoryRegion>
MemoryRegion::create(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false) {
    return std::unique_ptr<MemoryRegion>(new MemoryRegion(pd, addr, length));
}

}  // namespace rdma_util
