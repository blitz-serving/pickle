#include "rdma_util.h"

#include <infiniband/verbs.h>

#include <cstdio>
#include <stdexcept>

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
    printf("Context destructor\n");
    if (this->inner) {
        ibv_close_device(this->inner);
    }
}

std::shared_ptr<Context> Context::create(const char* dev_name) noexcept(false) {
    return std::shared_ptr<Context>(new Context(dev_name));
}

ProtectionDomain::ProtectionDomain(std::shared_ptr<Context> context) noexcept(false) {
    this->context_ = context;
    this->inner = ibv_alloc_pd(context->inner);
    if (this->inner == nullptr) {
        throw std::runtime_error("Failed to allocate protection domain");
    }
}

std::shared_ptr<ProtectionDomain> ProtectionDomain::create(std::shared_ptr<Context> context) noexcept(false) {
    return std::shared_ptr<ProtectionDomain>(new ProtectionDomain(context));
}

ProtectionDomain::~ProtectionDomain() {
    printf("ProtectionDomain destructor\n");
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

std::shared_ptr<RcQueuePair> RcQueuePair::create(const char* dev_name) noexcept(false) {
    return std::shared_ptr<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(Context::create(dev_name))));
}

std::shared_ptr<RcQueuePair> RcQueuePair::create(std::shared_ptr<Context> context) noexcept(false) {
    return std::shared_ptr<RcQueuePair>(new RcQueuePair(ProtectionDomain::create(context)));
}

std::shared_ptr<RcQueuePair> RcQueuePair::create(std::shared_ptr<ProtectionDomain> pd) noexcept(false) {
    return std::shared_ptr<RcQueuePair>(new RcQueuePair(pd));
}

RcQueuePair::~RcQueuePair() {
    printf("RcQueuePair destructor\n");
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

int RcQueuePair::post_send(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey, bool signaled) noexcept {
    ibv_sge sge {};
    sge.addr = addr;
    sge.length = length;
    sge.lkey = lkey;
    ibv_send_wr wr = {};
    wr.wr_id = wr_id;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = signaled ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0;
    ibv_send_wr* bad_wr;
    return ibv_post_send(this->inner, &wr, &bad_wr);
}

int RcQueuePair::post_recv(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey) noexcept {
    ibv_sge sge {};
    sge.addr = addr;
    sge.length = length;
    sge.lkey = lkey;
    ibv_recv_wr wr = {};
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
                work_completion.status = work_completions[i].status;
                work_completion.wr_id = work_completions[i].wr_id;
                work_completion.bytes_transferred = work_completions[i].byte_len;
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
                work_completion.status = work_completions[i].status;
                work_completion.wr_id = work_completions[i].wr_id;
                work_completion.bytes_transferred = work_completions[i].byte_len;
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

MemoryRegion::MemoryRegion(std::shared_ptr<ProtectionDomain> pd, void* addr, size_t length) noexcept(false) {
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
    printf("MemoryRegion destructor\n");
    if (this->inner) {
        ibv_dereg_mr(this->inner);
    }
}

std::shared_ptr<MemoryRegion>
MemoryRegion::create(std::shared_ptr<ProtectionDomain> pd, void* addr, size_t length) noexcept(false) {
    return std::shared_ptr<MemoryRegion>(new MemoryRegion(pd, addr, length));
}

}  // namespace rdma_util
