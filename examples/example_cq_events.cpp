#include <infiniband/verbs.h>
#include <linux/types.h>
#include <netinet/in.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#define ASSERT(expr)                                                           \
    do {                                                                       \
        if (!(expr)) {                                                         \
            printf("%s:%d Assertion failed: %s\n", __FILE__, __LINE__, #expr); \
            abort();                                                           \
        }                                                                      \
    } while (0)

int main() {
    int gid_index = 3;

    auto device_list = ibv_get_device_list(nullptr);
    ASSERT(device_list != nullptr);

    auto device = device_list[0];
    ASSERT(device != nullptr);

    auto context = ibv_open_device(device);
    ASSERT(context != nullptr);

    auto pd = ibv_alloc_pd(context);
    ASSERT(pd != nullptr);

    auto mr = ibv_reg_mr(
        pd,
        malloc(1024 * 1024 * 2),
        1024 * 1024 * 2,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
    );
    ASSERT(mr != nullptr);

    auto send_comp_channel = ibv_create_comp_channel(context);
    auto recv_comp_channel = ibv_create_comp_channel(context);
    ASSERT(send_comp_channel != nullptr && recv_comp_channel != nullptr);

    // TODO: set comp_vector
    auto send_cq = ibv_create_cq(context, 16, nullptr, send_comp_channel, 0);
    auto recv_cq = ibv_create_cq(context, 16, nullptr, recv_comp_channel, 0);
    ASSERT(send_cq != nullptr && recv_cq != nullptr);

    ibv_qp_init_attr qp_init_attr {};
    qp_init_attr.send_cq = send_cq;
    qp_init_attr.recv_cq = recv_cq;
    qp_init_attr.cap.max_send_wr = 128;
    qp_init_attr.cap.max_recv_wr = 1024;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_inline_data = 64;
    qp_init_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    auto qp = ibv_create_qp(pd, &qp_init_attr);
    ASSERT(qp != nullptr);

    ibv_gid gid {};
    ASSERT(ibv_query_gid(context, 1, gid_index, &gid) == 0);
    ibv_port_attr attr {};
    ASSERT(ibv_query_port(context, 1, &attr) == 0);
    uint16_t lid = attr.lid;
    uint32_t remote_qp_num = qp->qp_num;

    {
        // Check QP state
        int mask = ibv_qp_attr_mask::IBV_QP_STATE;
        ibv_qp_attr attr;
        ibv_qp_init_attr init_attr;
        ASSERT(ibv_query_qp(qp, &attr, mask, &init_attr) == 0);
        ASSERT(attr.qp_state == ibv_qp_state::IBV_QPS_RESET);
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
        ASSERT(ibv_modify_qp(qp, &attr, mask) == 0);
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
        attr.ah_attr.static_rate = ibv_rate::IBV_RATE_MAX;
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 0;
        ASSERT(ibv_modify_qp(qp, &attr, mask) == 0);
    }
    {
        // Modify QP to ready-to-send
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT | ibv_qp_attr_mask::IBV_QP_RNR_RETRY | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

        ibv_qp_attr attr {};
        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.sq_psn = qp->qp_num;
        attr.max_rd_atomic = 16;
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        ASSERT(ibv_modify_qp(qp, &attr, mask) == 0);
    }

    ASSERT(ibv_req_notify_cq(send_cq, 0) == 0);
    ASSERT(ibv_req_notify_cq(recv_cq, 0) == 0);

    ibv_send_wr* bad_send_wr = nullptr;
    ibv_recv_wr* bad_recv_wr = nullptr;

    {
        ibv_sge sge {};
        ibv_recv_wr wr {};
        sge.addr = reinterpret_cast<uint64_t>(mr->addr);
        sge.length = mr->length;
        sge.lkey = mr->lkey;
        wr.wr_id = 0x8;
        wr.next = nullptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        ASSERT(ibv_post_recv(qp, &wr, &bad_recv_wr) == 0);
    }

    {
        ibv_sge sge {};
        ibv_send_wr wr {};
        sge.addr = reinterpret_cast<uint64_t>(mr->addr);
        sge.length = mr->length;
        sge.lkey = mr->lkey;
        wr.wr_id = 0x24;
        wr.next = nullptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.send_flags = static_cast<uint32_t>(ibv_send_flags::IBV_SEND_SIGNALED);
        wr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(mr->addr);
        wr.wr.rdma.rkey = mr->rkey;
        wr.imm_data = reinterpret_cast<__be32>(0xdeadbeef);
        ASSERT(ibv_post_send(qp, &wr, &bad_send_wr) == 0);
    }

    {
        ibv_wc wc;

        ibv_cq* send_cq = nullptr;
        void* send_cq_context = nullptr;
        ASSERT(ibv_get_cq_event(send_comp_channel, &send_cq, &send_cq_context) == 0);
        ibv_ack_cq_events(send_cq, 1);
        ASSERT(ibv_poll_cq(send_cq, 1, &wc) == 1);
        ASSERT(wc.status == ibv_wc_status::IBV_WC_SUCCESS);
        ASSERT(wc.opcode == ibv_wc_opcode::IBV_WC_RDMA_WRITE);

        ibv_cq* recv_cq = nullptr;
        void* recv_cq_context = nullptr;
        ASSERT(ibv_get_cq_event(recv_comp_channel, &recv_cq, &recv_cq_context) == 0);
        ibv_ack_cq_events(recv_cq, 1);
        ASSERT(ibv_poll_cq(recv_cq, 1, &wc) == 1);
        ASSERT(wc.status == ibv_wc_status::IBV_WC_SUCCESS);
        ASSERT(wc.opcode == ibv_wc_opcode::IBV_WC_RECV_RDMA_WITH_IMM);
        ASSERT(wc.imm_data == reinterpret_cast<__be32>(0xdeadbeef));
    }

    return 0;
}