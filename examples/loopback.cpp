#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "rdma_util.h"

using rdma_util::bring_up_rc;
using rdma_util::create_rc;
using rdma_util::HandshakeData;
using rdma_util::open_ib_device;
using rdma_util::query_handshake_data;

void loopback() {
    ibv_context* context = nullptr;
    open_ib_device(&context, "mlx5_0");
    auto qp = create_rc(context);
    HandshakeData data;
    if (query_handshake_data(qp, &data)) {
        printf("query_handshake_data failed\n");
        return;
    }
    if (bring_up_rc(qp, data)) {
        printf("bring_up_rc failed\n");
        return;
    }

    void* buffer = malloc(1024 * 10);
    auto mr = ibv_reg_mr(
        qp->pd,
        buffer,
        1024 * 10,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
    );

    ibv_sge sge {};
    sge.addr = uint64_t(buffer);
    sge.length = 1024 * 10;
    sge.lkey = mr->lkey;
    ibv_send_wr wr {};
    wr.wr_id = 0;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = uint32_t(ibv_send_flags::IBV_SEND_SIGNALED);
    wr.wr.rdma.remote_addr = uint64_t(buffer);
    wr.wr.rdma.rkey = mr->rkey;

    // ibv_sge sge = {
    //     .addr = uint64_t(buffer),
    //     .length = 1024 * 10,
    //     .lkey = mr->lkey,
    // };
    // ibv_send_wr wr = {
    //     .wr_id = 0,
    //     .next = nullptr,
    //     .sg_list = &sge,
    //     .num_sge = 1,
    //     .opcode = IBV_WR_RDMA_READ,
    //     .send_flags = uint32_t(ibv_send_flags::IBV_SEND_SIGNALED),
    //     .wr =
    //         {
    //             .rdma =
    //                 {
    //                     .remote_addr = uint64_t(buffer),
    //                     .rkey = mr->rkey,
    //                 },
    //         },
    // };

    if (ibv_post_send(qp, &wr, nullptr)) {
        printf("ibv_post_send failed\n");
        return;
    } else {
        printf("ibv_post_send succeeded\n");
    }

    ibv_wc wc;

    while (ibv_poll_cq(qp->send_cq, 1, &wc)) {}
    printf("ibv_poll_cq\n");

    if (ibv_post_send(qp, &wr, nullptr)) {
        printf("ibv_post_send failed\n");
        return;
    } else {
        printf("ibv_post_send succeeded\n");
    }

    while (ibv_poll_cq(qp->send_cq, 1, &wc)) {}
    printf("ibv_poll_cq\n");
}

int main() {
    loopback();
    return 0;
}