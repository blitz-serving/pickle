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

void rc_read() {
    ibv_context* sender_context = nullptr;
    open_ib_device(&sender_context, "mlx5_0");
    auto sender = create_rc(sender_context);

    ibv_context* recver_context = nullptr;
    open_ib_device(&recver_context, "mlx5_1");
    auto recver = create_rc(recver_context);

    HandshakeData sender_handshake_data, recver_handshake_data;
    query_handshake_data(sender, &sender_handshake_data);
    query_handshake_data(recver, &recver_handshake_data);

    if (bring_up_rc(sender, recver_handshake_data)) {
        printf("bring_up_rc sender failed\n");
        return;
    };
    if (bring_up_rc(recver, sender_handshake_data)) {
        printf("bring_up_rc recver failed\n");
        return;
    }

    void* send_buffer = malloc(1024 * 10);
    void* recv_buffer = malloc(1024 * 10);

    auto send_mr = ibv_reg_mr(
        sender->pd,
        send_buffer,
        1024 * 10,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
    );

    auto recv_mr = ibv_reg_mr(
        recver->pd,
        recv_buffer,
        1024 * 10,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
    );

    ibv_sge read_sge[10];
    for (int i = 0; i < 10; ++i) {
        read_sge[i].addr = uint64_t(recv_buffer) + i * 1024;
        read_sge[i].length = 1024;
        read_sge[i].lkey = recv_mr->lkey;
    }
    ibv_send_wr read_wr_list[10];
    for (int i = 0; i < 10; ++i) {
        ibv_send_wr wr {};
        wr.wr_id = uint64_t(i);
        wr.next = nullptr;
        wr.sg_list = &read_sge[i];
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = uint32_t(ibv_send_flags::IBV_SEND_SIGNALED);
        wr.wr.rdma.remote_addr = uint64_t(send_buffer) + i * 1024;
        wr.wr.rdma.rkey = send_mr->rkey;
        read_wr_list[i] = wr;

        // read_wr_list[i] = {
        //     .wr_id = uint64_t(i),
        //     .next = nullptr,
        //     .sg_list = &read_sge[i],
        //     .num_sge = 1,
        //     .opcode = IBV_WR_RDMA_READ,
        //     .send_flags = uint32_t(ibv_send_flags::IBV_SEND_SIGNALED),
        //     .wr =
        //         {
        //             .rdma =
        //                 {
        //                     .remote_addr = uint64_t(send_buffer) + i * 1024,
        //                     .rkey = send_mr->rkey,
        //                 },
        //         },
        // };
    };

    ibv_send_wr* bad_wr_list[10];
    for (int i = 0; i < 10; ++i) {
        int ret = ibv_post_send(recver, &read_wr_list[i], &bad_wr_list[i]);
        if (ret) {
            printf("ibv_post_send %d failed error %d\n", i, ret);
            return;
        }
    }

    printf("ibv_post_send\n");

    ibv_wc wc;
    int count = 0;
    while (count < 10) {
        int ret = ibv_poll_cq(recver->send_cq, 1, &wc);
        if (ret > 0) {
            for (int i = 0; i < ret; ++i) {
                printf("ibv_poll_cq %ld %d\n", wc.wr_id, wc.status);
            }
            count += ret;
        } else if (ret < 0) {
            printf("ibv_poll_cq error\n");
            return;
        }
    }
    printf("ibv_poll_cq\n");
}

int main() {
    rc_read();
    return 0;
}