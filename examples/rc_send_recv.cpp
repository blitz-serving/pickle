#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "rdma_util.h"

using rdma_util::bring_up_rc;
using rdma_util::create_rc;
using rdma_util::HandshakeData;
using rdma_util::open_ib_device;
using rdma_util::query_handshake_data;

void rc_send_recv() {
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

    // void* send_buffer = rdma_util::malloc_gpu_buffer(1024 * 10, "16:00.0");
    // void* recv_buffer = rdma_util::malloc_gpu_buffer(1024 * 10, "16:00.0");

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

    ibv_sge recv_sge[10];
    for (int i = 0; i < 10; ++i) {
        recv_sge[i].addr = uint64_t(recv_buffer) + i * 1024;
        recv_sge[i].length = 1024;
        recv_sge[i].lkey = recv_mr->lkey;
    }
    ibv_recv_wr recv_wr_list[10];
    for (int i = 0; i < 10; ++i) {
        recv_wr_list[i] = {
            .wr_id = uint64_t(i),
            .next = nullptr,
            .sg_list = &recv_sge[i],
            .num_sge = 1,
        };
    }
    for (int i = 0; i < 10; ++i) {
        if (ibv_post_recv(recver, &recv_wr_list[i], nullptr)) {
            printf("ibv_post_recv %d failed\n", i);
        }
    }

    ibv_sge send_sge[10];
    for (int i = 0; i < 10; ++i) {
        send_sge[i].addr = uint64_t(send_buffer) + i * 1024;
        send_sge[i].length = 1024;
        send_sge[i].lkey = send_mr->lkey;
    }
    ibv_send_wr send_wr_list[10];
    for (int i = 0; i < 10; ++i) {
        send_wr_list[i] = {
            .wr_id = uint64_t(i),
            .next = nullptr,
            .sg_list = &send_sge[i],
            .num_sge = 1,
            .opcode = IBV_WR_SEND,
            .send_flags = i == 9 ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0,
        };
    }
    for (int i = 0; i < 10; ++i) {
        if (ibv_post_send(sender, &send_wr_list[i], nullptr)) {
            printf("ibv_post_send %d failed\n", i);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    ibv_wc work_completion_list[10];
    printf("[sender] ibv_poll_cq send_cq %d\n", ibv_poll_cq(sender->send_cq, 10, work_completion_list));
    printf("[sender] ibv_poll_cq recv_cq %d\n", ibv_poll_cq(sender->recv_cq, 10, work_completion_list));
    printf("[recver] ibv_poll_cq send_cq %d\n", ibv_poll_cq(recver->send_cq, 10, work_completion_list));
    printf("[recver] ibv_poll_cq recv_cq %d\n", ibv_poll_cq(recver->recv_cq, 10, work_completion_list));
}

int main() {
    rc_send_recv();
    return 0;
}