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

struct HandshakeData {
    ibv_gid gid;
    uint16_t lid;
    uint32_t qp_num;
    uint32_t sq_psn;
    uint32_t rq_psn;
};

// int client_thread() {
//     rdma_util::Result_t result;
//     int ret;

//     // ibv_context* context = nullptr;
//     // result = rdma_util::open_ib_device(&context, "mlx5_0");
//     // if (result != rdma_util::Result_t::SUCCESS) {
//     //     printf("open_ib_device failed %d\n", result);
//     //     return 1;
//     // }

//     // ibv_pd* pd = ibv_alloc_pd(context);
//     // if (pd == nullptr) {
//     //     printf("ibv_alloc_pd failed\n");
//     //     return 1;
//     // } else {
//     //     printf("ibv_alloc_pd succeeded\n");
//     // }

//     rdma_addrinfo* addr_info = nullptr;
//     rdma_addrinfo hints = {};
//     hints.ai_port_space = RDMA_PS_TCP;
//     ret = rdma_getaddrinfo("192.168.118.1", "12345", &hints, &addr_info);
//     if (ret) {
//         printf("rdma_getaddrinfo: %s\n", gai_strerror(ret));
//         return 1;
//     } else {
//         printf("rdma_getaddrinfo succeeded\n");
//     }

//     ibv_qp_init_attr init_attr {
//         .qp_context = nullptr,
//         .send_cq = nullptr,
//         .recv_cq = nullptr,
//         .srq = nullptr,
//         .cap =
//             {
//                 .max_send_wr = 128,
//                 .max_recv_wr = 512,
//                 .max_send_sge = 16,
//                 .max_recv_sge = 16,
//                 .max_inline_data = 0,
//             },
//         .qp_type = IBV_QPT_RC,
//         .sq_sig_all = 0,
//     };

//     rdma_cm_id* identifer = nullptr;
//     ret = rdma_create_ep(&identifer, addr_info, nullptr, &init_attr);
//     if (ret) {
//         printf("rdma_create_ep failed\n");
//         return 1;
//     } else {
//         printf("rdma_create_ep succeeded\n");
//     }

//     int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

//     // uint8_t* send_buffer = new uint8_t[1024 * 10];
//     void* send_buffer = rdma_util::malloc_gpu_buffer(1024 * 10, "10:00.0");

//     ibv_mr* send_mr = ibv_reg_mr(identifer->pd, static_cast<void*>(send_buffer), 1024 * 10, access);
//     if (!send_mr) {
//         printf("ibv_reg_mr failed\n");
//         return 1;
//     } else {
//         printf("ibv_reg_mr succeeded\n");
//     }

//     ret = rdma_connect(identifer, nullptr);
//     if (ret) {
//         printf("rdma_connect failed\n");
//         return 1;
//     } else {
//         printf("rdma_connect succeeded\n");
//     }

//     ibv_qp_attr qp_attr;
//     ibv_qp_init_attr init_attr2;
//     ret = ibv_query_qp(
//         identifer->qp,
//         &qp_attr,
//         ibv_qp_attr_mask::IBV_QP_CUR_STATE | ibv_qp_attr_mask::IBV_QP_CAP,
//         &init_attr2
//     );
//     if (ret) {
//         printf("ibv_query_qp failed\n");
//         return 1;
//     } else {
//         printf("ibv_query_qp succeeded\n");
//     }

//     printf("qp_attr.qp_state %d\n", qp_attr.qp_state);
//     printf("qp_attr.qp_attr.cap.max_send_wr %d\n", qp_attr.cap.max_send_wr);
//     printf("qp_attr.qp_attr.cap.max_recv_wr %d\n", qp_attr.cap.max_recv_wr);

//     ibv_sge send_sge[10];
//     for (int i = 0; i < 10; ++i) {
//         send_sge[i].addr = reinterpret_cast<uint64_t>(send_buffer) + i * 1024;
//         send_sge[i].length = 1024;
//         send_sge[i].lkey = send_mr->lkey;
//     }

//     ibv_send_wr send_wr_list[10];
//     for (int i = 0; i < 10; ++i) {
//         uint32_t flags;
//         if (i == 9) {
//             flags = ibv_send_flags::IBV_SEND_SIGNALED;
//         } else {
//             flags = 0;
//         }
//         send_wr_list[i] = {
//             .wr_id = static_cast<uint64_t>(i),
//             .next = nullptr,
//             .sg_list = &send_sge[i],
//             .num_sge = 1,
//             .opcode = IBV_WR_SEND,
//             .send_flags = ibv_send_flags::IBV_SEND_SIGNALED,
//         };
//     }

//     std::this_thread::sleep_for(std::chrono::milliseconds(100));

//     ibv_send_wr* bad_wr;
//     for (int i = 0; i < 5; ++i) {
//         ret = ibv_post_send(identifer->qp, &send_wr_list[i], &bad_wr);
//     }

//     std::this_thread::sleep_for(std::chrono::milliseconds(100));
//     ibv_wc work_completion_list[10];
//     memset(work_completion_list, 0, sizeof(work_completion_list));
//     printf("[client] ibv_poll_cq send_cq %d\n", ibv_poll_cq(identifer->send_cq, 10, work_completion_list));

//     for (int i = 5; i < 10; ++i) {
//         ret = ibv_post_send(identifer->qp, &send_wr_list[i], &bad_wr);
//     }

//     std::this_thread::sleep_for(std::chrono::milliseconds(500));

//     memset(work_completion_list, 0, sizeof(work_completion_list));
//     printf("[client] ibv_poll_cq send_cq %d\n", ibv_poll_cq(identifer->send_cq, 10, work_completion_list));

//     std::this_thread::sleep_for(std::chrono::milliseconds(500));

//     return 0;
// }

// int server_thread() {
//     rdma_util::Result_t result;
//     int ret;

//     // ibv_context* context = nullptr;
//     // result = rdma_util::open_ib_device(&context, "mlx5_1");
//     // if (result != rdma_util::Result_t::SUCCESS) {
//     //     printf("open_ib_device failed\n");
//     //     return 1;
//     // }

//     // ibv_pd* pd = ibv_alloc_pd(context);
//     // if (!pd) {
//     //     printf("ibv_alloc_pd failed\n");
//     //     return 1;
//     // } else {
//     //     printf("ibv_alloc_pd succeeded\n");
//     // }

//     rdma_addrinfo* addr_info = nullptr;
//     rdma_addrinfo hints = {};
//     memset(&hints, 0, sizeof(hints));
//     hints.ai_flags = RAI_PASSIVE;
//     hints.ai_port_space = RDMA_PS_TCP;
//     ret = rdma_getaddrinfo(NULL, "12345", &hints, &addr_info);
//     if (ret) {
//         printf("rdma_getaddrinfo: %s\n", gai_strerror(ret));
//         return 1;
//     } else {
//         printf("rdma_getaddrinfo succeeded\n");
//     }

//     rdma_cm_id* listen_id = nullptr;
//     ibv_qp_init_attr init_attr {
//         .qp_context = nullptr,
//         .send_cq = nullptr,
//         .recv_cq = nullptr,
//         .srq = nullptr,
//         .cap =
//             {
//                 .max_send_wr = 128,
//                 .max_recv_wr = 512,
//                 .max_send_sge = 16,
//                 .max_recv_sge = 16,
//                 .max_inline_data = 0,
//             },
//         .qp_type = IBV_QPT_RC,
//         .sq_sig_all = 0,
//     };

//     ret = rdma_create_ep(&listen_id, addr_info, nullptr, &init_attr);
//     if (ret) {
//         printf("rdma_create_ep failed\n");
//         return 1;
//     } else {
//         printf("rdma_create_ep succeeded\n");
//     }

//     ret = rdma_listen(listen_id, 0);
//     if (ret) {
//         printf("rdma_listen failed\n");
//         return 1;
//     } else {
//         printf("rdma_listen succeeded\n");
//     }

//     rdma_cm_id* identifer = nullptr;
//     ret = rdma_get_request(listen_id, &identifer);
//     if (ret) {
//         printf("rdma_get_request failed\n");
//         return 1;
//     } else {
//         printf("rdma_get_request succeeded\n");
//     }

//     ret = rdma_accept(identifer, nullptr);
//     if (ret) {
//         printf("rdma_accept failed\n");
//         return 1;
//     } else {
//         printf("rdma_accept succeeded\n");
//     }

//     ibv_qp_attr qp_attr;
//     ibv_qp_init_attr init_attr2;
//     ret = ibv_query_qp(
//         identifer->qp,
//         &qp_attr,
//         ibv_qp_attr_mask::IBV_QP_CUR_STATE | ibv_qp_attr_mask::IBV_QP_CAP,
//         &init_attr2
//     );
//     if (ret) {
//         printf("ibv_query_qp failed\n");
//         return 1;
//     } else {
//         printf("ibv_query_qp succeeded\n");
//     }

//     printf("qp_attr.qp_state %d\n", qp_attr.qp_state);
//     printf("qp_attr.qp_attr.cap.max_send_wr %d\n", qp_attr.cap.max_send_wr);
//     printf("qp_attr.qp_attr.cap.max_recv_wr %d\n", qp_attr.cap.max_recv_wr);

//     int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

//     // uint8_t* recv_buffer = new uint8_t[1024 * 10];
//     void* recv_buffer = rdma_util::malloc_gpu_buffer(1024 * 10, "16:00.0");

//     ibv_mr* recv_mr = ibv_reg_mr(identifer->pd, static_cast<void*>(recv_buffer), 1024 * 10, access);
//     if (!recv_mr) {
//         printf("ibv_reg_mr failed\n");
//         return 1;
//     } else {
//         printf("ibv_reg_mr succeeded\n");
//     }

//     const int MAX_NUM_WR = 10;
//     ibv_sge recv_sge[10];
//     for (int i = 0; i < 10; ++i) {
//         recv_sge[i].addr = reinterpret_cast<uint64_t>(recv_buffer) + i * 1024;
//         recv_sge[i].length = 1024;
//         recv_sge[i].lkey = recv_mr->lkey;
//     }
//     ibv_recv_wr recv_wr_list[10];
//     for (int i = 0; i < 10; ++i) {
//         ibv_recv_wr recv_wr = {
//             .wr_id = static_cast<uint64_t>(i),
//             .next = nullptr,
//             .sg_list = &recv_sge[i],
//             .num_sge = 1,
//         };
//         recv_wr_list[i] = recv_wr;
//     }

//     ibv_recv_wr* bad_wr;
//     for (int i = 0; i < 10; ++i) {
//         ret = ibv_post_recv(identifer->qp, &recv_wr_list[i], &bad_wr);
//     }

//     std::this_thread::sleep_for(std::chrono::milliseconds(500));

//     ibv_wc work_completion_list[10];
//     memset(work_completion_list, 0, sizeof(work_completion_list));
//     auto b = work_completion_list;

//     int c = 0;
//     int count = 0;
//     while (count < 10) {
//         c = ibv_poll_cq(identifer->recv_cq, 10, b);
//         if (c > 0) {
//             count += c;
//             printf("[server] ibv_poll_cq %d\n", c);
//         } else if (c < 0) {
//             printf("[server] ibv_poll_cq error\n");
//             return 1;
//         }
//     }

//     std::this_thread::sleep_for(std::chrono::milliseconds(500));

//     // rdma_util::close_ib_device(context);

//     return 0;
// }

ibv_qp* create_rc(const char* dev_name) {
    ibv_context* context = nullptr;
    auto result = rdma_util::open_ib_device(&context, dev_name);
    auto pd = ibv_alloc_pd(context);
    auto send_cq = ibv_create_cq(context, 128, nullptr, nullptr, 0);
    auto recv_cq = ibv_create_cq(context, 1024, nullptr, nullptr, 0);
    ibv_qp_init_attr init_attr {
        .send_cq = send_cq,
        .recv_cq = recv_cq,
        .cap =
            {
                .max_send_wr = 128,
                .max_recv_wr = 1024,
                .max_send_sge = 16,
                .max_recv_sge = 16,
                .max_inline_data = 0,
            },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0,
    };

    return ibv_create_qp(pd, &init_attr);
}

int query_handshake_data(ibv_qp* qp, HandshakeData* handshake_data) {
    int ret;

    ibv_gid gid;
    ret = ibv_query_gid(qp->context, 1, 0, &gid);
    if (ret) {
        return ret;
    }

    ibv_port_attr attr;
    ret = ibv_query_port(qp->context, 1, &attr);
    if (ret) {
        return ret;
    }
    uint16_t lid = attr.lid;

    HandshakeData data {
        .gid = gid,
        .lid = lid,
        .qp_num = qp->qp_num,
        .sq_psn = 13,
        .rq_psn = 13,
    };
    return 0;
}

int bring_up_rc(ibv_qp* qp, HandshakeData& data) {
    ibv_gid gid = data.gid;
    uint16_t lid = data.lid;
    uint32_t dest_qp_num = data.qp_num;
    uint32_t sq_psn = data.rq_psn;
    uint32_t rq_psn = data.sq_psn;

    {
        // Modify QP to INIT
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        ibv_qp_attr attr = {
            .qp_state = ibv_qp_state::IBV_QPS_INIT,
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
            .pkey_index = 0,
            .port_num = 1,
        };
        int ret = ibv_modify_qp(qp, &attr, mask);
        if (ret) {
            printf("Failed to modify to INIT\n");
            return ret;
        }
    }

    {
        // Modify QP to ready-to-receive
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_AV | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        ibv_qp_attr attr = {
            .qp_state = ibv_qp_state::IBV_QPS_RTR,
            .path_mtu = IBV_MTU_4096,
            .rq_psn = rq_psn,
            .dest_qp_num = dest_qp_num,
            .ah_attr =
                {
                    .grh {
                        .dgid = gid,
                        .flow_label = 0,
                        .sgid_index = 0,
                        .hop_limit = 255,
                    },
                    .dlid = lid,
                    .is_global = 1,
                    .port_num = 1,
                },
            .max_dest_rd_atomic = 16,
            .min_rnr_timer = 0,
        };

        int ret = ibv_modify_qp(qp, &attr, mask);
        if (ret) {
            printf("Failed to modify to RTR\n");
            return ret;
        }
    }

    {
        // Modify QP to ready-to-send
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT | ibv_qp_attr_mask::IBV_QP_RNR_RETRY | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

        ibv_qp_attr attr = {
            .qp_state = ibv_qp_state::IBV_QPS_RTS,
            .sq_psn = sq_psn,
            .max_rd_atomic = 16,
            .max_dest_rd_atomic = 16,
            .timeout = 20,
            .retry_cnt = 7,
            .rnr_retry = 7,
        };

        int ret = ibv_modify_qp(qp, &attr, mask);
        if (ret) {
            printf("Failed to modify to RTS\n");
            return ret;
        }
    }

    {
        // Check QP state
        ibv_qp_attr attr;
        ibv_qp_init_attr init_attr;
        ibv_query_qp(qp, &attr, ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_CUR_STATE, &init_attr);
        printf("qp_state %d\n", attr.qp_state);
        printf("cur_qp_state %d\n", attr.cur_qp_state);
    }

    return 0;
}

void loopback() {
    auto sender = create_rc("mlx5_0");
    auto recver = create_rc("mlx5_1");

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

    ibv_sge read_sge[10];
    for (int i = 0; i < 10; ++i) {
        read_sge[i].addr = uint64_t(recv_buffer) + i * 1024;
        read_sge[i].length = 1024;
        read_sge[i].lkey = recv_mr->lkey;
    }
    ibv_send_wr read_wr_list[10];
    for (int i = 0; i < 10; ++i) {
        ibv_send_wr read_wr = {
            .wr_id = uint64_t(i),
            .next = nullptr,
            .sg_list = &read_sge[i],
            .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ,
            .send_flags = uint32_t(ibv_send_flags::IBV_SEND_SIGNALED),
            .wr =
                {
                    .rdma =
                        {
                            .remote_addr = uint64_t(send_buffer) + i * 1024,
                            .rkey = send_mr->rkey,
                        },
                },

        };
    };

    ibv_send_wr* bad_wr_list[10];
    for (int i = 0; i < 10; ++i) {
        int ret = ibv_post_send(recver, &read_wr_list[i], &bad_wr_list[i]);
        if (ret) {
            printf("ibv_post_send %d failed error %d\n", i, ret);
        }
    }

    // ibv_sge recv_sge[10];
    // for (int i = 0; i < 10; ++i) {
    //     recv_sge[i].addr = uint64_t(recv_buffer) + i * 1024;
    //     recv_sge[i].length = 1024;
    //     recv_sge[i].lkey = recv_mr->lkey;
    // }
    // ibv_recv_wr recv_wr_list[10];
    // for (int i = 0; i < 10; ++i) {
    //     ibv_recv_wr recv_wr = {
    //         .wr_id = uint64_t(i),
    //         .next = nullptr,
    //         .sg_list = &recv_sge[i],
    //         .num_sge = 1,
    //     };
    //     recv_wr_list[i] = recv_wr;
    // }
    // for (int i = 0; i < 10; ++i) {
    //     if (ibv_post_recv(recver, &recv_wr_list[i], nullptr)) {
    //         printf("ibv_post_recv %d failed\n", i);
    //     }
    // }

    // ibv_sge send_sge[10];
    // for (int i = 0; i < 10; ++i) {
    //     send_sge[i].addr = uint64_t(send_buffer) + i * 1024;
    //     send_sge[i].length = 1024;
    //     send_sge[i].lkey = send_mr->lkey;
    // }
    // ibv_send_wr send_wr_list[10];
    // for (int i = 0; i < 10; ++i) {
    //     send_wr_list[i] = {
    //         .wr_id = uint64_t(i),
    //         .next = nullptr,
    //         .sg_list = &send_sge[i],
    //         .num_sge = 1,
    //         .opcode = IBV_WR_SEND,
    //         .send_flags = i == 9 ? uint32_t(ibv_send_flags::IBV_SEND_SIGNALED) : 0,
    //     };
    // }
    // for (int i = 0; i < 10; ++i) {
    //     if (ibv_post_send(sender, &send_wr_list[i], nullptr)) {
    //         printf("ibv_post_send %d failed\n", i);
    //     }
    // }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ibv_wc work_completion_list[10];
    printf("[sender] ibv_poll_cq send_cq %d\n", ibv_poll_cq(sender->send_cq, 10, work_completion_list));
    printf("[recver] ibv_poll_cq recv_cq %d\n", ibv_poll_cq(recver->recv_cq, 10, work_completion_list));
}

int main() {
    loopback();
    return 0;
}