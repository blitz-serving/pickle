#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "rdma_util.h"

#ifdef USE_CUDA

#include "gpu_mem_util.h"
constexpr uint32_t kGPU1 = 2;
constexpr uint32_t kGPU2 = 7;

#endif

constexpr uint64_t kChunkSize = 64ull * 1024 * 1024;
constexpr uint64_t kSlotNum = 64;
constexpr uint64_t kBufferSize = 2 * kChunkSize * kSlotNum;
constexpr uint64_t kSendRecvCount = 128ull * 1024 * 1024 * 1024 / kChunkSize;
constexpr uint64_t kThreadNum = 1;

constexpr const char* kRNIC1 = "mlx5_1";
constexpr const char* kRNIC2 = "mlx5_5";

static std::atomic<uint64_t> g_bytes_transferred(0);

int reporter_thread();
int recver_thread(rdma_util::Arc<rdma_util::RcQueuePair> qp, rdma_util::Arc<rdma_util::MemoryRegion> mr);
int sender_thread(rdma_util::Arc<rdma_util::RcQueuePair> qp, rdma_util::Arc<rdma_util::MemoryRegion> mr);

int main() {
#ifdef USE_CUDA
    auto send_buffer = gpu_mem_util::malloc_gpu_buffer(kBufferSize, kGPU1);
    auto recv_buffer = gpu_mem_util::malloc_gpu_buffer(kBufferSize, kGPU2);
#else
    auto send_buffer = malloc(kBufferSize);
    auto recv_buffer = malloc(kBufferSize);
#endif

    if (send_buffer == nullptr || recv_buffer == nullptr) {
        printf("Failed to allocate buffer\n");
        return 1;
    }

    {
        std::vector<std::thread> threads;
        std::vector<rdma_util::Arc<rdma_util::RcQueuePair>> qp_list_1;
        std::vector<rdma_util::Arc<rdma_util::RcQueuePair>> qp_list_2;

        rdma_util::Arc<rdma_util::ProtectionDomain> pd1 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC1));
        rdma_util::Arc<rdma_util::MemoryRegion> mr1 = rdma_util::MemoryRegion::create(pd1, send_buffer, kBufferSize);

        rdma_util::Arc<rdma_util::ProtectionDomain> pd2 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC2));
        rdma_util::Arc<rdma_util::MemoryRegion> mr2 = rdma_util::MemoryRegion::create(pd2, recv_buffer, kBufferSize);

        printf("mr1: %p\n", mr1->get_addr());
        printf("mr2: %p\n", mr2->get_addr());

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            rdma_util::Arc<rdma_util::RcQueuePair> qp1 = rdma_util::RcQueuePair::create(pd1);
            rdma_util::Arc<rdma_util::RcQueuePair> qp2 = rdma_util::RcQueuePair::create(pd2);

            qp1->bring_up(qp2->get_handshake_data());
            qp2->bring_up(qp1->get_handshake_data());

            qp_list_1.push_back(qp1);
            qp_list_2.push_back(qp2);
        }

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            threads.push_back(std::thread(recver_thread, qp_list_2[i], mr2));
            threads.push_back(std::thread(sender_thread, qp_list_1[i], mr1));
        }

        std::thread(reporter_thread).join();

        for (auto& thread : threads) {
            thread.join();
        }
    }

#ifdef USE_CUDA
    gpu_mem_util::free_gpu_buffer(send_buffer, 0);
    gpu_mem_util::free_gpu_buffer(recv_buffer, 7);
#else
    free(send_buffer);
    free(recv_buffer);
#endif

    return 0;
}

int reporter_thread() {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = g_bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
        if (curr >= kThreadNum * kSendRecvCount * kChunkSize) {
            return 0;
        }
    }
}

int recver_thread(rdma_util::Arc<rdma_util::RcQueuePair> qp, rdma_util::Arc<rdma_util::MemoryRegion> mr) {
    uint64_t base_addr = uint64_t(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();

    uint64_t recv_posted = 0, recv_polled = 0;

    for (uint64_t i = 0; i < 2 * kSlotNum; ++i) {
        qp->post_recv(i, base_addr + i * kChunkSize, kChunkSize, lkey);
        recv_posted++;
    }

    std::vector<rdma_util::WorkCompletion> wcs;
    wcs.reserve(kSlotNum);

    while (recv_polled < kSendRecvCount) {
        if (qp->poll_recv_cq_once(kSlotNum, wcs) < 0) {
            printf("Failed to poll recv CQ\n");
            return -1;
        }
        for (const auto& wc : wcs) {
            recv_polled++;
            g_bytes_transferred.fetch_add(kChunkSize);
            if (recv_posted < kSendRecvCount) {
                qp->post_recv(wc.wr_id, base_addr + wc.wr_id * kChunkSize, kChunkSize, lkey);
                recv_posted++;
            }
        }
    }

    return 0;
}

int sender_thread(rdma_util::Arc<rdma_util::RcQueuePair> qp, rdma_util::Arc<rdma_util::MemoryRegion> mr) {
    uint64_t base_addr = uint64_t(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();

    uint64_t send_posted = 0, send_polled = 0;

    for (uint64_t i = 0; i < kSlotNum; ++i) {
        qp->post_send_send(i, base_addr + i * kChunkSize, kChunkSize, lkey, true);
        send_posted++;
    }

    std::vector<rdma_util::WorkCompletion> wcs;
    wcs.reserve(kSlotNum);

    while (send_polled < kSendRecvCount) {
        if (qp->poll_send_cq_once(kSlotNum, wcs) < 0) {
            printf("Failed to poll recv CQ\n");
            return -1;
        }
        for (const auto& wc : wcs) {
            send_polled++;
            if (send_posted < kSendRecvCount) {
                qp->post_send_send(wc.wr_id, base_addr + wc.wr_id * kChunkSize, kChunkSize, lkey, true);
                send_posted++;
            }
        }
    }

    return 0;
}