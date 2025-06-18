#include <cuda_runtime.h>
#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "rdma_util.h"

constexpr uint64_t kChunkSize = 512;
constexpr uint64_t kSlotNum = 64;
constexpr uint64_t kBufferSize = 2 * kChunkSize * kSlotNum;
constexpr uint64_t kSendRecvCount = 128ull * 1024 * 1024 * 1024 / kChunkSize;
constexpr uint64_t kThreadNum = 1;

static std::atomic<uint64_t> g_bytes_transferred(0);

int reporter_thread();
int recver_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr);
int sender_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr);

#define CUDA_CHECK(expr)                                                                               \
    do {                                                                                               \
        cudaError_t err = expr;                                                                        \
        if (err != cudaSuccess) {                                                                      \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__, cudaGetErrorString(err)); \
            exit(1);                                                                                   \
        }                                                                                              \
    } while (0)

#define ASSERT(expr)                                                                       \
    do {                                                                                   \
        if (!(expr)) {                                                                     \
            fprintf(stderr, "Assertion failed at %s:%d: %s\n", __FILE__, __LINE__, #expr); \
            exit(1);                                                                       \
        }                                                                                  \
    } while (0)

void* malloc_buffer(uint64_t size) {
    void* p = nullptr;
    // cudaSetDevice(2);
    // CUDA_CHECK(cudaMalloc(&p, size));
    p = malloc(size);
    ASSERT(p != nullptr);
    return p;
}

void free_buffer(void* p) {
    ASSERT(p != nullptr);
    // CUDA_CHECK(cudaFree(p));
    free(p);
}

int main() {
    constexpr const char* kRNIC1 = "mlx5_0";
    constexpr const char* kRNIC2 = "mlx5_1";
    constexpr uint32_t kGidIndex = 3;

    auto send_buffer = std::shared_ptr<void>(malloc_buffer(kBufferSize), free_buffer);
    auto recv_buffer = std::shared_ptr<void>(malloc_buffer(kBufferSize), free_buffer);

    void* send_buffer_ptr = send_buffer.get();
    void* recv_buffer_ptr = recv_buffer.get();

    {
        std::vector<std::thread> threads;
        std::vector<std::shared_ptr<rdma_util::RcQueuePair>> qp_list_1;
        std::vector<std::shared_ptr<rdma_util::RcQueuePair>> qp_list_2;

        std::shared_ptr<rdma_util::ProtectionDomain> pd1 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC1));
        std::shared_ptr<rdma_util::MemoryRegion> mr1 =
            rdma_util::MemoryRegion::create(pd1, send_buffer_ptr, kBufferSize);

        std::shared_ptr<rdma_util::ProtectionDomain> pd2 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC2));
        std::shared_ptr<rdma_util::MemoryRegion> mr2 =
            rdma_util::MemoryRegion::create(pd2, recv_buffer_ptr, kBufferSize);

        printf("mr1: %p\n", mr1->get_addr());
        printf("mr2: %p\n", mr2->get_addr());

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            std::shared_ptr<rdma_util::RcQueuePair> qp1 = rdma_util::RcQueuePair::create(pd1);
            std::shared_ptr<rdma_util::RcQueuePair> qp2 = rdma_util::RcQueuePair::create(pd2);

            qp1->bring_up(qp2->get_handshake_data(kGidIndex), kGidIndex);
            qp2->bring_up(qp1->get_handshake_data(kGidIndex), kGidIndex);

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

int recver_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr) {
    uint64_t base_addr = uint64_t(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();

    uint64_t recv_posted = 0, recv_polled = 0;

    for (uint64_t i = 0; i < 2 * kSlotNum; ++i) {
        qp->post_recv(i, base_addr + i * kChunkSize, kChunkSize, lkey);
        recv_posted++;
    }

    std::vector<ibv_wc> wcs;
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

int sender_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr) {
    uint64_t base_addr = uint64_t(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();

    uint64_t send_posted = 0, send_polled = 0;

    for (uint64_t i = 0; i < kSlotNum; ++i) {
        qp->post_send_send(i, base_addr + i * kChunkSize, kChunkSize, lkey, true);
        send_posted++;
    }

    std::vector<ibv_wc> wcs;
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