#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "rdma_util.h"

constexpr const char* kDevice1 = "mlx5_0";
constexpr const char* kDevice2 = "mlx5_1";
constexpr int32_t kGPU1 = 0;
constexpr int32_t kGPU2 = 1;
constexpr uint64_t kChunkSize = 64ull * 1024;
constexpr uint64_t kBufferSize = 4ull * 1024 * 1024 * 1024;
constexpr uint64_t kSlotNum = kBufferSize / kChunkSize;
constexpr uint64_t kOutstandingReads = 16;
constexpr uint64_t kReadCount = 64ull * 1024 * 1024 * 1024 / kChunkSize;
constexpr uint64_t kThreadNum = 8;
constexpr int32_t kGidIndex = 3;

static std::atomic<uint64_t> g_bytes_transferred(0);

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

void* malloc_host_buffer(uint64_t size) {
    void* p = malloc(size);
    ASSERT(p != nullptr);
    return p;
}

void free_host_buffer(void* p) {
    free(p);
}

void* malloc_device_buffer(uint64_t size, int device) {
    void* p = nullptr;
    CUDA_CHECK(cudaSetDevice(device));
    CUDA_CHECK(cudaMalloc(&p, size));
    return p;
}

void free_device_buffer(void* p) {
    CUDA_CHECK(cudaFree(p));
}

int reporter_thread();
int read_thread(
    std::shared_ptr<rdma_util::RcQueuePair> qp,
    std::shared_ptr<rdma_util::MemoryRegion> local_mr,
    std::shared_ptr<rdma_util::MemoryRegion> remote_mr
);

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <host|device>\n", argv[0]);
        return -1;
    }

    std::shared_ptr<void> buffer_1, buffer_2;

    if (strcmp(argv[1], "host") == 0) {
        printf("Using host buffer\n");
        buffer_1 = std::shared_ptr<void>(malloc_host_buffer(kBufferSize), free_host_buffer);
        buffer_2 = std::shared_ptr<void>(malloc_host_buffer(kBufferSize), free_host_buffer);
    } else if (strcmp(argv[1], "device") == 0) {
        printf("Using device buffer\n");
        buffer_1 = std::shared_ptr<void>(malloc_device_buffer(kBufferSize, kGPU1), free_device_buffer);
        buffer_2 = std::shared_ptr<void>(malloc_device_buffer(kBufferSize, kGPU2), free_device_buffer);
    } else {
        fprintf(stderr, "Invalid argument: %s. Use 'host' or 'device'.\n", argv[1]);
        return -1;
    }

    auto buffer_ptr_1 = buffer_1.get();
    auto buffer_ptr_2 = buffer_2.get();

    printf("buffer_addr_1: %p\n", buffer_ptr_1);
    printf("buffer_addr_2: %p\n", buffer_ptr_2);

    {
        std::vector<std::shared_ptr<rdma_util::RcQueuePair>> qp_list_1;
        std::vector<std::shared_ptr<rdma_util::RcQueuePair>> qp_list_2;
        std::vector<std::thread> threads;

        std::shared_ptr<rdma_util::ProtectionDomain> pd1 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kDevice1));
        std::shared_ptr<rdma_util::MemoryRegion> mr1 = rdma_util::MemoryRegion::create(pd1, buffer_ptr_1, kBufferSize);

        std::shared_ptr<rdma_util::ProtectionDomain> pd2 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kDevice2));
        std::shared_ptr<rdma_util::MemoryRegion> mr2 = rdma_util::MemoryRegion::create(pd2, buffer_ptr_2, kBufferSize);

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            std::shared_ptr<rdma_util::RcQueuePair> qp1 = rdma_util::RcQueuePair::create(pd1);
            std::shared_ptr<rdma_util::RcQueuePair> qp2 = rdma_util::RcQueuePair::create(pd2);

            qp1->bring_up(qp2->get_handshake_data(kGidIndex), kGidIndex);
            qp2->bring_up(qp1->get_handshake_data(kGidIndex), kGidIndex);

            qp_list_1.push_back(qp1);
            qp_list_2.push_back(qp2);
        }

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            threads.push_back(std::thread(read_thread, qp_list_1[i], mr1, mr2));
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
        curr = g_bytes_transferred.load(std::memory_order_relaxed);
        bandwidth = (curr - prev) / 1000.0 / 1000.0 / 1000.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
        if (curr >= kThreadNum * kReadCount * kChunkSize) {
            return 0;
        }
    }
}

int read_thread(
    std::shared_ptr<rdma_util::RcQueuePair> qp,
    std::shared_ptr<rdma_util::MemoryRegion> local_mr,
    std::shared_ptr<rdma_util::MemoryRegion> remote_mr
) {
    std::mt19937 rng(std::hash<std::thread::id> {}(std::this_thread::get_id()));
    std::uniform_int_distribution<uint64_t> dist(0, kSlotNum - 1);

    uint64_t base_laddr = uint64_t(local_mr->get_addr());
    uint64_t base_raddr = uint64_t(remote_mr->get_addr());
    const uint32_t lkey = local_mr->get_lkey();
    const uint32_t rkey = remote_mr->get_rkey();

    uint64_t read_posted = 0, read_polled = 0;

    std::vector<ibv_send_wr> wr_list(kOutstandingReads);
    std::vector<ibv_sge> sge_list(kOutstandingReads);
    qp->post_send_wrs(nullptr);

    uint64_t wr_id = 0;

    for (uint64_t i = 0; i < kOutstandingReads; ++i) {
        uint64_t slot_index = dist(rng);
        rdma_util::RcQueuePair::fill_post_send_read_wr(
            wr_id++,
            base_laddr + slot_index * kChunkSize,
            base_raddr + slot_index * kChunkSize,
            kChunkSize,
            lkey,
            rkey,
            true,
            wr_list[i],
            sge_list[i]
        );
    }
    ASSERT(rdma_util::RcQueuePair::freeze_wr_list(wr_list.data(), kOutstandingReads) == 0);
    qp->post_send_wrs(wr_list.data());
    read_posted += kOutstandingReads;

    std::vector<ibv_wc> wcs;
    wcs.reserve(kOutstandingReads);

    int n;
    while (read_polled < kReadCount) {
        n = qp->poll_send_cq_once(kOutstandingReads, wcs);
        if (n < 0) {
            printf("Failed to poll recv CQ\n");
            return -1;
        } else if (n == 0) {
            continue;
        } else {
            read_polled += n;
            g_bytes_transferred.fetch_add(kChunkSize * n, std::memory_order_relaxed);
            for (const auto& wc : wcs) {
                if (wc.status) {
                    printf(
                        "Failed to read data. status=%d, opcode=%d, vendor_err=%d, status_str=\"%s\"\n",
                        wc.status,
                        wc.opcode,
                        wc.vendor_err,
                        ibv_wc_status_str(wc.status)
                    );
                    return -1;
                }
            }

            if (read_polled >= kReadCount) {
                break;
            } else if (read_posted == kReadCount) {
                continue;
            }

            int valid_length = std::min<int>(n, kReadCount - read_posted);
            for (int i = 0; i < valid_length; ++i) {
                uint64_t slot_index = dist(rng);
                rdma_util::RcQueuePair::fill_post_send_read_wr(
                    wr_id++,
                    base_laddr + slot_index * kChunkSize,
                    base_raddr + slot_index * kChunkSize,
                    kChunkSize,
                    lkey,
                    rkey,
                    true,
                    wr_list[i],
                    sge_list[i]
                );
            }
            ASSERT(rdma_util::RcQueuePair::freeze_wr_list(wr_list.data(), valid_length) == 0);
            qp->post_send_wrs(wr_list.data());
            read_posted += valid_length;
        }
    }
    return 0;
}
