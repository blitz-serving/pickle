#include <rdma_util.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <utility>

#ifdef USE_CUDA

#include "gpu_mem_util.h"

constexpr uint64_t kDataBufferSize = 40ull * 1024 * 1024 * 1024;

#else

constexpr uint64_t kDataBufferSize = 40ull * 1024 * 1024 * 1024;

#endif

constexpr const char* kRNIC1 = "mlx5_0";
constexpr const char* kRNIC2 = "mlx5_5";
constexpr uint32_t kGPU1 = 2;
constexpr uint32_t kGPU2 = 7;

constexpr uint32_t kChunkSize = 4ull * 1024 * 1024;

std::atomic<uint64_t> bytes_transferred;

void reporter_thread() {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
        if (curr >= kDataBufferSize) {
            return;
        }
    }
}

void sender_thread_v2(std::unique_ptr<rdma_util::RcQueuePair> qp, uint32_t device) {
#ifdef USE_CUDA
    auto data_buffer = gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, device);
#else
    auto data_buffer = malloc(kDataBufferSize);
#endif
    auto data_mr = rdma_util::MemoryRegion::create(qp->get_pd(), data_buffer, kDataBufferSize);
    auto context = rdma_util::TcclContext::create(std::move(qp));

    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();

    for (int i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        context->send(0, base_addr + i * kChunkSize, kChunkSize, lkey);
    }

    while (bytes_transferred.load() < kDataBufferSize) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
};

void recver_thread_v2(std::unique_ptr<rdma_util::RcQueuePair> qp, uint32_t device) {
#ifdef USE_CUDA
    auto data_buffer = gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, device);
#else
    auto data_buffer = malloc(kDataBufferSize);
#endif
    auto data_mr = rdma_util::MemoryRegion::create(qp->get_pd(), data_buffer, kDataBufferSize);
    auto context = rdma_util::TcclContext::create(std::move(qp));

    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    for (int i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        context->recv(0, base_addr + i * kChunkSize, kChunkSize, rkey);
        bytes_transferred.fetch_add(kChunkSize);
    }
};

int main() {
    auto qp1 = rdma_util::RcQueuePair::create(kRNIC1);
    auto qp2 = rdma_util::RcQueuePair::create(kRNIC2);

    qp1->bring_up(qp2->get_handshake_data());
    qp2->bring_up(qp1->get_handshake_data());

    std::thread reporter(reporter_thread);
    std::thread sender(sender_thread_v2, std::move(qp1), kGPU1);
    std::thread recver(recver_thread_v2, std::move(qp2), kGPU2);

    reporter.join();
    sender.join();
    recver.join();

    printf("received\n");

    return 0;
}