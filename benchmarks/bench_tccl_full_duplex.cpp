#include <rdma_util.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <utility>

#ifdef USE_CUDA

#include "gpu_mem_util.h"

constexpr uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;

#else

constexpr uint64_t kDataBufferSize = 40ull * 1024 * 1024 * 1024;

#endif

constexpr const char* kRNIC1 = "mlx5_0";
constexpr const char* kRNIC2 = "mlx5_1";
constexpr uint32_t kGPU1 = 0;
constexpr uint32_t kGPU2 = 3;

constexpr uint32_t kChunkSize = 16ull * 1024 * 1024;

std::atomic<uint64_t> bytes_transferred;

void reporter_thread(rdma_util::Arc<std::atomic<uint8_t>> finished) {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (finished->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
    }
}

void sender_thread(
    rdma_util::Arc<rdma_util::TcclContext> context,
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id,
    rdma_util::Arc<std::atomic<uint8_t>> finished
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();

    for (int i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        context->send(stream_id, base_addr + i * kChunkSize, kChunkSize, lkey).wait();
    }

    while (finished->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
};

void recver_thread(
    rdma_util::Arc<rdma_util::TcclContext> context,
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id,
    rdma_util::Arc<std::atomic<uint8_t>> finished
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    for (int i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        context->recv(stream_id, base_addr + (kDataBufferSize / kChunkSize - i - 1) * kChunkSize, kChunkSize, rkey)
            .wait();
        bytes_transferred.fetch_add(kChunkSize);
    }

    finished->fetch_add(1);
};

int main() {
    auto qp1 = rdma_util::RcQueuePair::create(kRNIC1);
    auto qp2 = rdma_util::RcQueuePair::create(kRNIC2);

    qp1->bring_up(qp2->get_handshake_data());
    qp2->bring_up(qp1->get_handshake_data());

#ifdef USE_CUDA
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
        qp1->get_pd(),
        rdma_util::Arc<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU1),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU1); }
        ),
        kDataBufferSize
    );
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
        qp2->get_pd(),
        rdma_util::Arc<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU2),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU2); }
        ),
        kDataBufferSize
    );
#else
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
        qp1->get_pd(),
        rdma_util::Arc<void>(malloc(kDataBufferSize), free),
        kDataBufferSize
    );
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
        qp2->get_pd(),
        rdma_util::Arc<void>(malloc(kDataBufferSize), free),
        kDataBufferSize
    );
#endif

    auto context1 = rdma_util::TcclContext::create(std::move(qp1));
    auto context2 = rdma_util::TcclContext::create(std::move(qp2));

    auto finished = std::shared_ptr<std::atomic<uint8_t>>(new std::atomic<uint8_t>(0));

    std::thread reporter(reporter_thread, finished);
    std::thread sender1(sender_thread, context1, data_mr1, 0, finished);
    std::thread recver1(recver_thread, context2, data_mr2, 0, finished);

    std::thread sender2(sender_thread, context2, data_mr2, 0, finished);
    std::thread recver2(recver_thread, context1, data_mr1, 0, finished);

    reporter.join();
    sender1.join();
    sender2.join();
    recver1.join();
    recver2.join();

    printf("received\n");

    return 0;
}