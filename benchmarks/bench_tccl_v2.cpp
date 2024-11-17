#include <cuda_runtime.h>
#include <rdma_util.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <utility>

#include "gpu_mem_util.h"

constexpr const char* kRNIC1 = "mlx5_0";
constexpr const char* kRNIC2 = "mlx5_5";
constexpr uint32_t kGPU1 = 2;
constexpr uint32_t kGPU2 = 7;

constexpr uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;
constexpr uint32_t kChunkSize = 16ull * 1024 * 1024;

std::atomic<uint64_t> bytes_transferred;

int gpu_mem_cpy(void* dst, void* src, uint64_t length) {
    return cudaMemcpy(dst, src, length, cudaMemcpyDeviceToDevice);
}

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

void sender_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());

    for (int i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        context->send_v2(stream_id, base_addr + i * kChunkSize, kChunkSize);
    }

    while (bytes_transferred.load() < kDataBufferSize) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
};

void recver_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());

    for (int i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        context->recv_v2(stream_id, base_addr + i * kChunkSize, kChunkSize);
        bytes_transferred.fetch_add(kChunkSize);
    }
};

int main() {
    auto qp1 = rdma_util::RcQueuePair::create(kRNIC1);
    auto qp2 = rdma_util::RcQueuePair::create(kRNIC2);

    qp1->bring_up(qp2->get_handshake_data());
    qp2->bring_up(qp1->get_handshake_data());

    std::shared_ptr<rdma_util::MemoryRegion> device_send_buffer_1 = rdma_util::MemoryRegion::create(
        qp1->get_pd(),
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kChunkSize, kGPU1),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU1); }
        ),
        kChunkSize
    );

    std::shared_ptr<rdma_util::MemoryRegion> device_recv_buffer_1 = rdma_util::MemoryRegion::create(
        qp1->get_pd(),
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kChunkSize, kGPU1),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU1); }
        ),
        kChunkSize
    );

    std::shared_ptr<rdma_util::MemoryRegion> device_send_buffer_2 = rdma_util::MemoryRegion::create(
        qp2->get_pd(),
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kChunkSize, kGPU2),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU2); }
        ),
        kChunkSize
    );

    std::shared_ptr<rdma_util::MemoryRegion> device_recv_buffer_2 = rdma_util::MemoryRegion::create(
        qp2->get_pd(),
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kChunkSize, kGPU2),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU2); }
        ),
        kChunkSize
    );

    std::shared_ptr<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
        qp1->get_pd(),
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU1),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU1); }
        ),
        kDataBufferSize
    );
    std::shared_ptr<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
        qp2->get_pd(),
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU2),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU2); }
        ),
        kDataBufferSize
    );

    auto context1 =
        rdma_util::TcclContext::create_v2(std::move(qp1), device_send_buffer_1, device_recv_buffer_1, gpu_mem_cpy);
    auto context2 =
        rdma_util::TcclContext::create_v2(std::move(qp2), device_send_buffer_2, device_recv_buffer_2, gpu_mem_cpy);

    std::thread reporter(reporter_thread);
    std::thread sender(sender_thread, context1, data_mr1, 0);
    std::thread recver(recver_thread, context2, data_mr2, 0);

    reporter.join();
    sender.join();
    recver.join();

    printf("received\n");

    return 0;
}