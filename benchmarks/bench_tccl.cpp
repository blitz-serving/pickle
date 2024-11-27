#include <infiniband/verbs.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "rdma_util.h"

#ifdef USE_CUDA

#include "gpu_mem_util.h"

constexpr uint32_t kGPU1 = 0;
constexpr uint32_t kGPU2 = 1;
constexpr uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;

#else

constexpr uint64_t kDataBufferSize = 40ull * 1024 * 1024 * 1024;

#endif

constexpr const char* kRNIC1 = "mlx5_0";
constexpr const char* kRNIC2 = "mlx5_0";

constexpr uint32_t kChunkSize = 64ull * 1024;
constexpr uint64_t dop = 256;
constexpr const ibv_rate kRate = ibv_rate::IBV_RATE_MAX;

static std::atomic<uint64_t> bytes_transferred(0);
static std::atomic<bool> recver_exited(false);

void reporter_thread() {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
        if (recver_exited.load()) {
            return;
        }
    }
}

void sender_thread(
    rdma_util::Arc<rdma_util::TcclContext> context,
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();

    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize / dop; ++i) {
        std::vector<rdma_util::Handle> handles;
        for (uint64_t j = 0; j < dop; ++j) {
            handles.push_back(context->send(stream_id, base_addr + (i * dop + j) * kChunkSize, kChunkSize, lkey));
        }
        for (const auto& handle : handles) {
            handle.wait();
        }
    }

    while (bytes_transferred.load() < kDataBufferSize / kChunkSize / dop * kChunkSize * dop) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
};

void recver_thread(
    rdma_util::Arc<rdma_util::TcclContext> context,
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize / dop; ++i) {
        std::vector<rdma_util::Handle> handles;
        for (uint64_t j = 0; j < dop; ++j) {
            handles.push_back(context->recv(stream_id, base_addr + (i * dop + j) * kChunkSize, kChunkSize, rkey));
        }
        for (const auto& handle : handles) {
            handle.wait();
            bytes_transferred.fetch_add(kChunkSize);
        }
    }
    recver_exited.store(true);
};

int main() {
    auto qp1 = rdma_util::RcQueuePair::create(kRNIC1);
    auto qp2 = rdma_util::RcQueuePair::create(kRNIC2);

    qp1->bring_up(qp2->get_handshake_data(), kRate);
    qp2->bring_up(qp1->get_handshake_data(), kRate);

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

    auto context1 = rdma_util::TcclContext::create(std::move(qp1), false);
    auto context2 = rdma_util::TcclContext::create(std::move(qp2), false);

    std::thread reporter(reporter_thread);
    std::thread sender(sender_thread, context1, data_mr1, 0);
    std::thread recver(recver_thread, context2, data_mr2, 0);

    std::thread([context1, context2] {
        while (1) {
            context1->poll_both();
            context2->poll_both();
            if (recver_exited.load()) {
                return;
            }
        }
    }).join();
    reporter.join();
    sender.join();
    recver.join();

    printf("received\n");

    return 0;
}