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

constexpr uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;

#else

constexpr uint64_t kDataBufferSize = 40ull * 1024 * 1024 * 1024;

#endif

constexpr uint32_t kChunkSize = 256ull * 1024;
constexpr uint64_t dop = 4096;
constexpr const ibv_rate kRate = ibv_rate::IBV_RATE_MAX;

static std::atomic<uint64_t> bytes_transferred(0);
static std::atomic<uint64_t> exited_recver_count(0);
static bool started = false;

void reporter_thread() {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
        if (exited_recver_count.load() >= 4) {
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

    while (!started) {
        std::this_thread::yield();
    }

    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize / dop; ++i) {
        std::vector<rdma_util::Handle> handles;
        for (uint64_t j = 0; j < dop; ++j) {
            handles.push_back(context->send(stream_id, base_addr + (i * dop + j) * kChunkSize, kChunkSize, lkey));
        }
        for (const auto& handle : handles) {
            handle.wait();
        }
    }

    while (exited_recver_count.load() < 4) {
        std::this_thread::yield();
    }
    printf("Sender exited\n");
};

void recver_thread(
    rdma_util::Arc<rdma_util::TcclContext> context,
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    while (!started) {
        std::this_thread::yield();
    }

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
    exited_recver_count.fetch_add(1);
    printf("Recver exited\n");
};

int main() {
    std::vector<const char*> RNICs {"mlx5_0", "mlx5_1", "mlx5_4", "mlx5_5"};
    std::vector<uint64_t> GPUs {0, 1, 2, 3, 4, 5, 6, 7};

    std::vector<std::thread> threads;

    for (uint64_t i = 0; i < 4; ++i) {
        auto rnic = RNICs[i];
        auto qp1 = rdma_util::RcQueuePair::create(rnic);
        auto qp2 = rdma_util::RcQueuePair::create(rnic);
        qp1->bring_up(qp2->get_handshake_data(), kRate);
        qp2->bring_up(qp1->get_handshake_data(), kRate);

#ifdef USE_CUDA
        auto gpu_idx1 = GPUs[i * 2];
        auto gpu_idx2 = GPUs[i * 2 + 1];
        rdma_util::Arc<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
            qp1->get_pd(),
            rdma_util::Arc<void>(
                gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, gpu_idx1),
                [gpu_idx1](void* p) { gpu_mem_util::free_gpu_buffer(p, gpu_idx1); }
            ),
            kDataBufferSize
        );
        rdma_util::Arc<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
            qp2->get_pd(),
            rdma_util::Arc<void>(
                gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, gpu_idx2),
                [gpu_idx2](void* p) { gpu_mem_util::free_gpu_buffer(p, gpu_idx2); }
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

        threads.push_back(std::thread(sender_thread, context1, data_mr1, 0));
        threads.push_back(std::thread(recver_thread, context2, data_mr2, 0));
    }

    std::thread reporter(reporter_thread);
    started = 1;

    for (auto& thread : threads) {
        thread.join();
    }
    reporter.join();
    return 0;
}