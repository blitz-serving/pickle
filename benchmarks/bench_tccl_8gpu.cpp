#include <infiniband/verbs.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "gpu_mem_util.h"
#include "rdma_util.h"

constexpr const uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;

constexpr const uint64_t KB = 1024ull;
constexpr const uint64_t MB = 1024ull * KB;
constexpr const uint64_t GB = 1024ull * MB;

constexpr const uint32_t kChunkSize = 256ull * KB;
constexpr const uint64_t dop = 256;
constexpr const ibv_rate kRate = ibv_rate::IBV_RATE_MAX;

constexpr const uint64_t kSlotCount = kDataBufferSize / kChunkSize;
constexpr const uint64_t kSendRecvCount = 1024 * GB / kChunkSize;

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
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    std::vector<rdma_util::Handle> handles {};
    handles.reserve(dop);

    for (uint64_t i = 0; i < kSendRecvCount; ++i) {
        auto slot_idx = distr(eng) % kSlotCount;
        handles.push_back(context->send(stream_id, base_addr + slot_idx * kChunkSize, kChunkSize, lkey));
        if (handles.size() == dop) {
            handles.back().wait();
            handles.clear();
        }
    }

    if (handles.size() == dop) {
        handles.back().wait();
        handles.clear();
    }

    while (exited_recver_count.load() < 4) {
        std::this_thread::yield();
    }
    printf("Sender exited\n");
};

void recver_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    std::vector<rdma_util::Handle> handles {};
    handles.reserve(dop);

    for (uint64_t i = 0; i < kSendRecvCount; ++i) {
        auto slot_idx = distr(eng) % kSlotCount;
        handles.push_back(context->recv(stream_id, base_addr + slot_idx * kChunkSize, kChunkSize, rkey));
        if (handles.size() == dop) {
            handles.back().wait();
            bytes_transferred.fetch_add(kChunkSize * dop);
            handles.clear();
        }
    }

    if (handles.size() == dop) {
        handles.back().wait();
        handles.clear();
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

        auto gpu_idx1 = GPUs[i * 2];
        auto gpu_idx2 = GPUs[i * 2 + 1];
        std::shared_ptr<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
            qp1->get_pd(),
            std::shared_ptr<void>(
                gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, gpu_idx1),
                [gpu_idx1](void* p) { gpu_mem_util::free_gpu_buffer(p, gpu_idx1); }
            ),
            kDataBufferSize
        );
        std::shared_ptr<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
            qp2->get_pd(),
            std::shared_ptr<void>(
                gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, gpu_idx2),
                [gpu_idx2](void* p) { gpu_mem_util::free_gpu_buffer(p, gpu_idx2); }
            ),
            kDataBufferSize
        );

        auto context1 = rdma_util::TcclContext::create(std::move(qp1), false);
        auto context2 = rdma_util::TcclContext::create(std::move(qp2), false);

        threads.push_back(std::thread(sender_thread, context1, data_mr1, 0));
        threads.push_back(std::thread(recver_thread, context2, data_mr2, 0));
        threads.push_back(std::thread([context1, context2] {
            while (1) {
                context1->poll_both();
                context2->poll_both();
                if (exited_recver_count.load() >= 4) {
                    return;
                }
            }
        }));
    }

    std::thread reporter(reporter_thread);
    started = 1;

    for (auto& thread : threads) {
        thread.join();
    }
    reporter.join();
    return 0;
}