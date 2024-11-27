#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "rdma_util.h"

#ifdef USE_CUDA

#include "gpu_mem_util.h"

constexpr uint32_t kGPU1 = 4;
constexpr uint32_t kGPU2 = 6;
constexpr uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;

#else

constexpr uint64_t kDataBufferSize = 40ull * 1024 * 1024 * 1024;

#endif

constexpr const char* kRNIC1 = "mlx5_4";
constexpr const char* kRNIC2 = "mlx5_5";

constexpr const uint64_t KB = 1024ull;
constexpr const uint64_t MB = 1024ull * KB;
constexpr const uint64_t GB = 1024ull * MB;

static std::atomic<uint64_t> bytes_transferred;
static bool started = false;

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
    uint32_t chunk_size,
    uint32_t dop,
    rdma_util::Arc<std::atomic<uint8_t>> finished
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();
    const uint32_t send_recv_count = 1024 * GB / chunk_size;
    const uint32_t slot_count = kDataBufferSize / chunk_size;

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    std::vector<rdma_util::Handle> handles {};
    handles.reserve(dop);

    for (uint64_t i = 0; i < send_recv_count; ++i) {
        auto slot_idx = distr(eng) % slot_count;
        handles.push_back(context->send(stream_id, base_addr + slot_idx * chunk_size, chunk_size, lkey));
        if (handles.size() == dop) {
            handles.back().wait();
            handles.clear();
        }
    }

    if (handles.size() == dop) {
        handles.back().wait();
        handles.clear();
    }

    while (finished->load(std::memory_order_relaxed) < 2) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
};

void recver_thread(
    rdma_util::Arc<rdma_util::TcclContext> context,
    rdma_util::Arc<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id,
    uint32_t chunk_size,
    uint32_t dop,
    std::string name,
    rdma_util::Arc<std::atomic<uint8_t>> finished
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();
    const uint32_t send_recv_count = 1024 * GB / chunk_size;
    const uint32_t slot_count = kDataBufferSize / chunk_size;

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    std::vector<rdma_util::Handle> handles {};
    handles.reserve(dop);

    auto s_time = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i < send_recv_count; ++i) {
        auto slot_idx = distr(eng) % slot_count;
        handles.push_back(context->recv(stream_id, base_addr + slot_idx * chunk_size, chunk_size, rkey));
        if (handles.size() == dop) {
            handles.back().wait();
            auto e_time = std::chrono::high_resolution_clock::now();
            double bandwidth = chunk_size * dop / 1024.0 / 1024.0 / 1024.0
                / std::chrono::duration_cast<std::chrono::microseconds>(e_time - s_time).count() * 1000000.0;
            printf("[%s] Stream %d Bandwidth: %.2f GB/s\n", name.c_str(), stream_id, bandwidth);
            bytes_transferred.fetch_add(chunk_size * dop);
            handles.clear();
            s_time = std::chrono::high_resolution_clock::now();
        }
    }

    if (handles.size() == dop) {
        handles.back().wait();
        handles.clear();
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

    auto context1 = rdma_util::TcclContext::create(std::move(qp1), false);
    auto context2 = rdma_util::TcclContext::create(std::move(qp2), false);

    auto finished = std::shared_ptr<std::atomic<uint8_t>>(new std::atomic<uint8_t>(0));

    std::thread reporter(reporter_thread, finished);

    std::thread sender0(sender_thread, context1, data_mr1, 1, 256 * KB, 64, finished);
    std::thread recver0(recver_thread, context2, data_mr2, 1, 256 * KB, 64, "recver0", finished);

    std::thread sender1(sender_thread, context2, data_mr2, 0, 64 * KB, 64, finished);
    std::thread recver1(recver_thread, context1, data_mr1, 0, 64 * KB, 64, "recver1", finished);

    std::vector<std::thread> polling_threads {};
    polling_threads.push_back(std::thread([context1, context2, finished] {
        while (finished->load(std::memory_order_relaxed) < 2) {
            context1->poll_both();
            context2->poll_both();
        }
    }));

    started = true;

    reporter.join();
    sender0.join();
    sender1.join();
    recver0.join();
    recver1.join();

    for (auto& thread : polling_threads) {
        thread.join();
    }

    printf("received\n");

    return 0;
}