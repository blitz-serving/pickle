#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "concurrentqueue.h"
#include "gpu_mem_util.h"
#include "rdma_util.h"

constexpr uint32_t kGPU0 = 4;
constexpr uint32_t kGPU1 = 2;
constexpr uint32_t kGPU2 = 6;
constexpr const char* kRNIC0 = "mlx5_4";
constexpr const char* kRNIC1 = "mlx5_1";
constexpr const char* kRNIC2 = "mlx5_5";

constexpr const uint64_t KB = 1024ull;
constexpr const uint64_t MB = 1024ull * KB;
constexpr const uint64_t GB = 1024ull * MB;

constexpr const uint64_t kChunkSize = 256 * KB;
constexpr const uint64_t kDataBufferSize = 75ull * GB;

static bool started = false;

void send_params_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();
    const uint32_t slot_count = kDataBufferSize / kChunkSize;

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    rdma_util::Handle handle;

    while (1) {
        for (uint64_t i = 0; i < 256; ++i) {
            auto slot_idx = distr(eng) % slot_count;
            handle = context->send(stream_id, base_addr + slot_idx * kChunkSize, kChunkSize, lkey);
        }
        handle.wait();
    }
}

void recv_params_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();
    const uint32_t slot_count = kDataBufferSize / kChunkSize;

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    rdma_util::Handle handle;

    while (1) {
        auto s_time = std::chrono::high_resolution_clock::now();
        for (uint64_t j = 0; j < 224; ++j) {
            for (uint64_t i = 0; i < 256; ++i) {
                auto slot_idx = distr(eng) % slot_count;
                handle = context->recv(stream_id, base_addr + slot_idx * kChunkSize, kChunkSize, rkey);
            }
            handle.wait();
        }
        auto e_time = std::chrono::high_resolution_clock::now();
        auto duration_as_micros = std::chrono::duration_cast<std::chrono::microseconds>(e_time - s_time).count();
        double bandwidth = kChunkSize * 224 * 256 / 1024.0 / 1024.0 / 1024.0 / duration_as_micros * 1000000.0;
        printf("[Params]   Latency %.2fms Bandwidth: %.2f GB/s\n", duration_as_micros / 1000.0, bandwidth);
    }
}

void send_kv_cache_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    std::shared_ptr<moodycamel::ConcurrentQueue<uint32_t>> queue,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();
    const uint32_t slot_count = kDataBufferSize / kChunkSize;

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    uint32_t _a;
    std::vector<rdma_util::Handle> handles {};
    handles.reserve(256);

    while (1) {
        if (queue->try_dequeue(_a)) {
            auto s_time = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < 8000; ++i) {
                auto slot_idx = distr(eng) % slot_count;
                handles.push_back(context->send(stream_id, base_addr + slot_idx * kChunkSize, kChunkSize, lkey));
                if (handles.size() == 256) {
                    handles.back().wait();
                    handles.clear();
                }
            }
            if (handles.size()) {
                handles.back().wait();
                handles.clear();
            }
            auto e_time = std::chrono::high_resolution_clock::now();
            auto duration_as_micros = std::chrono::duration_cast<std::chrono::microseconds>(e_time - s_time).count();
            double bandwidth = 256.0 * KB * 8000 / 1024.0 / 1024.0 / 1024.0 / duration_as_micros * 1000000.0;
            printf("[KV Cache] Latency %.2fms Bandwidth: %.2f GB/s\n", duration_as_micros / 1000.0, bandwidth);
        } else {
            std::this_thread::yield();
        }
    }
};

void recv_kv_cache_thread(
    std::shared_ptr<rdma_util::TcclContext> context,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    std::shared_ptr<moodycamel::ConcurrentQueue<uint32_t>> queue,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();
    const uint32_t slot_count = kDataBufferSize / kChunkSize;

    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    while (!started) {
        std::this_thread::yield();
    }

    uint32_t _a;
    std::vector<rdma_util::Handle> handles {};
    handles.reserve(256);

    while (1) {
        if (queue->try_dequeue(_a)) {
            for (uint64_t i = 0; i < 512 * KB * 4000; ++i) {
                auto slot_idx = distr(eng) % slot_count;
                handles.push_back(context->recv(stream_id, base_addr + slot_idx * kChunkSize, kChunkSize, rkey));
                if (handles.size() == 256) {
                    handles.back().wait();
                    handles.clear();
                }
            }
            if (handles.size()) {
                handles.back().wait();
                handles.clear();
            }
        } else {
            std::this_thread::yield();
        }
    }
};

int main() {
    std::shared_ptr<rdma_util::Context> r_context_0 = rdma_util::Context::create(kRNIC0);
    std::shared_ptr<rdma_util::Context> r_context_1 = rdma_util::Context::create(kRNIC1);
    std::shared_ptr<rdma_util::Context> r_context_2 = rdma_util::Context::create(kRNIC2);

    std::shared_ptr<rdma_util::ProtectionDomain> pd0 = rdma_util::ProtectionDomain::create(r_context_0);
    std::shared_ptr<rdma_util::ProtectionDomain> pd1 = rdma_util::ProtectionDomain::create(r_context_1);
    std::shared_ptr<rdma_util::ProtectionDomain> pd2 = rdma_util::ProtectionDomain::create(r_context_2);

    auto qp0_1 = rdma_util::RcQueuePair::create(pd0);
    auto qp0_2 = rdma_util::RcQueuePair::create(pd0);
    auto qp1 = rdma_util::RcQueuePair::create(pd1);
    auto qp2 = rdma_util::RcQueuePair::create(pd2);

    qp0_1->bring_up(qp1->get_handshake_data());
    qp1->bring_up(qp0_1->get_handshake_data());

    qp0_2->bring_up(qp2->get_handshake_data());
    qp2->bring_up(qp0_2->get_handshake_data());

    std::shared_ptr<rdma_util::MemoryRegion> data_mr0 = rdma_util::MemoryRegion::create(
        pd0,
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU0),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU0); }
        ),
        kDataBufferSize
    );
    std::shared_ptr<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
        pd1,
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU1),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU1); }
        ),
        kDataBufferSize
    );
    std::shared_ptr<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
        pd2,
        std::shared_ptr<void>(
            gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU2),
            [](void* p) { gpu_mem_util::free_gpu_buffer(p, kGPU2); }
        ),
        kDataBufferSize
    );

    auto context0_1 = rdma_util::TcclContext::create(std::move(qp0_1), false);
    auto context0_2 = rdma_util::TcclContext::create(std::move(qp0_2), false);
    auto context1 = rdma_util::TcclContext::create(std::move(qp1), false);
    auto context2 = rdma_util::TcclContext::create(std::move(qp2), false);

    auto finished = std::shared_ptr<std::atomic<uint8_t>>(new std::atomic<uint8_t>(0));

    auto send_queue = std::make_shared<moodycamel::ConcurrentQueue<uint32_t>>();
    auto recv_queue = std::make_shared<moodycamel::ConcurrentQueue<uint32_t>>();

    std::thread sender1(send_kv_cache_thread, context0_1, data_mr0, send_queue, 0);
    std::thread recver1(recv_kv_cache_thread, context1, data_mr1, recv_queue, 0);

    std::thread sender2(send_params_thread, context0_2, data_mr0, 0);
    std::thread recver2(recv_params_thread, context2, data_mr2, 0);

    std::vector<std::thread> polling_threads {};
    polling_threads.push_back(std::thread([context1, context2, context0_1, context0_2] {
        while (1) {
            context1->poll_both();
            context2->poll_both();
            context0_1->poll_both();
            context0_2->poll_both();
        }
    }));

    started = true;

    while (1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        send_queue->enqueue(0);
        recv_queue->enqueue(0);
    }
    return 0;
}