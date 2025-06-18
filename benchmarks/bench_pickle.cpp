#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "pickle.h"
#include "pickle_logger.h"

constexpr const char* kRNIC1 = "mlx5_0";
constexpr const char* kRNIC2 = "mlx5_1";
constexpr const uint64_t kPacketSize = 256 * 1024;
constexpr const int32_t kGPU1 = 0;
constexpr const int32_t kGPU2 = 1;
constexpr const uint32_t kGidIndex = 3;

constexpr const uint64_t kDataBufferSize = 1ull * 16 * 1024 * 1024 * 1024;
constexpr const uint32_t kChunkSize = 1ull * 1024 * 1024;
constexpr const ibv_rate kRate = ibv_rate::IBV_RATE_MAX;

static std::atomic<uint64_t> bytes_transferred(0);
static std::atomic<bool> recver_exited(false);
static std::atomic<bool> sender_exited(false);

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

void* malloc_buffer(uint64_t size, int device = 0) {
    void* p = nullptr;
    // cudaSetDevice(device);
    // CUDA_CHECK(cudaMalloc(&p, size));
    p = malloc(size);
    ASSERT(p != nullptr);
    return p;
}

void free_buffer(void* p) {
    // CUDA_CHECK(cudaFree(p));
    free(p);
}

void reporter_thread() {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (!recver_exited.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        INFO("Bandwidth: {} GB/s", bandwidth);
        prev = curr;
    }
}

void sender_thread(
    std::shared_ptr<pickle::PickleSender> sender,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();

    std::vector<pickle::Handle> handles;
    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        handles.push_back(sender->send(stream_id, base_addr + i * kChunkSize, kChunkSize, lkey));
    }

    for (const auto& handle : handles) {
        handle.wait();
    }

    while (bytes_transferred.load() < kDataBufferSize / kChunkSize * kChunkSize) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    sender_exited.store(true);
};

void recver_thread(
    std::shared_ptr<pickle::PickleRecver> recver,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t stream_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    std::vector<pickle::Handle> handles;
    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        handles.push_back(recver->recv(stream_id, base_addr + i * kChunkSize, kChunkSize, rkey));
    }

    for (const auto& handle : handles) {
        handle.wait();
        bytes_transferred.fetch_add(kChunkSize);
    }
    recver_exited.store(true);
};

int main() {
    auto qp1 = rdma_util::RcQueuePair::create(kRNIC1);
    auto qp2 = rdma_util::RcQueuePair::create(kRNIC2);

    qp1->bring_up(qp2->get_handshake_data(kGidIndex), kGidIndex, kRate);
    qp2->bring_up(qp1->get_handshake_data(kGidIndex), kGidIndex, kRate);

    std::shared_ptr<rdma_util::MemoryRegion> data_mr1 = rdma_util::MemoryRegion::create(
        qp1->get_pd(),
        std::shared_ptr<void>(malloc_buffer(kDataBufferSize, kGPU1), free_buffer),
        kDataBufferSize
    );
    std::shared_ptr<rdma_util::MemoryRegion> data_mr2 = rdma_util::MemoryRegion::create(
        qp2->get_pd(),
        std::shared_ptr<void>(malloc_buffer(kDataBufferSize, kGPU2), free_buffer),
        kDataBufferSize
    );

    std::shared_ptr<pickle::Flusher> flusher = pickle::Flusher::create(qp2->get_pd());

    auto sender = pickle::PickleSender::create(std::move(qp1), kPacketSize);
    auto recver = pickle::PickleRecver::create(std::move(qp2), flusher);

    std::thread thread_reporter(reporter_thread);
    std::thread thread_sender(sender_thread, sender, data_mr1, 20250102);
    std::thread thread_recver(recver_thread, recver, data_mr2, 20250102);
    std::thread thread_poller([sender, recver, flusher] {
        while (!(recver_exited.load() && sender_exited.load())) {
            sender->poll();
            recver->poll();
            flusher->poll();
        }
    });

    thread_reporter.join();
    thread_sender.join();
    thread_recver.join();
    thread_poller.join();

    return 0;
}