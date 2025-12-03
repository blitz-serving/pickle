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

#include "cuda_util.h"
#include "executor_rdma.h"
#include "pickle_logger.h"

constexpr const char* kDevice1 = "ib7s400p0";
constexpr const char* kDevice2 = "ib7s400p1";
constexpr uint32_t kGidIndex = 0;
constexpr int32_t kGPU1 = 0;
constexpr int32_t kGPU2 = 1;
constexpr uint64_t kPacketSize = 1024;
constexpr uint64_t kDataBufferSize = 1ull * 4 * 1024 * 1024 * 1024;
constexpr uint32_t kChunkSize = 1ull * 2 * 1024 * 1024 * 1024;
constexpr ibv_rate kRate = ibv_rate::IBV_RATE_MAX;

static std::atomic<uint64_t> bytes_transferred(0);
static std::atomic<bool> recver_exited(false);
static std::atomic<bool> sender_exited(false);

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
    std::shared_ptr<pickle::RdmaSender> sender,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t unique_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t lkey = data_mr->get_lkey();

    std::vector<std::shared_ptr<pickle::Event>> handles;
    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        handles.push_back(sender->send(unique_id, base_addr + i * kChunkSize, kChunkSize, lkey));
    }

    for (const auto& handle : handles) {
        handle->wait();
    }

    while (bytes_transferred.load() < kDataBufferSize / kChunkSize * kChunkSize) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    sender_exited.store(true);
};

void recver_thread(
    std::shared_ptr<pickle::RdmaRecver> recver,
    std::shared_ptr<rdma_util::MemoryRegion> data_mr,
    uint32_t unique_id
) {
    const uint64_t base_addr = uint64_t(data_mr->get_addr());
    const uint32_t rkey = data_mr->get_rkey();

    std::vector<std::shared_ptr<pickle::Event>> handles;
    for (uint64_t i = 0; i < kDataBufferSize / kChunkSize; ++i) {
        handles.push_back(recver->recv(unique_id, base_addr + i * kChunkSize, kChunkSize, rkey));
    }

    for (const auto& handle : handles) {
        handle->wait();
        bytes_transferred.fetch_add(kChunkSize);
    }
    recver_exited.store(true);
};

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <host|device>\n", argv[0]);
        return -1;
    }

    std::shared_ptr<void> buffer1, buffer2;

    if (strcmp(argv[1], "host") == 0) {
        printf("Using host buffer\n");
        buffer1 = std::shared_ptr<void>(std::malloc(kDataBufferSize), std::free);
        buffer2 = std::shared_ptr<void>(std::malloc(kDataBufferSize), std::free);
        PICKLE_ASSERT(buffer1 != nullptr && buffer2 != nullptr);
    } else if (strcmp(argv[1], "device") == 0) {
        printf("Using device buffer\n");
        buffer1 = std::shared_ptr<void>(cuda_util::try_malloc(kDataBufferSize, kGPU1).unwrap(), cuda_util::free_unwrap);
        buffer2 = std::shared_ptr<void>(cuda_util::try_malloc(kDataBufferSize, kGPU2).unwrap(), cuda_util::free_unwrap);
    } else {
        fprintf(stderr, "Invalid argument: %s. Use 'host' or 'device'.\n", argv[1]);
        return -1;
    }

    auto qp1 = rdma_util::RcQueuePair::create(kDevice1);
    auto qp2 = rdma_util::RcQueuePair::create(kDevice2);

    qp1->bring_up(qp2->get_handshake_data(kGidIndex), kGidIndex, kRate);
    qp2->bring_up(qp1->get_handshake_data(kGidIndex), kGidIndex, kRate);

    std::shared_ptr data_mr1 = rdma_util::MemoryRegion::create(qp1->get_pd(), buffer1.get(), kDataBufferSize);
    std::shared_ptr data_mr2 = rdma_util::MemoryRegion::create(qp2->get_pd(), buffer1.get(), kDataBufferSize);

    std::shared_ptr flusher = pickle::RdmaFlusher::create(qp2->get_pd());

    auto sender = pickle::RdmaSender::create(std::move(qp1), kPacketSize);
    auto recver = pickle::RdmaRecver::create(std::move(qp2), nullptr);

    std::thread thread_reporter(reporter_thread);
    std::thread thread_sender(sender_thread, sender, data_mr1, 20250625);
    std::thread thread_recver(recver_thread, recver, data_mr2, 20250625);
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
