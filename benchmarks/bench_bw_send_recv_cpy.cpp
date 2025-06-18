#include <cuda_runtime.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "cuda_util.h"
#include "rdma_util.h"

constexpr uint64_t kChunkSize = 64ull * 1024 * 1024;
constexpr uint64_t kBufferSize = 75ull * 1024 * 1024 * 1024;
constexpr uint64_t kSendRecvCount = kBufferSize / kChunkSize - 1;
constexpr uint64_t kThreadNum = 1;

constexpr const char* kRNIC1 = "mlx5_1";
constexpr const char* kRNIC2 = "mlx5_5";
constexpr uint32_t kGPU1 = 2;
constexpr uint32_t kGPU2 = 7;

static std::atomic<uint64_t> g_bytes_transferred(0);

int reporter_thread();
int recver_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr);
int sender_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr);

int main() {
    auto send_buffer = cuda_util::malloc_gpu_buffer(kBufferSize, kGPU1);
    auto recv_buffer = cuda_util::malloc_gpu_buffer(kBufferSize, kGPU2);

    if (send_buffer == nullptr || recv_buffer == nullptr) {
        printf("Failed to allocate buffer\n");
        return 1;
    }

    {
        std::vector<std::thread> threads;
        std::vector<std::shared_ptr<rdma_util::RcQueuePair>> qp_list_1;
        std::vector<std::shared_ptr<rdma_util::RcQueuePair>> qp_list_2;

        std::shared_ptr<rdma_util::ProtectionDomain> pd1 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC1));
        std::shared_ptr<rdma_util::MemoryRegion> mr1 = rdma_util::MemoryRegion::create(pd1, send_buffer, kBufferSize);

        std::shared_ptr<rdma_util::ProtectionDomain> pd2 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kRNIC2));
        std::shared_ptr<rdma_util::MemoryRegion> mr2 = rdma_util::MemoryRegion::create(pd2, recv_buffer, kBufferSize);

        printf("mr1: %p\n", mr1->get_addr());
        printf("mr2: %p\n", mr2->get_addr());

        std::shared_ptr<rdma_util::RcQueuePair> qp1 = rdma_util::RcQueuePair::create(pd1);
        std::shared_ptr<rdma_util::RcQueuePair> qp2 = rdma_util::RcQueuePair::create(pd2);

        qp1->bring_up(qp2->get_handshake_data(3), 3);
        qp2->bring_up(qp1->get_handshake_data(3), 3);

        auto t1 = std::thread(sender_thread, qp1, mr1);
        auto t2 = std::thread(recver_thread, qp2, mr2);

        std::thread(reporter_thread).join();

        t1.join();
        t2.join();
    }

    cuda_util::free_gpu_buffer(send_buffer);
    cuda_util::free_gpu_buffer(recv_buffer);

    return 0;
}

int reporter_thread() {
    uint64_t prev = 0, curr = 0;
    double bandwidth = 0;

    while (1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curr = g_bytes_transferred.load();
        bandwidth = (curr - prev) / 1024.0 / 1024.0 / 1024.0;
        printf("Bandwidth: %.2f GB/s\n", bandwidth);
        prev = curr;
        if (curr >= kThreadNum * kSendRecvCount * kChunkSize) {
            return 0;
        }
    }
}

int recver_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr) {
    const uint64_t base_addr = (uint64_t)(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();

    uint64_t i = 0;
    std::vector<ibv_wc> polled_wcs;

    while (i < kSendRecvCount) {
        i++;
        qp->post_recv(i, base_addr, kChunkSize, lkey);
        qp->wait_until_recv_completion(1, polled_wcs);
        cudaMemcpy((void*)(base_addr + i * kChunkSize), (void*)(base_addr), kChunkSize, cudaMemcpyDeviceToDevice);
        g_bytes_transferred.fetch_add(kChunkSize);
    }

    return 0;
}

int sender_thread(std::shared_ptr<rdma_util::RcQueuePair> qp, std::shared_ptr<rdma_util::MemoryRegion> mr) {
    const uint64_t base_addr = (uint64_t)(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();

    uint64_t i = 0;
    std::vector<ibv_wc> polled_wcs;

    while (i < kSendRecvCount) {
        i++;
        cudaMemcpy((void*)(base_addr), (void*)(base_addr + i * kChunkSize), kChunkSize, cudaMemcpyDeviceToDevice);
        qp->post_send_send_with_imm(i, base_addr, kChunkSize, lkey, 1, true);
        qp->wait_until_send_completion(1, polled_wcs);
    }

    return 0;
}
