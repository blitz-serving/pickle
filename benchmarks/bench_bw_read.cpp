#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "rdma_util.h"

constexpr uint64_t kChunkSize = 64ull * 1024 * 1024;
constexpr uint64_t kSlotNum = 1;
constexpr uint64_t kBufferSize = kChunkSize * kSlotNum;
constexpr uint64_t kReadCount = 128ull * 1024 * 1024 * 1024 / kChunkSize;
constexpr uint64_t kThreadNum = 1;

static std::atomic<uint64_t> g_bytes_transferred(0);

constexpr const char* kDevice1 = "mlx5_1";
constexpr const char* kDevice2 = "mlx5_5";

int reporter_thread();
int read_thread(
    rdma_util::Arc<rdma_util::RcQueuePair> qp,
    rdma_util::Arc<rdma_util::MemoryRegion> local_mr,
    rdma_util::Arc<rdma_util::MemoryRegion> remote_mr
);

int main() {
    auto send_buffer = malloc(kBufferSize);
    auto recv_buffer = malloc(kBufferSize);

    {
        std::vector<rdma_util::Arc<rdma_util::RcQueuePair>> qp_list_1;
        std::vector<rdma_util::Arc<rdma_util::RcQueuePair>> qp_list_2;

        std::vector<std::thread> threads;

        rdma_util::Arc<rdma_util::ProtectionDomain> pd1 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kDevice1));
        rdma_util::Arc<rdma_util::MemoryRegion> mr1 = rdma_util::MemoryRegion::create(pd1, send_buffer, kBufferSize);

        rdma_util::Arc<rdma_util::ProtectionDomain> pd2 =
            rdma_util::ProtectionDomain::create(rdma_util::Context::create(kDevice2));
        rdma_util::Arc<rdma_util::MemoryRegion> mr2 = rdma_util::MemoryRegion::create(pd2, recv_buffer, kBufferSize);

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            rdma_util::Arc<rdma_util::RcQueuePair> qp1 = rdma_util::RcQueuePair::create(pd1);
            rdma_util::Arc<rdma_util::RcQueuePair> qp2 = rdma_util::RcQueuePair::create(pd2);

            qp1->bring_up(qp2->get_handshake_data());
            qp2->bring_up(qp1->get_handshake_data());

            qp_list_1.push_back(qp1);
            qp_list_2.push_back(qp2);
        }

        for (uint64_t i = 0; i < kThreadNum; ++i) {
            threads.push_back(std::thread(read_thread, qp_list_1[i], mr1, mr2));
        }

        std::thread(reporter_thread).join();

        for (auto& thread : threads) {
            thread.join();
        }
    }

    free(send_buffer);
    free(recv_buffer);

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
        if (curr >= kThreadNum * kReadCount * kChunkSize) {
            return 0;
        }
    }
}

int read_thread(
    rdma_util::Arc<rdma_util::RcQueuePair> qp,
    rdma_util::Arc<rdma_util::MemoryRegion> local_mr,
    rdma_util::Arc<rdma_util::MemoryRegion> remote_mr
) {
    uint64_t base_laddr = uint64_t(local_mr->get_addr());
    uint64_t base_raddr = uint64_t(remote_mr->get_addr());
    const uint32_t lkey = local_mr->get_lkey();
    const uint32_t rkey = remote_mr->get_rkey();

    uint64_t read_posted = 0, read_polled = 0;

    for (uint64_t i = 0; i < kSlotNum; ++i) {
        qp->post_send_read(i, base_laddr + i * kChunkSize, base_raddr + i * kChunkSize, kChunkSize, lkey, rkey, true);
        read_posted++;
    }

    std::vector<rdma_util::WorkCompletion> wcs;
    wcs.reserve(kSlotNum);

    while (read_polled < kReadCount) {
        if (qp->poll_send_cq_once(kSlotNum, wcs) < 0) {
            printf("Failed to poll recv CQ\n");
            return -1;
        }
        for (const auto& wc : wcs) {
            if (wc.status) {
                printf("Failed to read data. status: %d\n", wc.status);
                return -1;
            }
            read_polled++;
            g_bytes_transferred.fetch_add(kChunkSize);
            if (read_posted < kReadCount) {
                qp->post_send_read(
                    wc.wr_id,
                    base_laddr + wc.wr_id * kChunkSize,
                    base_raddr + wc.wr_id * kChunkSize,
                    kChunkSize,
                    lkey,
                    rkey,
                    true
                );
                read_posted++;
            }
        }
    }

    return 0;
}
