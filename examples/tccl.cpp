#include <rdma_util.h>

#include <cstdint>
#include <cstdio>
#include <thread>
#include <utility>
#include <vector>

#include "gpu_mem_util.h"

constexpr uint32_t kGPU1 = 4;
constexpr uint32_t kGPU2 = 7;
constexpr uint64_t kDataBufferSize = 75ull * 1024 * 1024 * 1024;

constexpr const char* kRNIC1 = "mlx5_4";
constexpr const char* kRNIC2 = "mlx5_5";

static bool stopped = false;

int main() {
    auto data_buffer1 = gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU1);
    auto data_buffer2 = gpu_mem_util::malloc_gpu_buffer(kDataBufferSize, kGPU2);

    auto qp1 = rdma_util::RcQueuePair::create(kRNIC1);
    auto qp2 = rdma_util::RcQueuePair::create(kRNIC2);

    qp1->bring_up(qp2->get_handshake_data());
    qp2->bring_up(qp1->get_handshake_data());

    auto data_mr_1 = rdma_util::MemoryRegion::create(qp1->get_pd(), data_buffer1, kDataBufferSize);
    auto data_mr_2 = rdma_util::MemoryRegion::create(qp2->get_pd(), data_buffer2, kDataBufferSize);

    printf("created data mr\n");

    auto context1 = rdma_util::TcclContext::create(std::move(qp1), false);
    auto context2 = rdma_util::TcclContext::create(std::move(qp2), false);

    printf("created tccl context\n");

    std::thread polling_thread([context1, context2]() {
        while (!stopped) {
            context1->poll_both();
            context2->poll_both();
        }
    });

    std::vector<rdma_util::Handle> handles;

    handles.push_back(context1->send(0, uint64_t(data_mr_1->get_addr()), 64 * 1024 * 1024, data_mr_1->get_lkey()));
    handles.push_back(context1->send(0, uint64_t(data_mr_1->get_addr()), 64 * 1024 * 1024, data_mr_1->get_lkey()));
    handles.push_back(context1->send(3, uint64_t(data_mr_1->get_addr()), 64 * 1024 * 1024, data_mr_1->get_lkey()));
    handles.push_back(context1->send(2, uint64_t(data_mr_1->get_addr()), 64 * 1024 * 1024, data_mr_1->get_lkey()));

    std::thread wait_handles([&handles]() {
        printf("waiting for handles\n");
        for (auto& handle : handles) {
            handle.wait();
        }
        printf("all handles are done\n");
    });

    std::this_thread::sleep_for(std::chrono::seconds(3));
    context2->recv(0, uint64_t(data_mr_2->get_addr()), 64 * 1024 * 1024, data_mr_2->get_rkey()).wait();
    context2->recv(2, uint64_t(data_mr_2->get_addr()), 64 * 1024 * 1024, data_mr_2->get_rkey()).wait();
    context2->recv(3, uint64_t(data_mr_2->get_addr()), 64 * 1024 * 1024, data_mr_2->get_rkey()).wait();
    context2->recv(0, uint64_t(data_mr_2->get_addr()), 64 * 1024 * 1024, data_mr_2->get_rkey()).wait();

    printf("received\n");

    wait_handles.join();

    gpu_mem_util::free_gpu_buffer(data_buffer1, kGPU1);
    gpu_mem_util::free_gpu_buffer(data_buffer2, kGPU2);

    stopped = true;
    polling_thread.join();
    return 0;
}