#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cuda_util.h"
#include "executor_rdma.h"
#include "pickle_logger.h"
#include "rdma_util.h"

using namespace std;

inline std::pair<void*, size_t> count_aligned_chunks(void* ptr, size_t buffer_size, size_t chunk_size) {
    PICKLE_ASSERT(chunk_size > 0 && (chunk_size & (chunk_size - 1)) == 0);  // power of two

    uintptr_t p = reinterpret_cast<uintptr_t>(ptr);
    uintptr_t aligned_start = (p + chunk_size - 1) & ~(chunk_size - 1);
    if (aligned_start - p >= buffer_size) {
        return {nullptr, 0};
    }
    size_t available = buffer_size - (aligned_start - p);
    size_t count = available / chunk_size;
    return {reinterpret_cast<void*>(aligned_start), count};
}

void print_usage(const char* prog) {
    fprintf(
        stderr,
        "Usage: %s [--gpu0 N] [--gpu1 N] [--nic0 DEV] [--nic1 DEV] "
        "[--num-channels N] [--gid-index N] [--buffer-size BYTES] "
        "[--chunk-size BYTES] [--ring-buffer-cap N]\n",
        prog
    );
}

int main(int argc, char** argv) {
    int gpu0 = 0;
    int gpu1 = 1;
    std::string nic0 = "ib7s400p0";
    std::string nic1 = "ib7s400p1";
    int num_channels = 1;
    int gid_index = 0;
    size_t buffer_size = 1ull * 40 * 1024 * 1024 * 1024;
    size_t chunk_size = 1024 * 1024;

    int ring_buffer_cap = 16;

    auto need_value = [&](int idx) -> const char* {
        if (idx + 1 >= argc) {
            fprintf(stderr, "Missing value for %s\n", argv[idx]);
            print_usage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
        return argv[idx + 1];
    };

    for (int i = 1; i < argc; i++) {
        const char* arg = argv[i];
        if (strcmp(arg, "--gpu0") == 0) {
            gpu0 = std::stoi(need_value(i));
            i++;
        } else if (strcmp(arg, "--gpu1") == 0) {
            gpu1 = std::stoi(need_value(i));
            i++;
        } else if (strcmp(arg, "--nic0") == 0) {
            nic0 = need_value(i);
            i++;
        } else if (strcmp(arg, "--nic1") == 0) {
            nic1 = need_value(i);
            i++;
        } else if (strcmp(arg, "--num-channels") == 0) {
            num_channels = std::stoi(need_value(i));
            i++;
        } else if (strcmp(arg, "--gid-index") == 0) {
            gid_index = std::stoi(need_value(i));
            i++;
        } else if (strcmp(arg, "--buffer-size") == 0) {
            buffer_size = std::stoull(need_value(i));
            i++;
        } else if (strcmp(arg, "--chunk-size") == 0) {
            chunk_size = std::stoull(need_value(i));
            i++;
        } else if (strcmp(arg, "--ring-buffer-cap") == 0) {
            ring_buffer_cap = std::stoi(need_value(i));
            i++;
        } else if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", arg);
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    auto buffer0 = std::shared_ptr<void>(cuda_util::try_malloc(buffer_size, gpu0).unwrap(), cuda_util::free_unwrap);
    auto buffer1 = std::shared_ptr<void>(cuda_util::try_malloc(buffer_size, gpu1).unwrap(), cuda_util::free_unwrap);

    shared_ptr pd0 = rdma_util::ProtectionDomain::create(rdma_util::Context::create(nic0.c_str()));
    shared_ptr pd1 = rdma_util::ProtectionDomain::create(rdma_util::Context::create(nic1.c_str()));

    shared_ptr mr0 = rdma_util::MemoryRegion::create(pd0, buffer0, buffer_size);
    shared_ptr mr1 = rdma_util::MemoryRegion::create(pd1, buffer1, buffer_size);

    vector<shared_ptr<pickle::RdmaSender>> senders;
    vector<shared_ptr<pickle::RdmaRecver>> recvers;

    for (int i = 0; i < num_channels; i++) {
        auto qp0 = rdma_util::RcQueuePair::create(pd0);
        auto qp1 = rdma_util::RcQueuePair::create(pd1);
        qp0->bring_up(qp1->get_handshake_data(gid_index), gid_index);
        qp1->bring_up(qp0->get_handshake_data(gid_index), gid_index);
        senders.push_back(pickle::RdmaSender::create(std::move(qp0)));
        recvers.push_back(pickle::RdmaRecver::create(std::move(qp1)));
    }

    atomic_uint64_t counter(0);

    auto rand_send = [=, &counter](shared_ptr<pickle::RdmaSender> sender, shared_ptr<pickle::MemoryRegion> mr) {
        int unique_id = 0;
        vector<shared_ptr<pickle::Event>> events;
        auto [base_addr, num_chunks] = count_aligned_chunks(mr->get_addr(), mr->get_length(), chunk_size);
        std::mt19937 rng(std::hash<std::thread::id> {}(std::this_thread::get_id()));
        std::uniform_int_distribution<uint64_t> dist(0, num_chunks - 1);
        auto lkey = mr->get_lkey();

        for (int i = 0; i < ring_buffer_cap; i++) {
            events.push_back(sender->send(unique_id, uint64_t(base_addr) + dist(rng) * chunk_size, chunk_size, lkey));
        }

        int i = 0;
        while (true) {
            sender->poll();
            if (!events[i]->is_notified()) {
                continue;
            }
            counter.fetch_add(chunk_size);
            events[i] = sender->send(unique_id, uint64_t(base_addr) + dist(rng) * chunk_size, chunk_size, lkey);
            i = (i + 1) % ring_buffer_cap;
        }
    };

    auto rand_recv = [=](shared_ptr<pickle::RdmaRecver> recver, shared_ptr<pickle::MemoryRegion> mr) {
        int unique_id = 0;
        vector<shared_ptr<pickle::Event>> events;
        auto [base_addr, num_chunks] = count_aligned_chunks(mr->get_addr(), mr->get_length(), chunk_size);
        auto rkey = mr->get_rkey();

        for (int i = 0; i < ring_buffer_cap; i++) {
            events.push_back(recver->recv(unique_id, uint64_t(base_addr) + i * chunk_size, chunk_size, rkey));
        }

        int i = 0;
        while (true) {
            recver->poll();
            if (!events[i]->is_notified()) {
                continue;
            }
            events[i]->wait();
            events[i] = recver->recv(unique_id, uint64_t(base_addr) + i * chunk_size, chunk_size, rkey);
            i = (i + 1) % ring_buffer_cap;
        }
    };

    auto reporter = [&counter]() {
        uint64_t prev = 0;
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t curr = counter.load();
            double bandwidth = (curr - prev) * 8.0 / 1e9;
            INFO("Bandwidth: {} Gbps", bandwidth);
            prev = curr;
        }
    };

    vector<thread> threads;
    for (int i = 0; i < num_channels; i++) {
        threads.emplace_back(rand_send, senders[i], mr0);
        threads.emplace_back(rand_recv, recvers[i], mr1);
    }
    threads.emplace_back(reporter);

    while (true) {
        this_thread::sleep_for(chrono::seconds(10));
    }

    return 0;
}
