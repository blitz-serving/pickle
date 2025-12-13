#include <cuda_runtime.h>
#include <mpi.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cuda_util.h"
#include "executor_rdma.h"
#include "rdma_util.h"

using std::shared_ptr;
using std::string;
using std::vector;

static void print_usage(const char* prog) {
    std::fprintf(
        stderr,
        "Usage: mpirun -n 2 %s \\n"
        "  [--sender-gpu N] [--recver-gpu N] \\n"
        "  [--sender-nic DEV] [--recver-nic DEV] \\n"
        "  [--num-channels N] [--gid-index N] \\n"
        "  [--size BYTES] \\n"
        "  [--warmup N] [--iters N]\n",
        prog
    );
}

static inline void mpi_fail(const char* msg) {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0) {
        std::fprintf(stderr, "%s\n", msg);
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
}

int main(int argc, char** argv) {
    int sender_gpu = 0;
    int recver_gpu = 1;
    string sender_nic = "ib7s400p0";
    string recver_nic = "ib7s400p1";
    int num_channels = 1;
    int gid_index = 0;

    size_t bytes = 64ull * 1024 * 1024;
    int warmup = 10;
    int iters = 100;

    auto need_value = [&](int idx) -> const char* {
        if (idx + 1 >= argc) {
            std::fprintf(stderr, "Missing value for %s\n", argv[idx]);
            print_usage(argv[0]);
            std::exit(EXIT_FAILURE);
        }
        return argv[idx + 1];
    };

    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (std::strcmp(arg, "--sender-gpu") == 0) {
            sender_gpu = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--recver-gpu") == 0) {
            recver_gpu = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--sender-nic") == 0) {
            sender_nic = need_value(i);
            ++i;
        } else if (std::strcmp(arg, "--recver-nic") == 0) {
            recver_nic = need_value(i);
            ++i;
        } else if (std::strcmp(arg, "--num-channels") == 0) {
            num_channels = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--gid-index") == 0) {
            gid_index = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--size") == 0) {
            bytes = std::stoull(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--warmup") == 0) {
            warmup = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--iters") == 0) {
            iters = std::stoi(need_value(i));
            ++i;
        } else if (std::strcmp(arg, "--help") == 0 || std::strcmp(arg, "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", arg);
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    MPI_Init(&argc, &argv);

    int world_size = 0;
    int rank = 0;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (world_size != 2) {
        mpi_fail("bench_pickle_p2p requires exactly 2 MPI processes.");
    }
    if (num_channels <= 0 || iters <= 0 || warmup < 0) {
        mpi_fail("Invalid args: require num-channels>0, iters>0, warmup>=0.");
    }
    if (bytes == 0) {
        mpi_fail("Invalid args: require size>0.");
    }

    // Choose local device / nic by rank.
    const bool is_sender = (rank == 0);
    const int local_gpu = is_sender ? sender_gpu : recver_gpu;
    const string& local_nic = is_sender ? sender_nic : recver_nic;

    int dev_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&dev_count));
    if (local_gpu < 0 || local_gpu >= dev_count) {
        if (rank == 0) {
            std::fprintf(
                stderr,
                "Invalid GPU id(s): sender=%d recver=%d, device_count=%d\n",
                sender_gpu,
                recver_gpu,
                dev_count
            );
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    CUDA_CHECK(cudaSetDevice(local_gpu));

    // Allocate + register GPU buffer.
    // Allocate exactly 'bytes'; effective bytes may shrink after shard rounding.
    size_t buffer_size = bytes;
    auto buffer = std::shared_ptr<void>(cuda_util::try_malloc(buffer_size, local_gpu).unwrap(), cuda_util::free_unwrap);

    shared_ptr pd = rdma_util::ProtectionDomain::create(rdma_util::Context::create(local_nic.c_str()));
    shared_ptr mr = rdma_util::MemoryRegion::create(pd, buffer, buffer_size);

    const uint64_t base_addr = reinterpret_cast<uint64_t>(mr->get_addr());
    const uint32_t lkey = mr->get_lkey();
    const uint32_t rkey = mr->get_rkey();

    // Create QPs and corresponding sender/recver objects.
    vector<shared_ptr<pickle::RdmaSender>> senders;
    vector<shared_ptr<pickle::RdmaRecver>> recvers;
    senders.reserve(num_channels);
    recvers.reserve(num_channels);

    for (int c = 0; c < num_channels; ++c) {
        auto qp = rdma_util::RcQueuePair::create(pd);

        // Exchange handshake data with peer.
        rdma_util::HandshakeData local_hs = qp->get_handshake_data(gid_index);
        rdma_util::HandshakeData remote_hs;

        MPI_Sendrecv(
            &local_hs,
            sizeof(local_hs),
            MPI_BYTE,
            /*dest=*/1 - rank,
            /*sendtag=*/1000 + c,
            &remote_hs,
            sizeof(remote_hs),
            MPI_BYTE,
            /*source=*/1 - rank,
            /*recvtag=*/1000 + c,
            MPI_COMM_WORLD,
            MPI_STATUS_IGNORE
        );

        qp->bring_up(remote_hs, gid_index);

        if (is_sender) {
            senders.push_back(pickle::RdmaSender::create(std::move(qp)));
        } else {
            recvers.push_back(pickle::RdmaRecver::create(std::move(qp)));
        }
    }

    // Manual sharding across channels.
    vector<size_t> shard_bytes(num_channels, 0);
    {
        size_t chunk_size = bytes / num_channels;
        size_t remains = bytes % num_channels;
        for (int c = 0; c < num_channels; ++c) {
            shard_bytes[c] = chunk_size + ((c == num_channels - 1) ? remains : 0);
        }
    }

    vector<uint64_t> shard_offsets(num_channels, 0);
    {
        uint64_t off = 0;
        for (int c = 0; c < num_channels; ++c) {
            shard_offsets[c] = off;
            off += shard_bytes[c];
        }
    }

    auto do_sender_iter = [&](int unique_id) {
        vector<shared_ptr<pickle::Event>> events;
        events.reserve(num_channels);
        for (int c = 0; c < num_channels; ++c) {
            const size_t sb = shard_bytes[c];
            if (sb == 0)
                continue;
            auto& s = senders[c];
            uint64_t addr = base_addr + shard_offsets[c];
            events.push_back(s->send(unique_id, addr, sb, lkey));
        }
        while (!events.empty()) {
            for (auto& s : senders) {
                s->poll();
            }
            if (events.back()->is_notified()) {
                events.pop_back();
            }
        }
    };

    auto do_recver_iter = [&](int unique_id) {
        vector<shared_ptr<pickle::Event>> events;
        events.reserve(num_channels);
        for (int c = 0; c < num_channels; ++c) {
            const size_t sb = shard_bytes[c];
            if (sb == 0)
                continue;
            auto& r = recvers[c];
            uint64_t addr = base_addr + shard_offsets[c];
            events.push_back(r->recv(unique_id, addr, sb, rkey));
        }
        while (!events.empty()) {
            for (auto& r : recvers) {
                r->poll();
            }
            if (events.back()->is_notified()) {
                events.pop_back();
            }
        }
    };

    auto run_once = [&](int iter, bool measure) -> double {
        const int unique_id = iter;  // used to disambiguate iterations across QPs

        MPI_Barrier(MPI_COMM_WORLD);
        auto t0 = std::chrono::high_resolution_clock::now();

        if (is_sender) {
            do_sender_iter(unique_id);
        } else {
            do_recver_iter(unique_id);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        auto t1 = std::chrono::high_resolution_clock::now();

        if (!measure) {
            return 0.0;
        }
        std::chrono::duration<double> dt = t1 - t0;
        return dt.count();
    };

    // Warmup.
    for (int i = 0; i < warmup; ++i) {
        run_once(i, /*measure=*/false);
    }

    double total_sec = 0.0;
    for (int i = 0; i < iters; ++i) {
        double sec = run_once(warmup + i, /*measure=*/true);
        if (is_sender) {
            total_sec += sec;
            double gbps = (static_cast<double>(bytes) * 8.0) / (sec * 1e9);
            std::printf(
                "Iter %d: %.2f Gbps (%.3f ms), payload %zu bytes, channels %d\n",
                i,
                gbps,
                sec * 1e3,
                bytes,
                num_channels
            );
        }
    }

    if (is_sender) {
        double avg_gbps = (static_cast<double>(bytes) * 8.0 * iters) / (total_sec * 1e9);
        std::printf(
            "Average bandwidth: %.2f Gbps over %d iterations, payload %zu bytes, channels %d\n",
            avg_gbps,
            iters,
            bytes,
            num_channels
        );
    }

    MPI_Finalize();
    return 0;
}
