// examples/benchmark_nvlink_bandwidth.cpp
//
// Measure end-to-end NVLink transfer bandwidth using the NvlinkSender/NvlinkRecver
// executor pair in two processes (fork), similar to examples/test_nvlink_fork.cpp.
//
// Timing is taken on the sender side: each send completes only after receiver finishes
// matching + copy and the ACK is observed by the sender event (ev_send notified).

#include <cuda_runtime.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "cuda_util.h"
#include "executor_nvlink.h"
#include "spsc.h"

using namespace pickle;
using namespace std::chrono_literals;

static constexpr int GPU_SENDER = 0;
static constexpr int GPU_RECVER = 1;

static constexpr const char* TICKET_QUEUE_NAME = "/nvlink_ticket_queue";
static constexpr const char* ACK_QUEUE_NAME = "/nvlink_ack_queue";

constexpr std::size_t QUEUE_BYTES_TICKET = 1 << 20;  // 1MB
constexpr std::size_t QUEUE_BYTES_ACK = 1 << 16;  // 64KB

constexpr std::chrono::milliseconds QUEUE_WAIT_TIMEOUT {5000};

static void cleanup_stale_queues() {
    ::shm_unlink(TICKET_QUEUE_NAME);
    ::shm_unlink(ACK_QUEUE_NAME);
}

struct BenchConfig {
    std::vector<std::size_t> sizes_bytes;
    int warmup_iters = 50;
    int iters = 200;
    bool busy_poll = true;  // if false, poll + tiny sleep
};

static std::size_t parse_size_bytes(const std::string& s) {
    // Accept: plain bytes (e.g. "1048576") or suffix: K, M, G (binary 1024-based).
    if (s.empty())
        return 0;
    char suffix = s.back();
    std::size_t mul = 1;
    std::string num = s;

    if (suffix == 'K' || suffix == 'k') {
        mul = 1024ull;
        num.pop_back();
    } else if (suffix == 'M' || suffix == 'm') {
        mul = 1024ull * 1024ull;
        num.pop_back();
    } else if (suffix == 'G' || suffix == 'g') {
        mul = 1024ull * 1024ull * 1024ull;
        num.pop_back();
    }

    char* end = nullptr;
    unsigned long long v = std::strtoull(num.c_str(), &end, 10);
    if (!end || *end != '\0') {
        std::cerr << "Invalid size: " << s << "\n";
        std::exit(1);
    }
    if (v > std::numeric_limits<std::size_t>::max() / mul) {
        std::cerr << "Size overflow: " << s << "\n";
        std::exit(1);
    }
    return static_cast<std::size_t>(v) * mul;
}

static std::vector<std::size_t> parse_sizes_list(const std::string& csv) {
    std::vector<std::size_t> out;
    std::size_t start = 0;
    while (start < csv.size()) {
        std::size_t comma = csv.find(',', start);
        std::string token = (comma == std::string::npos) ? csv.substr(start) : csv.substr(start, comma - start);
        // trim spaces
        while (!token.empty() && token.front() == ' ')
            token.erase(token.begin());
        while (!token.empty() && token.back() == ' ')
            token.pop_back();
        if (!token.empty())
            out.push_back(parse_size_bytes(token));
        if (comma == std::string::npos)
            break;
        start = comma + 1;
    }
    out.erase(std::remove(out.begin(), out.end(), 0), out.end());
    if (out.empty()) {
        std::cerr << "Empty --sizes\n";
        std::exit(1);
    }
    return out;
}

static BenchConfig parse_args(int argc, char** argv) {
    BenchConfig cfg;
    // Defaults: sweep a few common sizes.
    cfg.sizes_bytes = {4 * 1024, 64 * 1024, 1 * 1024 * 1024, 16 * 1024 * 1024};

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto need_val = [&](const char* opt) {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << opt << "\n";
                std::exit(1);
            }
            return std::string(argv[++i]);
        };

        if (a == "--sizes") {
            cfg.sizes_bytes = parse_sizes_list(need_val("--sizes"));
        } else if (a == "--size") {
            cfg.sizes_bytes = {parse_size_bytes(need_val("--size"))};
        } else if (a == "--warmup") {
            cfg.warmup_iters = std::stoi(need_val("--warmup"));
        } else if (a == "--iters") {
            cfg.iters = std::stoi(need_val("--iters"));
        } else if (a == "--sleep-poll") {
            cfg.busy_poll = false;
        } else if (a == "--busy-poll") {
            cfg.busy_poll = true;
        } else if (a == "--help" || a == "-h") {
            std::cout << "Usage: benchmark_nvlink_bandwidth [options]\n"
                      << "Options:\n"
                      << "  --sizes <csv>        e.g. 4K,64K,1M,16M\n"
                      << "  --size <one>         single size, e.g. 8M\n"
                      << "  --warmup <N>         warmup iters per size (default 50)\n"
                      << "  --iters <N>          measured iters per size (default 200)\n"
                      << "  --busy-poll          tight polling (default)\n"
                      << "  --sleep-poll         poll + short sleep (less CPU)\n";
            std::exit(0);
        } else {
            std::cerr << "Unknown arg: " << a << "\n";
            std::exit(1);
        }
    }

    if (cfg.warmup_iters < 0 || cfg.iters <= 0) {
        std::cerr << "Invalid iters\n";
        std::exit(1);
    }
    for (auto s : cfg.sizes_bytes) {
        if (s == 0) {
            std::cerr << "Invalid size 0\n";
            std::exit(1);
        }
    }
    return cfg;
}

static void poll_pause(bool busy_poll) {
    if (busy_poll) {
        // Yield to reduce contention a bit while still being effectively busy.
        std::this_thread::yield();
    } else {
        std::this_thread::sleep_for(50us);
    }
}

static bool write_exact(int fd, const void* buf, std::size_t n) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w < 0)
            return false;
        p += static_cast<std::size_t>(w);
        n -= static_cast<std::size_t>(w);
    }
    return true;
}

static bool read_exact(int fd, void* buf, std::size_t n) {
    uint8_t* p = reinterpret_cast<uint8_t*>(buf);
    while (n > 0) {
        ssize_t r = ::read(fd, p, n);
        if (r <= 0)
            return false;
        p += static_cast<std::size_t>(r);
        n -= static_cast<std::size_t>(r);
    }
    return true;
}

// Child -> Parent message: ready for a given size index; Parent -> Child: go-ahead is implicit by send loop,
// but we also do a "done" barrier per size to keep runs aligned.
struct PipeMsg {
    uint32_t kind;  // 1=ready, 2=done, 3=error
    uint32_t size_idx;  // which size in cfg.sizes_bytes
};

int run_child_recver(const BenchConfig& cfg, int child_to_parent_fd, int parent_to_child_fd) {
    std::cout << "[Child] Receiver started (PID=" << getpid() << ")\n";

    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count <= GPU_RECVER) {
        std::cerr << "[Child] Need at least " << (GPU_RECVER + 1) << " GPUs, got " << device_count << "\n";
        PipeMsg m {3, 0};
        (void)write_exact(child_to_parent_fd, &m, sizeof(m));
        return 1;
    }

    CUDA_CHECK(cudaSetDevice(GPU_RECVER));

    auto recver = NvlinkRecver::create(
        GPU_RECVER,
        ipc::SPSCQueue<NvlinkSendTicket>::wait_consumer(TICKET_QUEUE_NAME, QUEUE_WAIT_TIMEOUT).unwrap(),
        ipc::SPSCQueue<NvlinkAck>::wait_producer(ACK_QUEUE_NAME, QUEUE_WAIT_TIMEOUT).unwrap()
    );

    const std::size_t max_size = *std::max_element(cfg.sizes_bytes.begin(), cfg.sizes_bytes.end());

    void* dst = cuda_util::try_malloc(max_size, GPU_RECVER).unwrap();

    // For each size:
    // 1) Post all recv events first (receiver-initiated control-plane).
    // 2) Notify parent "ready".
    // 3) Poll until all those recv events complete.
    // 4) Notify parent "done" (barrier).
    uint32_t uid_base = 1000;

    for (uint32_t si = 0; si < cfg.sizes_bytes.size(); ++si) {
        const uint32_t total = static_cast<uint32_t>(cfg.warmup_iters + cfg.iters);
        const uint32_t bytes = static_cast<uint32_t>(cfg.sizes_bytes[si]);

        std::vector<decltype(recver->recv(uid_base, 0, 0))> evs;
        evs.reserve(total);

        for (uint32_t i = 0; i < total; ++i) {
            uint32_t uid = uid_base + i;
            evs.push_back(recver->recv(uid, reinterpret_cast<uint64_t>(dst), bytes));
        }

        PipeMsg ready {1, si};
        if (!write_exact(child_to_parent_fd, &ready, sizeof(ready))) {
            std::cerr << "[Child] Failed to write ready\n";
            cuda_util::try_free(dst).unwrap();
            return 1;
        }

        uint32_t done_cnt = 0;
        std::vector<uint8_t> done(total, 0);

        while (done_cnt < total) {
            recver->poll();
            // Check notifications (avoid O(total) scan too often by scanning each loop; total is small enough for bench).
            for (uint32_t i = 0; i < total; ++i) {
                if (!done[i] && evs[i]->is_notified()) {
                    done[i] = 1;
                    ++done_cnt;
                }
            }
            if (done_cnt < total)
                poll_pause(cfg.busy_poll);
        }

        PipeMsg done_msg {2, si};
        if (!write_exact(child_to_parent_fd, &done_msg, sizeof(done_msg))) {
            std::cerr << "[Child] Failed to write done\n";
            cuda_util::try_free(dst).unwrap();
            return 1;
        }

        // Wait for parent barrier "ack" (so next size doesn't get ahead on control-plane)
        PipeMsg parent_barrier {};
        if (!read_exact(parent_to_child_fd, &parent_barrier, sizeof(parent_barrier))) {
            std::cerr << "[Child] Failed to read parent barrier\n";
            cuda_util::try_free(dst).unwrap();
            return 1;
        }
        if (parent_barrier.kind != 2 || parent_barrier.size_idx != si) {
            std::cerr << "[Child] Unexpected barrier msg\n";
            cuda_util::try_free(dst).unwrap();
            return 1;
        }

        uid_base += 100000;  // keep uids unique across sizes
    }

    cuda_util::try_free(dst).unwrap();
    std::cout << "[Child] Receiver exiting\n";
    return 0;
}

int run_parent_sender(const BenchConfig& cfg, pid_t child_pid, int child_to_parent_fd, int parent_to_child_fd) {
    std::cout << "[Parent] Sender started (PID=" << getpid() << ", child=" << child_pid << ")\n";

    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count <= GPU_SENDER || device_count <= GPU_RECVER) {
        std::cerr << "[Parent] Need at least " << (GPU_RECVER + 1) << " GPUs, got " << device_count << "\n";
        return 1;
    }

    CUDA_CHECK(cudaSetDevice(GPU_SENDER));

    auto sender = NvlinkSender::create(
        GPU_SENDER,
        ipc::SPSCQueue<NvlinkSendTicket>::create_producer(TICKET_QUEUE_NAME, QUEUE_BYTES_TICKET).unwrap(),
        ipc::SPSCQueue<NvlinkAck>::create_consumer(ACK_QUEUE_NAME, QUEUE_BYTES_ACK).unwrap()
    );

    const std::size_t max_size = *std::max_element(cfg.sizes_bytes.begin(), cfg.sizes_bytes.end());

    printf("max_size = %zu bytes\n", max_size);
    void* src = cuda_util::try_malloc(max_size, GPU_SENDER).unwrap();

    // Initialize src once (not required for bandwidth, but avoids any lazy behavior).
    std::vector<uint8_t> host_init(4096, 0xA5);
    CUDA_CHECK(cudaMemcpy(src, host_init.data(), std::min(host_init.size(), max_size), cudaMemcpyHostToDevice));

    cudaIpcMemHandle_t handle {};
    CUDA_CHECK(cudaIpcGetMemHandle(&handle, src));

    std::cout << "==== NVLink Bandwidth Benchmark (fork, sender-timed) ====\n";
    std::cout << "GPUs: sender=" << GPU_SENDER << ", recver=" << GPU_RECVER << "\n";
    std::cout << "warmup=" << cfg.warmup_iters << ", iters=" << cfg.iters
              << ", poll=" << (cfg.busy_poll ? "busy" : "sleep") << "\n\n";

    uint32_t uid_base = 1000;

    for (uint32_t si = 0; si < cfg.sizes_bytes.size(); ++si) {
        // Wait child ready for this size.
        PipeMsg m {};
        if (!read_exact(child_to_parent_fd, &m, sizeof(m))) {
            std::cerr << "[Parent] Failed to read child msg\n";
            return 1;
        }
        if (m.kind == 3) {
            std::cerr << "[Parent] Child reported error\n";
            return 1;
        }
        if (m.kind != 1 || m.size_idx != si) {
            std::cerr << "[Parent] Unexpected child msg\n";
            return 1;
        }

        const uint32_t bytes = static_cast<uint32_t>(cfg.sizes_bytes[si]);

        // Warmup
        for (int i = 0; i < cfg.warmup_iters; ++i) {
            uint32_t uid = uid_base + static_cast<uint32_t>(i);
            auto ev = sender->send(uid, /*offset=*/0, bytes, handle);
            while (!ev->is_notified()) {
                sender->poll();
                poll_pause(cfg.busy_poll);
            }
        }

        // Measured
        auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < cfg.iters; ++i) {
            uint32_t uid = uid_base + static_cast<uint32_t>(cfg.warmup_iters + i);
            auto ev = sender->send(uid, /*offset=*/0, bytes, handle);
            while (!ev->is_notified()) {
                sender->poll();
                poll_pause(cfg.busy_poll);
            }
        }
        auto t1 = std::chrono::steady_clock::now();

        const double sec = std::chrono::duration<double>(t1 - t0).count();
        const double total_bytes = static_cast<double>(bytes) * static_cast<double>(cfg.iters);
        const double gib = total_bytes / (1024.0 * 1024.0 * 1024.0);
        const double gbps = (sec > 0.0) ? (gib / sec) : 0.0;

        std::cout << "size=" << cfg.sizes_bytes[si] << " B"
                  << "  time=" << sec << " s"
                  << "  throughput=" << gbps << " GiB/s"
                  << "\n";

        // Wait child done barrier
        PipeMsg done {};
        if (!read_exact(child_to_parent_fd, &done, sizeof(done))) {
            std::cerr << "[Parent] Failed to read child done\n";
            return 1;
        }
        if (done.kind != 2 || done.size_idx != si) {
            std::cerr << "[Parent] Unexpected child done\n";
            return 1;
        }

        // Send barrier ack back to child
        PipeMsg ack {2, si};
        if (!write_exact(parent_to_child_fd, &ack, sizeof(ack))) {
            std::cerr << "[Parent] Failed to write barrier ack\n";
            return 1;
        }

        uid_base += 100000;
    }

    cuda_util::try_free(src).unwrap();

    int status = 0;
    pid_t w = waitpid(child_pid, &status, 0);
    if (w == -1) {
        perror("[Parent] waitpid");
        return 1;
    }
    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code != 0) {
            std::cerr << "[Parent] Child exited with code " << code << "\n";
            return 1;
        }
    } else if (WIFSIGNALED(status)) {
        std::cerr << "[Parent] Child killed by signal " << WTERMSIG(status) << "\n";
        return 1;
    }

    std::cout << "\n[Parent] Benchmark complete.\n";
    return 0;
}

int main(int argc, char** argv) {
    BenchConfig cfg = parse_args(argc, argv);

    cleanup_stale_queues();

    // Pipes for lightweight synchronization.
    int child_to_parent[2] {-1, -1};
    int parent_to_child[2] {-1, -1};
    if (pipe(child_to_parent) != 0) {
        perror("pipe(child_to_parent)");
        return 1;
    }
    if (pipe(parent_to_child) != 0) {
        perror("pipe(parent_to_child)");
        return 1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }

    if (pid == 0) {
        // Child
        ::close(child_to_parent[0]);  // close read end
        ::close(parent_to_child[1]);  // close write end
        int rc = run_child_recver(cfg, child_to_parent[1], parent_to_child[0]);
        ::close(child_to_parent[1]);
        ::close(parent_to_child[0]);
        return rc;
    } else {
        // Parent
        ::close(child_to_parent[1]);  // close write end
        ::close(parent_to_child[0]);  // close read end
        int rc = run_parent_sender(cfg, pid, child_to_parent[0], parent_to_child[1]);
        ::close(child_to_parent[0]);
        ::close(parent_to_child[1]);
        return rc;
    }
}