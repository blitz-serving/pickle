#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "spsc.h"

// -------------------- payload types --------------------
template<size_t N>
struct Payload {
    std::uint64_t seq;
    std::uint8_t pad[N >= sizeof(std::uint64_t) ? (N - sizeof(std::uint64_t)) : 0];
};

static_assert(std::is_trivially_copyable_v<Payload<8>>);
static_assert(std::is_trivially_copyable_v<Payload<64>>);
static_assert(std::is_trivially_copyable_v<Payload<256>>);
static_assert(std::is_trivially_copyable_v<Payload<1024>>);

// -------------------- utils --------------------
static inline void pin_to_cpu0_best_effort() {
#ifdef __linux__
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    (void)::sched_setaffinity(0, sizeof(set), &set);
#endif
}

static inline uint64_t now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

static void write_u64(int fd, uint64_t v) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&v);
    size_t left = sizeof(v);
    while (left) {
        ssize_t n = ::write(fd, p + (sizeof(v) - left), left);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            std::perror("write");
            std::exit(1);
        }
        left -= static_cast<size_t>(n);
    }
}

static uint64_t read_u64(int fd) {
    uint64_t v = 0;
    uint8_t* p = reinterpret_cast<uint8_t*>(&v);
    size_t left = sizeof(v);
    while (left) {
        ssize_t n = ::read(fd, p + (sizeof(v) - left), left);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            std::perror("read");
            std::exit(1);
        }
        if (n == 0) {
            // pipe closed
            std::fprintf(stderr, "read: EOF\n");
            std::exit(1);
        }
        left -= static_cast<size_t>(n);
    }
    return v;
}

struct Args {
    std::string shm_name = "/ipc_spsc_bench";
    size_t shm_size = 64ULL * 1024 * 1024;  // 64MB default
    uint64_t iters = 50'000'000;
    uint64_t warmup = 1'000'000;
    int payload = 8;  // bytes
    bool verify = false;
    bool pin = true;
};

static Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; i++) {
        std::string s = argv[i];
        auto need = [&](const char* k) {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "missing value for %s\n", k);
                std::exit(2);
            }
            return std::string(argv[++i]);
        };

        if (s == "--name")
            a.shm_name = need("--name");
        else if (s == "--shm-size")
            a.shm_size = std::stoull(need("--shm-size"));
        else if (s == "--iters")
            a.iters = std::stoull(need("--iters"));
        else if (s == "--warmup")
            a.warmup = std::stoull(need("--warmup"));
        else if (s == "--payload")
            a.payload = std::stoi(need("--payload"));
        else if (s == "--verify")
            a.verify = true;
        else if (s == "--no-pin")
            a.pin = false;
        else if (s == "--help" || s == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "  --name <shm_name>       default: /ipc_spsc_bench\n"
                      << "  --shm-size <bytes>      default: 67108864 (64MB)\n"
                      << "  --iters <N>             default: 50000000\n"
                      << "  --warmup <N>            default: 1000000\n"
                      << "  --payload <bytes>       one of: 8,64,256,1024 (default 8)\n"
                      << "  --verify                check seq ordering\n"
                      << "  --no-pin                do not pin to CPU0\n";
            std::exit(0);
        } else {
            std::fprintf(stderr, "unknown arg: %s\n", s.c_str());
            std::exit(2);
        }
    }
    if (!(a.payload == 8 || a.payload == 64 || a.payload == 256 || a.payload == 1024)) {
        std::fprintf(stderr, "--payload must be 8/64/256/1024\n");
        std::exit(2);
    }
    return a;
}

// -------------------- benchmark core --------------------
template<typename T>
static int run_bench(const Args& args) {
    using Queue = ipc::SPSCQueue<T>;

    // parent<->child sync pipes
    int p2c[2], c2p[2];
    if (::pipe(p2c) != 0) {
        std::perror("pipe");
        return 1;
    }
    if (::pipe(c2p) != 0) {
        std::perror("pipe");
        return 1;
    }

    // parent creates shm as Producer
    auto qres = Queue::create_producer(args.shm_name, args.shm_size);
    if (!qres.is_ok()) {
        std::cerr << "create_producer failed: " << qres.unwrap_err() << "\n";
        return 1;
    }
    Queue producer = std::move(qres.unwrap());
    std::cout << "SHM name=" << args.shm_name << " shm_size=" << args.shm_size << " type_size=" << sizeof(T)
              << " capacity=" << producer.capacity() << " (usable slots)\n";

    pid_t pid = ::fork();
    if (pid < 0) {
        std::perror("fork");
        return 1;
    }

    if (pid == 0) {
        // child = consumer
        if (args.pin)
            pin_to_cpu0_best_effort();

        ::close(p2c[1]);  // read only
        ::close(c2p[0]);  // write only

        auto cres = Queue::wait_consumer(args.shm_name, std::chrono::milliseconds(5000));
        if (!cres.is_ok()) {
            std::cerr << "wait_consumer failed: " << cres.unwrap_err() << "\n";
            std::exit(1);
        }
        Queue consumer = std::move(cres.unwrap());

        // tell parent "ready"
        write_u64(c2p[1], 0xC0FFEE);

        // wait "start"
        (void)read_u64(p2c[0]);

        uint64_t empty_retries_warm = 0;
        uint64_t empty_retries = 0;
        uint64_t expected = 0;

        // warmup
        for (uint64_t i = 0; i < args.warmup; i++) {
            while (true) {
                auto r = consumer.try_pop();
                if (r.is_ok()) {
                    if (args.verify) {
                        auto v = r.unwrap();
                        if (v.seq != expected) {
                            std::cerr << "verify failed (warmup): got " << v.seq << " expected " << expected << "\n";
                            std::exit(1);
                        }
                    }
                    expected++;
                    break;
                } else {
                    empty_retries_warm++;
                }
            }
        }

        // timed
        for (uint64_t i = 0; i < args.iters; i++) {
            while (true) {
                auto r = consumer.try_pop();
                if (r.is_ok()) {
                    if (args.verify) {
                        auto v = r.unwrap();
                        if (v.seq != expected) {
                            std::cerr << "verify failed: got " << v.seq << " expected " << expected << "\n";
                            std::exit(1);
                        }
                    }
                    expected++;
                    break;
                } else {
                    empty_retries++;
                }
            }
        }

        // signal done + report retries
        write_u64(c2p[1], empty_retries_warm);
        write_u64(c2p[1], empty_retries);
        std::exit(0);
    }

    // parent = producer
    if (args.pin)
        pin_to_cpu0_best_effort();

    ::close(p2c[0]);  // write only
    ::close(c2p[1]);  // read only

    // wait child ready
    (void)read_u64(c2p[0]);

    uint64_t full_retries_warm = 0;
    uint64_t full_retries = 0;

    // warmup
    for (uint64_t i = 0; i < args.warmup; i++) {
        T v {};
        v.seq = i;
        while (true) {
            auto r = producer.try_push(v);
            if (r.is_ok())
                break;
            full_retries_warm++;
        }
    }

    // start timed section (both sides)
    write_u64(p2c[1], 0xBADC0DE);
    const uint64_t t0 = now_ns();

    for (uint64_t i = 0; i < args.iters; i++) {
        T v {};
        v.seq = args.warmup + i;
        while (true) {
            auto r = producer.try_push(v);
            if (r.is_ok())
                break;
            full_retries++;
        }
    }

    // wait child exit
    int status = 0;
    ::waitpid(pid, &status, 0);
    const uint64_t t1 = now_ns();

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        std::cerr << "child failed\n";
        return 1;
    }

    const uint64_t child_empty_warm = read_u64(c2p[0]);
    const uint64_t child_empty = read_u64(c2p[0]);

    const double sec = double(t1 - t0) / 1e9;
    const double mops = double(args.iters) / sec / 1e6;
    const double ns_per = double(t1 - t0) / double(args.iters);
    const double gbps = (double(args.iters) * double(sizeof(T))) / sec / (1024.0 * 1024.0 * 1024.0);

    std::cout << "---- Results (process-shm SPSC) ----\n";
    std::cout << "iters=" << args.iters << " warmup=" << args.warmup << "\n";
    std::cout << "time=" << sec << " s\n";
    std::cout << "throughput=" << mops << " Mops/s\n";
    std::cout << "bandwidth=" << gbps << " GiB/s (payload bytes counted as sizeof(T))\n";
    std::cout << "avg=" << ns_per << " ns/op (end-to-end)\n";
    std::cout << "producer full retries: warmup=" << full_retries_warm << " timed=" << full_retries << "\n";
    std::cout << "consumer empty retries: warmup=" << child_empty_warm << " timed=" << child_empty << "\n";

    return 0;
}

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);

    // 为了避免残留（比如上次异常退出），先尝试 unlink 一次
    ::shm_unlink(args.shm_name.c_str());

    if (args.payload == 8)
        return run_bench<Payload<8>>(args);
    if (args.payload == 64)
        return run_bench<Payload<64>>(args);
    if (args.payload == 256)
        return run_bench<Payload<256>>(args);
    return run_bench<Payload<1024>>(args);
}