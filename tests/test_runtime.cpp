#include <immintrin.h>  // for _mm_pause

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <thread>
#include <utility>
#include <vector>
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace pickle {

class Runtime;
class JoinHandle;

/**
 * @brief 轻量级的 build() 返回结果，仅提供 unwrap()。
 * 这里忽略错误处理，始终认为 build 成功。
 */
template<typename T>
class BuildResult {
public:
    explicit BuildResult(T value) noexcept : value_(std::move(value)) {}

    T unwrap() noexcept {
        return std::move(value_);
    }

private:
    T value_;
};

/**
 * @brief 后台线程的 JoinHandle，负责停止所有 worker 并 join。
 *
 * 使用方式：
 *   auto handle = runtime.spawn();
 *   ...
 *   handle.join(); // 或依赖析构自动 join
 */
class JoinHandle {
public:
    JoinHandle() = default;

    JoinHandle(std::shared_ptr<std::atomic<bool>> stop_flag, std::vector<std::thread>&& workers) noexcept :
        stop_flag_(std::move(stop_flag)),
        workers_(std::move(workers)) {}

    JoinHandle(const JoinHandle&) = delete;
    JoinHandle& operator=(const JoinHandle&) = delete;

    JoinHandle(JoinHandle&& other) noexcept :
        stop_flag_(std::move(other.stop_flag_)),
        workers_(std::move(other.workers_)) {}

    JoinHandle& operator=(JoinHandle&& other) noexcept {
        if (this != &other) {
            join();
            stop_flag_ = std::move(other.stop_flag_);
            workers_ = std::move(other.workers_);
        }
        return *this;
    }

    ~JoinHandle() {
        join();
    }

    /**
     * @brief 请求所有线程停止并 join。可以重复调用（幂等）。
     */
    void join() noexcept {
        auto flag = stop_flag_;
        if (!flag) {
            return;
        }

        flag->store(true, std::memory_order_release);
        for (auto& t : workers_) {
            if (t.joinable()) {
                t.join();
            }
        }
        workers_.clear();
        stop_flag_.reset();
    }

private:
    std::shared_ptr<std::atomic<bool>> stop_flag_;
    std::vector<std::thread> workers_;
};

/**
 * @brief 真正负责调度 poll-loop 的 Runtime。
 *
 * Runtime 持有：
 *  - senders 侧的 poll 回调集合；
 *  - recvers 侧的 poll 回调集合；
 *  - 线程数配置；
 *  - 可选的 NUMA node 绑定信息；
 *  - 可选的 poll interval（睡眠时间）。
 */
class Runtime {
public:
    using PollFn = std::function<void()>;

    Runtime(
        std::vector<PollFn> senders,
        std::vector<PollFn> recvers,
        std::vector<int> numa_nodes,
        std::chrono::nanoseconds poll_interval,
        std::size_t num_threads
    ) noexcept :
        sender_pollers_(std::move(senders)),
        recver_pollers_(std::move(recvers)),
        numa_nodes_(std::move(numa_nodes)),
        poll_interval_(poll_interval),
        num_threads_(num_threads) {}

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;
    Runtime(Runtime&&) = default;
    Runtime& operator=(Runtime&&) = default;

    /**
     * @brief 启动所有后台 worker 线程。
     *
     * 要求：senders 和 recvers 使用不同的线程组进行 poll。
     * 如果设置了 poll_interval_ 则使用 sleep_for，否则使用 _mm_pause() 忙等。
     * 如果设置了 numa_nodes_，则在每个线程启动时绑定对应节点。
     */
    JoinHandle spawn() noexcept {
        using namespace std::chrono;

        // 根据配置与实际 poller 数量，确定线程数量。
        std::size_t total_threads = num_threads_;

        const bool has_senders = !sender_pollers_.empty();
        const bool has_recvers = !recver_pollers_.empty();

        if (total_threads == 0) {
            if (has_senders && has_recvers) {
                total_threads = 2;  // 至少两个线程，分别 poll send/recv
            } else if (has_senders || has_recvers) {
                total_threads = 1;
            } else {
                // 没有任何 poller，直接返回空 handle
                return JoinHandle();
            }
        }

        if (has_senders && has_recvers && total_threads < 2) {
            // 同时存在 senders 和 recvers 时，强制至少两个线程
            total_threads = 2;
        }

        // 将发送和接收的 poller 拆分到不同的线程组。
        std::size_t sender_threads = 0;
        std::size_t recver_threads = 0;

        if (has_senders && has_recvers) {
            // 简单起见：一半线程给 sender，一半给 recver，多出的线程给 sender
            sender_threads = total_threads / 2 + (total_threads % 2);
            recver_threads = total_threads - sender_threads;
        } else if (has_senders) {
            sender_threads = total_threads;
        } else if (has_recvers) {
            recver_threads = total_threads;
        }

        // 保护性判断
        if (!has_senders) {
            sender_threads = 0;
        }
        if (!has_recvers) {
            recver_threads = 0;
        }

        // 把 poller 做一个简单的轮询分配到各自线程。
        std::vector<std::vector<PollFn>> sender_groups(sender_threads);
        for (std::size_t i = 0; i < sender_pollers_.size() && sender_threads > 0; ++i) {
            sender_groups[i % sender_threads].push_back(sender_pollers_[i]);
        }

        std::vector<std::vector<PollFn>> recver_groups(recver_threads);
        for (std::size_t i = 0; i < recver_pollers_.size() && recver_threads > 0; ++i) {
            recver_groups[i % recver_threads].push_back(recver_pollers_[i]);
        }

        auto stop_flag = std::make_shared<std::atomic<bool>>(false);
        std::vector<std::thread> workers;
        workers.reserve(total_threads);

        auto worker_fn = [stop_flag, interval = poll_interval_](std::vector<PollFn> pollers, int numa_node) mutable {
#ifdef __linux__
            if (numa_node >= 0) {
                // 这里为了简单，用 numa_node 作为 CPU id 做 affinity 绑定。
                // 如果需要严格 NUMA 绑定可以改用 libnuma。
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(static_cast<unsigned>(numa_node), &cpuset);
                pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
            }
#else
            (void)numa_node;
#endif
            while (!stop_flag->load(std::memory_order_acquire)) {
                for (auto& fn : pollers) {
                    fn();
                }

                if (interval.count() > 0) {
                    std::this_thread::sleep_for(interval);
                } else {
                    _mm_pause();
                }
            }
        };

        // 分配 NUMA node：简单 round-robin
        auto pick_numa = [this](std::size_t idx) -> int {
            if (this->numa_nodes_.empty()) {
                return -1;
            }
            return this->numa_nodes_[idx % this->numa_nodes_.size()];
        };

        std::size_t thread_index = 0;

        // 先启动 sender 线程
        for (std::size_t i = 0; i < sender_groups.size(); ++i) {
            auto& group = sender_groups[i];
            if (group.empty()) {
                continue;
            }
            int numa_node = pick_numa(thread_index++);
            workers.emplace_back(worker_fn, std::move(group), numa_node);
        }

        // 再启动 recver 线程
        for (std::size_t i = 0; i < recver_groups.size(); ++i) {
            auto& group = recver_groups[i];
            if (group.empty()) {
                continue;
            }
            int numa_node = pick_numa(thread_index++);
            workers.emplace_back(worker_fn, std::move(group), numa_node);
        }

        return JoinHandle(stop_flag, std::move(workers));
    }

private:
    std::vector<PollFn> sender_pollers_;
    std::vector<PollFn> recver_pollers_;
    std::vector<int> numa_nodes_;
    std::chrono::nanoseconds poll_interval_ {0};
    std::size_t num_threads_ {0};
};

/**
 * @brief Runtime 的构建器。
 *
 * 用法示例：
 *
 *   std::vector<std::shared_ptr<RdmaSender>> senders = ...;
 *   std::vector<std::shared_ptr<RdmaRecver>> recvers = ...;
 *
 *   using namespace std::chrono_literals;
 *
 *   auto join_handle = RuntimeBuilder()
 *       .with_senders(senders)
 *       .with_recvers(recvers)
 *       .with_numa_nodes({0})
 *       .with_poll_interval(1us)
 *       .with_threads(4)
 *       .build()
 *       .unwrap()
 *       .spawn();
 */
class RuntimeBuilder {
public:
    using PollFn = Runtime::PollFn;

    RuntimeBuilder() = default;

    /**
     * @brief 配置 sender 执行器集合。
     *
     * 这里假设 SenderPtr 支持 s->poll()，例如 std::shared_ptr<RdmaSender>。
     */
    template<typename SenderPtr>
    RuntimeBuilder& with_senders(const std::vector<SenderPtr>& senders) {
        sender_pollers_.clear();
        sender_pollers_.reserve(senders.size());
        for (auto& s : senders) {
            sender_pollers_.emplace_back([s]() { s->poll(); });
        }
        return *this;
    }

    /**
     * @brief 配置 recver 执行器集合。
     *
     * 同样假设 RecverPtr 支持 r->poll()。
     */
    template<typename RecverPtr>
    RuntimeBuilder& with_recvers(const std::vector<RecverPtr>& recvers) {
        recver_pollers_.clear();
        recver_pollers_.reserve(recvers.size());
        for (auto& r : recvers) {
            recver_pollers_.emplace_back([r]() { r->poll(); });
        }
        return *this;
    }

    /**
     * @brief 配置 NUMA node 列表。spawn 时按线程 index round-robin 绑定。
     */
    RuntimeBuilder& with_numa_nodes(std::vector<int> numa_nodes) noexcept {
        numa_nodes_ = std::move(numa_nodes);
        return *this;
    }

    /**
     * @brief 配置 poll interval。为 0 表示 busy-wait（_mm_pause）。
     */
    RuntimeBuilder& with_poll_interval(std::chrono::nanoseconds interval) noexcept {
        poll_interval_ = interval;
        return *this;
    }

    /**
     * @brief 配置 worker 线程的总数。
     */
    RuntimeBuilder& with_threads(std::size_t num_threads) noexcept {
        num_threads_ = num_threads;
        return *this;
    }

    /**
     * @brief 构建 Runtime，这里忽略错误，直接返回 BuildResult。
     */
    BuildResult<Runtime> build() noexcept {
        Runtime rt(
            std::move(sender_pollers_),
            std::move(recver_pollers_),
            std::move(numa_nodes_),
            poll_interval_,
            num_threads_
        );
        return BuildResult<Runtime>(std::move(rt));
    }

private:
    std::vector<PollFn> sender_pollers_;
    std::vector<PollFn> recver_pollers_;
    std::vector<int> numa_nodes_;
    std::chrono::nanoseconds poll_interval_ {0};
    std::size_t num_threads_ {0};
};

}  // namespace pickle