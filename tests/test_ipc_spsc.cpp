#include <cassert>
#include <iostream>

#include "result.h"
#include "spsc.h"

using namespace std::chrono_literals;

static void test_basic_push_pop() {
    std::cout << "[TEST] basic push/pop\n";

    constexpr std::size_t shm_size = 1 << 20;  // 1MB

    // 作为 Producer 创建队列
    auto producer = ipc::SPSCQueue<int>::create_producer("/spsc_test_queue", shm_size).unwrap();
    // 同一进程里，以 Consumer 身份附着

    auto consumer = ipc::SPSCQueue<int>::wait_consumer("/spsc_test_queue", 1000ms).unwrap();
    std::cout << "queue capacity = " << producer.capacity() << "\n";

    // 简单的 push / pop 测试
    constexpr int N = 100;
    for (int i = 0; i < N; ++i) {
        // 单线程这里不会真的 busy-wait，只是接口测试
        while (producer.try_push(i).is_err()) {
            // std::this_thread::yield();
        }
    }

    for (int i = 0; i < N; ++i) {
        auto res = consumer.try_pop();
        while (res.is_err()) {
            // std::this_thread::yield();
            res = consumer.try_pop();
        }
        int v = res.unwrap();
        std::cout << "consumed: " << v << "\n";
        assert(v == i);
    }

    std::cout << "[OK] basic push/pop passed\n\n";
}

static void test_role_enforcement() {
    std::cout << "[TEST] role enforcement (single producer / single consumer)\n";

    constexpr std::size_t shm_size = 1 << 20;  // 1MB

    // 重新创建一个新的 queue 名称，避免跟上一个测试冲突
    auto producer = ipc::SPSCQueue<int>::create_producer("/spsc_test_queue_role", shm_size).unwrap();

    // attach 一次 consumer：应该成功
    auto consumer = ipc::SPSCQueue<int>::open_consumer("/spsc_test_queue_role").unwrap();

    // 第二次以 Producer 身份附着：应失败
    ipc::SPSCQueue<int>::open_producer("/spsc_test_queue_role").unwrap_err();

    // 第二次以 Consumer 身份附着：也应失败
    ipc::SPSCQueue<int>::open_consumer("/spsc_test_queue_role").unwrap_err();

    std::cout << "[OK] role enforcement passed\n\n";
}

static void test_capacity_behavior() {
    std::cout << "[TEST] capacity behavior (full/empty)\n";

    constexpr std::size_t shm_size = 1 << 16;  // 小一点，方便测试满队列

    auto producer = ipc::SPSCQueue<int>::create_producer("/spsc_test_queue_cap", shm_size).unwrap();
    auto consumer = ipc::SPSCQueue<int>::open_consumer("/spsc_test_queue_cap").unwrap();

    const std::size_t cap = producer.capacity();
    std::cout << "queue capacity = " << cap << "\n";
    assert(cap >= 1);

    // 填满队列（理论上 capacity() = slots - 1，可以 push cap 个元素）
    for (std::size_t i = 0; i < cap; ++i) {
        producer.try_push(static_cast<int>(i)).unwrap();
    }

    // 再 push 一次应该失败
    producer.try_push(123456).unwrap_err();
    std::cout << "producer correctly reports full\n";

    // 全部 pop 掉
    for (std::size_t i = 0; i < cap; ++i) {
        auto v = consumer.try_pop().unwrap();
        assert(v == static_cast<int>(i));
    }

    // 再 pop 一次应该失败（空）
    consumer.try_pop().unwrap_err();
    std::cout << "consumer correctly reports empty\n";

    std::cout << "[OK] capacity behavior passed\n\n";
}

int main() {
    test_basic_push_pop();
    test_role_enforcement();
    test_capacity_behavior();

    std::cout << "All tests passed.\n";
    return 0;
}