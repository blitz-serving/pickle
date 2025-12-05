// examples/test_nvlink_fork.cpp

#include <cuda_runtime.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "cuda_util.h"
#include "executor_nvlink.h"
#include "spsc.h"

using namespace pickle;
using namespace std::chrono_literals;

static constexpr int GPU_SENDER = 0;
static constexpr int GPU_RECVER = 1;

static constexpr const char* TICKET_QUEUE_NAME = "/nvlink_ticket_queue";
static constexpr const char* ACK_QUEUE_NAME = "/nvlink_ack_queue";

constexpr std::size_t QUEUE_BYTES_TICKET = 1 << 20;  // 1MB 数据区
constexpr std::size_t QUEUE_BYTES_ACK = 1 << 16;  // 64KB 数据区

constexpr std::chrono::milliseconds QUEUE_WAIT_TIMEOUT {5000};

static void cleanup_stale_queues() {
    // Remove stale POSIX shm objects from previous runs to avoid EEXIST on create.
    ::shm_unlink(TICKET_QUEUE_NAME);
    ::shm_unlink(ACK_QUEUE_NAME);
}

int run_child_recver() {
    std::cout << "[Child] Receiver process started (PID=" << getpid() << ")\n";

    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count <= GPU_RECVER) {
        std::cerr << "[Child] Need at least " << (GPU_RECVER + 1) << " GPUs, got " << device_count << std::endl;
        return 1;
    }

    // 1. 绑定 GPU1
    CUDA_CHECK(cudaSetDevice(GPU_RECVER));

    // 2. 创建 NvlinkRecver
    auto recver = NvlinkRecver::create(
        GPU_RECVER,
        ipc::SPSCQueue<NvlinkSendTicket>::wait_consumer(TICKET_QUEUE_NAME, QUEUE_WAIT_TIMEOUT).unwrap(),
        ipc::SPSCQueue<NvlinkAck>::wait_producer(ACK_QUEUE_NAME, QUEUE_WAIT_TIMEOUT).unwrap()
    );

    // 4. 在 GPU1 上分配接收 buffer
    constexpr int N = 16;
    auto dst = cuda_util::try_malloc(N * sizeof(int), GPU_RECVER).unwrap();

    uint32_t uid = 1234;

    auto ev_recv = recver->recv(uid, reinterpret_cast<uint64_t>(dst), static_cast<uint32_t>(N * sizeof(int)));

    // 5. 后台轮询匹配 + 触发 copy 任务
    while (!ev_recv->is_notified()) {
        recver->poll();
        std::this_thread::sleep_for(1ms);
    }

    // 6. 拷贝回 host 校验数据
    int host_out[N];
    CUDA_CHECK(cudaMemcpy(host_out, dst, N * sizeof(int), cudaMemcpyDeviceToHost));

    std::cout << "[Child] Received data: ";
    for (int i = 0; i < N; ++i) {
        std::cout << host_out[i] << " ";
    }
    std::cout << std::endl;

    // 检查模式是否为 i * 10
    bool ok = true;
    for (int i = 0; i < N; ++i) {
        int expected = i * 10;
        if (host_out[i] != expected) {
            std::cerr << "[Child] Mismatch at " << i << ": got " << host_out[i] << ", expected " << expected
                      << std::endl;
            ok = false;
        }
    }

    if (ok) {
        std::cout << "[Child] Data check PASSED.\n";
    } else {
        std::cout << "[Child] Data check FAILED.\n";
    }

    cuda_util::try_free(dst).unwrap();

    std::cout << "[Child] Receiver process exiting.\n";
    return ok ? 0 : 1;
}

int run_parent_sender(pid_t child_pid) {
    std::cout << "[Parent] Sender process started (PID=" << getpid() << ", child=" << child_pid << ")\n";

    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count <= GPU_SENDER || device_count <= GPU_RECVER) {
        std::cerr << "[Parent] Need at least " << (GPU_RECVER + 1) << " GPUs, got " << device_count << std::endl;
        return 1;
    }

    // 1. 绑定 GPU0
    CUDA_CHECK(cudaSetDevice(GPU_SENDER));

    // 2. 创建 NvlinkSender
    auto sender = NvlinkSender::create(
        GPU_SENDER,
        ipc::SPSCQueue<NvlinkSendTicket>::create_producer(TICKET_QUEUE_NAME, QUEUE_BYTES_TICKET).unwrap(),
        ipc::SPSCQueue<NvlinkAck>::create_consumer(ACK_QUEUE_NAME, QUEUE_BYTES_ACK).unwrap()
    );

    // 4. 在 GPU0 上分配并填充发送数据
    constexpr int N = 16;
    int* src = static_cast<int*>(cuda_util::try_malloc(N * sizeof(int), GPU_SENDER).unwrap());

    int host_data[N];
    for (int i = 0; i < N; ++i) {
        host_data[i] = i * 10;
    }
    CUDA_CHECK(cudaMemcpy(src, host_data, N * sizeof(int), cudaMemcpyHostToDevice));
    cudaIpcMemHandle_t handle {};
    CUDA_CHECK(cudaIpcGetMemHandle(&handle, src));

    uint32_t uid = 1234;

    // 5. 发起 NVLink 发送
    // The sender API expects an offset from the IPC handle base pointer; the buffer
    // starts at the base, so offset should be 0 instead of passing the raw pointer.
    auto ev_send = sender->send(uid, /*offset=*/0, static_cast<uint32_t>(N * sizeof(int)), handle);

    // 6. 轮询 ACK，直到发送侧事件完成
    while (!ev_send->is_notified()) {
        sender->poll();
        std::this_thread::sleep_for(1ms);
    }

    std::cout << "[Parent] Send finished, waiting for child...\n";

    // 7. 等待子进程退出
    int status = 0;
    pid_t w = waitpid(child_pid, &status, 0);
    if (w == -1) {
        perror("[Parent] waitpid");
        return 1;
    }

    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        std::cout << "[Parent] Child exited with code " << code << "\n";
        if (code != 0) {
            return 1;
        }
    } else if (WIFSIGNALED(status)) {
        std::cout << "[Parent] Child killed by signal " << WTERMSIG(status) << "\n";
        return 1;
    }

    cuda_util::try_free(src).unwrap();

    std::cout << "[Parent] All done, test PASSED.\n";
    return 0;
}

int main() {
    std::cout << "==== NVLink Executor Multi-Process Test (fork) ====\n";

    cleanup_stale_queues();

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }

    if (pid == 0) {
        // 子进程：Receiver（GPU1）
        return run_child_recver();
    } else {
        // 父进程：Sender（GPU0）
        return run_parent_sender(pid);
    }
}
