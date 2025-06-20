#include <cuda_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>

#define SOCKET_PATH "/tmp/cuda_ipc_socket"
#define DATA_SIZE 1024

int parent() {
    int server_fd, client_fd;
    struct sockaddr_un addr;
    cudaError_t cudaStatus;
    void* d_data = NULL;
    cudaIpcMemHandle_t handle;

    // 1. 创建Unix socket
    if ((server_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        perror("socket error");
        exit(EXIT_FAILURE);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    unlink(SOCKET_PATH);  // 确保路径可用

    // 2. 绑定socket
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind error");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 3. 监听连接
    if (listen(server_fd, 1) < 0) {
        perror("listen error");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 4. 分配CUDA内存
    cudaStatus = cudaMalloc(&d_data, DATA_SIZE);
    if (cudaStatus != cudaSuccess) {
        fprintf(stderr, "cudaMalloc failed: %s\n", cudaGetErrorString(cudaStatus));
        exit(EXIT_FAILURE);
    }

    // 初始化数据（示例）
    char message[DATA_SIZE] {};
    memcpy(message, "Hello, CUDA!", sizeof(message));
    cudaMemcpy(d_data, message, DATA_SIZE, cudaMemcpyHostToDevice);

    // 5. 获取IPC句柄
    cudaStatus = cudaIpcGetMemHandle(&handle, d_data);
    if (cudaStatus != cudaSuccess) {
        fprintf(stderr, "cudaIpcGetMemHandle failed: %s\n", cudaGetErrorString(cudaStatus));
        cudaFree(d_data);
        exit(EXIT_FAILURE);
    }

    printf("Parent: Waiting for child connection...\n");
    client_fd = accept(server_fd, NULL, NULL);
    if (client_fd < 0) {
        perror("accept error");
        cudaFree(d_data);
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 6. 发送IPC句柄
    if (write(client_fd, &handle, sizeof(handle)) != sizeof(handle)) {
        perror("write error");
    } else {
        printf("Parent: Sent IPC handle to child\n");
    }

    // 等待子进程完成（实际应用中需用同步机制）
    sleep(2);

    // 清理
    cudaFree(d_data);
    close(client_fd);
    close(server_fd);
    unlink(SOCKET_PATH);
    printf("Parent: returned\n");
    return 0;
}

int child() {
    int sockfd;
    struct sockaddr_un addr;
    cudaError_t cudaStatus;
    void* d_data = NULL;
    cudaIpcMemHandle_t handle;

    // 1. 创建Unix socket
    if ((sockfd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        perror("socket error");
        exit(EXIT_FAILURE);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    // 2. 连接父进程
    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect error");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // 3. 接收IPC句柄
    if (read(sockfd, &handle, sizeof(handle)) != sizeof(handle)) {
        perror("read error");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    printf("Child: Received IPC handle\n");

    // 4. 映射共享内存
    cudaStatus = cudaIpcOpenMemHandle(&d_data, handle, cudaIpcMemLazyEnablePeerAccess);
    if (cudaStatus != cudaSuccess) {
        fprintf(stderr, "cudaIpcOpenMemHandle failed: %s\n", cudaGetErrorString(cudaStatus));
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    // 5. 使用CUDA内存（示例：读取数据）
    char message[DATA_SIZE] {};
    cudaMemcpy(&message, d_data, DATA_SIZE, cudaMemcpyDeviceToHost);
    printf("Child: Read data from shared CUDA memory: %s\n", message);

    // // Register shared memory `d_data` is illegal !!
    // rdma_util::MemoryRegion::create(
    //     rdma_util::ProtectionDomain::create(rdma_util::Context::create("mlx5_0")),
    //     d_data,
    //     DATA_SIZE
    // );

    // 6. 清理
    cudaIpcCloseMemHandle(d_data);
    close(sockfd);

    printf("Child: returned\n");
    return 0;
}

int main() {
    pid_t pid = fork();
    if (pid == 0) {
        child();
    } else {
        parent();
    }
    return 0;
}