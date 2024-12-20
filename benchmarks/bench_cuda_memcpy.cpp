#include <cuda_runtime.h>

#include <chrono>
#include <cstdint>
#include <cstdio>

#include "gpu_mem_util.h"

constexpr uint64_t kChunkSize = 64 * 1024 * 1024;
constexpr uint64_t kBufferSize = 76ull * 1024 * 1024 * 1024;

int main() {
    void* d_ptr = gpu_mem_util::malloc_gpu_buffer(kBufferSize, 7);

    auto s_time = std::chrono::high_resolution_clock::now();

    const auto slot_num = kBufferSize / kChunkSize;

    auto src_ptr = reinterpret_cast<uint8_t*>(d_ptr);

    for (uint64_t i = 1; i < slot_num; i++) {
        auto dst_ptr = src_ptr + i * kChunkSize;
        cudaMemcpy(src_ptr, dst_ptr, kChunkSize, cudaMemcpyDeviceToDevice);
    }

    auto e_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(e_time - s_time).count();

    printf("Duration: %f ms\n", duration / 1000.0);

    return 0;
}
