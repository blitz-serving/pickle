#include <cuda.h>
#include <cuda_runtime.h>

#include <cstdint>
#include <cstdio>

namespace gpu_mem_util {

void* malloc_gpu_buffer(uint64_t size, uint32_t device) noexcept {
    void* d_ptr;
    if (cudaSetDevice(device) != cudaSuccess) {
        return nullptr;
    } else if (cudaMalloc(&d_ptr, size) != cudaSuccess) {
        return nullptr;
    } else {
        return d_ptr;
    }
}

void free_gpu_buffer(void* d_ptr, uint32_t device) noexcept {
    cudaSetDevice(device);
    cudaFree(d_ptr);
}

void set_device(uint32_t device) noexcept {
    cudaSetDevice(device);
}

}  // namespace gpu_mem_util
