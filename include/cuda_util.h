#include <cuda_runtime.h>

#include <cstdint>

#include "pickle_logger.h"

namespace cuda_util {
inline void* malloc_gpu_buffer(uint64_t size, uint32_t device) noexcept {
    void* d_ptr;
    if (cudaSetDevice(device) != cudaSuccess) {
        return nullptr;
    } else if (cudaMalloc(&d_ptr, size) != cudaSuccess) {
        return nullptr;
    } else {
        return d_ptr;
    }
}

inline void free_gpu_buffer(void* d_ptr) noexcept {
    if (d_ptr == nullptr) {
        return;
    } else if (cudaFree(d_ptr) != cudaSuccess) {
        ERROR("Failed to free GPU buffer");
    }
}
}  // namespace cuda_util