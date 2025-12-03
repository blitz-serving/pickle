#pragma once

#include <cuda_runtime.h>

#include <cstdint>
#include <cstdlib>

#include "pickle_logger.h"
#include "result.h"

#define CUDA_TRY(expr)               \
    do {                             \
        cudaError_t err = expr;      \
        if (err != cudaSuccess) {    \
            return result::Err(err); \
        }                            \
    } while (0)

#define CUDA_CHECK(expr)                                      \
    do {                                                      \
        cudaError_t err = expr;                               \
        if (err != cudaSuccess) {                             \
            ERROR("CUDA error: {}", cudaGetErrorString(err)); \
            std::abort();                                     \
        }                                                     \
    } while (0)

inline std::ostream& operator<<(std::ostream& os, cudaError_t err) {
    os << cudaGetErrorString(err);
    return os;
}

namespace cuda_util {

inline result::Result<void*, cudaError_t> try_malloc(size_t size, int32_t device) noexcept {
    void* ptr;
    CUDA_TRY(cudaSetDevice(device));
    CUDA_TRY(cudaMalloc(&ptr, size));
    return result::Ok(ptr);
}

inline result::Result<void, cudaError_t> try_free(void* ptr) noexcept {
    CUDA_TRY(cudaFree(ptr));
    return result::Ok();
}

inline void free_unwrap(void* ptr) noexcept {
    cuda_util::try_free(ptr).unwrap();
}

}  // namespace cuda_util
