#pragma once

#include <cuda_runtime.h>

#include <cstdint>
#include <cstdlib>

#include "pickle_logger.h"
#include "result.h"

#define CUDA_TRY(expr)                                  \
    do {                                                \
        cudaError_t err = expr;                         \
        if (err != cudaSuccess) {                       \
            return result::Err(cuda_util::Error {err}); \
        }                                               \
    } while (0)

#define CUDA_CHECK(expr)                                      \
    do {                                                      \
        cudaError_t err = expr;                               \
        if (err != cudaSuccess) {                             \
            ERROR("CUDA error: {}", cudaGetErrorString(err)); \
            std::abort();                                     \
        }                                                     \
    } while (0)

namespace cuda_util {

struct Error {
    cudaError_t cuda_error;
};

inline std::ostream& operator<<(std::ostream& os, Error error) {
    os << cudaGetErrorString(error.cuda_error);
    return os;
}

inline result::Result<void*, cuda_util::Error> try_malloc(size_t size, int32_t device) noexcept {
    void* ptr;
    CUDA_TRY(cudaSetDevice(device));
    CUDA_TRY(cudaMalloc(&ptr, size));
    return result::Ok(ptr);
}

inline result::Result<void, cuda_util::Error> try_free(void* ptr) noexcept {
    CUDA_TRY(cudaFree(ptr));
    return result::Ok();
}

inline void free_unwrap(void* ptr) noexcept {
    cuda_util::try_free(ptr).unwrap();
}

}  // namespace cuda_util
