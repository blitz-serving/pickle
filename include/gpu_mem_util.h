#ifndef _GPU_MEM_UTIL_H_
#define _GPU_MEM_UTIL_H_

#include <cstdint>

namespace gpu_mem_util {

void* malloc_gpu_buffer(uint64_t size, uint32_t device) noexcept;
void free_gpu_buffer(void* d_ptr, uint32_t device) noexcept;

}  // namespace gpu_mem_util

#endif  // _GPU_MEM_UTIL_H_