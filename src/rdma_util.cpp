#include "rdma_util.h"

#include <cuda.h>
#include <infiniband/verbs.h>

#include <cstdio>

namespace rdma_util {

Result_t open_ib_device(ibv_context** ret, const char* dev_name) {
    ibv_device** dev_list = nullptr;
    ibv_device* dev = nullptr;
    Result_t result = Result_t::SUCCESS;

    dev_list = ibv_get_device_list(nullptr);
    if (!dev_list) {
        result = Result_t::DEVICE_NOT_FOUND;
        goto out;
    }

    for (int i = 0; dev_list[i] != nullptr; i++) {
        if (strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
            dev = dev_list[i];
            break;
        }
    }

    if (dev == nullptr) {
        result = Result_t::DEVICE_NOT_FOUND;
        goto clean_device_list;
    }

    *ret = ibv_open_device(dev);
    if (*ret == nullptr) {
        result = Result_t::DEVICE_OPEN_FAILED;
        goto clean_device_list;
    }

clean_device_list:
    ibv_free_device_list(dev_list);
out:
    return result;
}

Result_t close_ib_device(ibv_context* context) {
    if (ibv_close_device(context)) {
        return Result_t::DEVICE_CLOSE_FAILED;
    } else {
        return Result_t::SUCCESS;
    }
}

#define ASSERT(x) \
    do { \
        if (!(x)) { \
            fprintf(stdout, "Assertion \"%s\" failed at %s:%d\n", #x, __FILE__, __LINE__); \
        } \
    } while (0)

#define CUCHECK(stmt) \
    do { \
        CUresult result = (stmt); \
        ASSERT(CUDA_SUCCESS == result); \
    } while (0)

static int get_gpu_device_id_from_bdf(const char* bdf) {
    int given_bus_id = 0;
    int given_device_id = 0;
    int given_func = 0;
    int device_count = 0;
    int i;
    int ret_val;

    /*    "3e:02.0"*/
    ret_val = sscanf(bdf, "%x:%x.%x", &given_bus_id, &given_device_id, &given_func);
    if (ret_val != 3) {
        fprintf(
            stderr,
            "Wrong BDF format \"%s\". Expected format example: \"3e:02.0\", "
            "where 3e - bus id, 02 - device id, 0 - function\n",
            bdf
        );
        return -1;
    }
    if (given_func != 0) {
        fprintf(stderr, "Wrong pci function %d, 0 is expected\n", given_func);
        return -1;
    }
    CUCHECK(cuDeviceGetCount(&device_count));

    if (device_count == 0) {
        fprintf(stderr, "There are no available devices that support CUDA\n");
        return -1;
    }

    for (i = 0; i < device_count; i++) {
        CUdevice cu_dev;
        int pci_bus_id = 0;
        int pci_device_id = 0;

        CUCHECK(cuDeviceGet(&cu_dev, i));
        CUCHECK(cuDeviceGetAttribute(&pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, cu_dev)
        ); /*PCI bus identifier of the device*/
        CUCHECK(cuDeviceGetAttribute(&pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_dev)
        ); /*PCI device (also known as slot) identifier of the device*/
        if ((pci_bus_id == given_bus_id) && (pci_device_id == given_device_id)) {
            return i;
        }
    }
    fprintf(stderr, "Given BDF \"%s\" doesn't match one of GPU devices\n", bdf);
    return -1;
}

static CUcontext cuContext;

static void* init_gpu(size_t gpu_buf_size, const char* bdf) {
    const size_t gpu_page_size = 64 * 1024;
    size_t aligned_size;
    CUresult cu_result;

    aligned_size = (gpu_buf_size + gpu_page_size - 1) & ~(gpu_page_size - 1);
    printf("initializing CUDA\n");
    cu_result = cuInit(0);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuInit(0) returned %d\n", cu_result);
        return NULL;
    }

    int dev_id = get_gpu_device_id_from_bdf(bdf);
    if (dev_id < 0) {
        fprintf(stderr, "Wrong device index (%d) obtained from bdf \"%s\"\n", dev_id, bdf);
        /* This function returns NULL if there are no CUDA capable devices. */
        return NULL;
    }

    /* Pick up device by given dev_id - an ordinal in the range [0,
   * cuDeviceGetCount()-1] */
    CUdevice cu_dev;
    CUCHECK(cuDeviceGet(&cu_dev, dev_id));

    printf("creating CUDA Contnext\n");
    /* Create context */
    cu_result = cuCtxCreate(&cuContext, CU_CTX_MAP_HOST, cu_dev);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuCtxCreate() error=%d\n", cu_result);
        return NULL;
    }

    printf("making it the current CUDA Context\n");
    cu_result = cuCtxSetCurrent(cuContext);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuCtxSetCurrent() error=%d\n", cu_result);
        return NULL;
    }

    printf("cuMemAlloc() of a %zd bytes GPU buffer\n", aligned_size);
    CUdeviceptr d_A;
    cu_result = cuMemAlloc(&d_A, aligned_size);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuMemAlloc error=%d\n", cu_result);
        return NULL;
    }
    printf("allocated GPU buffer address at %016llx pointer=%p\n", d_A, (void*)d_A);

    return ((void*)d_A);
}

void* malloc_gpu_buffer(size_t length, const char* bdf) {
    return init_gpu(length, bdf);
}

}  // namespace rdma_util