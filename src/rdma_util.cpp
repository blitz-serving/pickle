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

ibv_qp* create_rc(ibv_context* context) {
    auto pd = ibv_alloc_pd(context);
    auto send_cq = ibv_create_cq(context, 128, nullptr, nullptr, 0);
    auto recv_cq = ibv_create_cq(context, 128, nullptr, nullptr, 0);
    ibv_qp_init_attr init_attr {
        .send_cq = send_cq,
        .recv_cq = recv_cq,
        .cap =
            {
                .max_send_wr = 128,
                .max_recv_wr = 1024,
                .max_send_sge = 1,
                .max_recv_sge = 1,
                .max_inline_data = 64,
            },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0,
    };

    return ibv_create_qp(pd, &init_attr);
}

int query_handshake_data(ibv_qp* qp, HandshakeData* handshake_data) {
    int ret;

    ibv_gid gid;
    ret = ibv_query_gid(qp->context, 1, 0, &gid);
    if (ret) {
        return ret;
    }

    ibv_port_attr attr;
    ret = ibv_query_port(qp->context, 1, &attr);
    if (ret) {
        return ret;
    }
    uint16_t lid = attr.lid;

    HandshakeData data {
        .gid = gid,
        .lid = lid,
        .qp_num = qp->qp_num,
    };
    return 0;
}

int bring_up_rc(ibv_qp* qp, HandshakeData& data) {
    ibv_gid gid = data.gid;
    uint16_t lid = data.lid;
    uint32_t remote_qp_num = data.qp_num;

    {
        int mask = ibv_qp_attr_mask::IBV_QP_STATE;
        ibv_qp_attr attr;
        ibv_qp_init_attr init_attr;
        int ret = ibv_query_qp(qp, &attr, mask, &init_attr);
        if (attr.qp_state != ibv_qp_state::IBV_QPS_RESET) {
            printf("QP state is not RESET\n");
            return 1;
        }
    }

    {
        // Modify QP to INIT
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        ibv_qp_attr attr = {
            .qp_state = ibv_qp_state::IBV_QPS_INIT,
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
            .pkey_index = 0,
            .port_num = 1,
        };
        int ret = ibv_modify_qp(qp, &attr, mask);
        if (ret) {
            printf("Failed to modify to INIT\n");
            return ret;
        }
    }

    {
        // Modify QP to ready-to-receive
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_AV | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        ibv_qp_attr attr = {
            .qp_state = ibv_qp_state::IBV_QPS_RTR,
            .path_mtu = ibv_mtu::IBV_MTU_4096,
            .rq_psn = remote_qp_num,
            .dest_qp_num = remote_qp_num,
            .ah_attr =
                {
                    .grh {
                        .dgid = gid,
                        .flow_label = 0,
                        .sgid_index = 0,
                        .hop_limit = 255,
                    },
                    .dlid = lid,
                    .is_global = 1,
                    .port_num = 1,
                },
            .max_dest_rd_atomic = 16,
            .min_rnr_timer = 0,
        };

        int ret = ibv_modify_qp(qp, &attr, mask);
        if (ret) {
            printf("Failed to modify to RTR\n");
            return ret;
        }
    }

    {
        // Modify QP to ready-to-send
        int mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT | ibv_qp_attr_mask::IBV_QP_RNR_RETRY | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

        ibv_qp_attr attr = {
            .qp_state = ibv_qp_state::IBV_QPS_RTS,
            .sq_psn = qp->qp_num,
            .max_rd_atomic = 16,
            .timeout = 14,
            .retry_cnt = 7,
            .rnr_retry = 7,
        };

        int ret = ibv_modify_qp(qp, &attr, mask);
        if (ret) {
            printf("Failed to modify to RTS\n");
            return ret;
        }
    }

    {
        // Check QP state
        ibv_qp_attr attr;
        ibv_qp_init_attr init_attr;
        ibv_query_qp(qp, &attr, ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_CUR_STATE, &init_attr);
        printf("qp_state %d\n", attr.qp_state);
        printf("cur_qp_state %d\n", attr.cur_qp_state);
    }

    return 0;
}

}  // namespace rdma_util