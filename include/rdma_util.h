#ifndef _RDMA_UTIL_H_
#define _RDMA_UTIL_H_

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

namespace rdma_util {

struct HandshakeData {
    ibv_gid gid;
    uint16_t lid;
    uint32_t qp_num;
};

void* malloc_gpu_buffer(size_t length, const char* bdf);

enum Result_t {
    SUCCESS = 0,
    DEVICE_NOT_FOUND = 1,
    DEVICE_OPEN_FAILED = 2,
    DEVICE_CLOSE_FAILED = 3,
};

Result_t open_ib_device(ibv_context** ret, const char* dev_name);
Result_t close_ib_device(ibv_context* context);
ibv_qp* create_rc(ibv_context* context);
int bring_up_rc(ibv_qp* qp, HandshakeData& data);
int query_handshake_data(ibv_qp* qp, HandshakeData* handshake_data);

// class Context;
// class ProtectionDomain;
// class QueuePair;
// class MemoryRegion;
// class Identifer;

// class Context {
//   private:
//     ibv_context* context_;

//     Context(const char* dev_name) {
//         auto dev_list = ibv_get_device_list(nullptr);
//         if (!dev_list) {
//             throw std::runtime_error("Failed to get device list");
//         }

//         ibv_device* dev = nullptr;
//         for (int i = 0; dev_list[i] != nullptr; i++) {
//             if (strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
//                 dev = dev_list[i];
//                 break;
//             }
//         }

//         if (dev == nullptr) {
//             ibv_free_device_list(dev_list);
//             throw std::runtime_error("Device not found");
//         }

//         auto context = ibv_open_device(dev);
//         if (context == nullptr) {
//             ibv_free_device_list(dev_list);
//             throw std::runtime_error("Failed to open device");
//         }

//         this->context_ = context;
//     }

//   public:
//     inline ibv_context* as_ptr() {
//         return this->context_;
//     }

//     static std::shared_ptr<Context> create(const char* dev_name) {
//         return std::make_shared<Context>(dev_name);
//     }

//     ~Context() {
//         if (this->context_) {
//             ibv_close_device(this->context_);
//         }
//     }
// };

// class ProtectionDomain {
//   private:
//     ibv_pd* pd_;

//     std::shared_ptr<Context> context_;

//     ProtectionDomain(std::shared_ptr<Context> context) : context_(context) {
//         this->pd_ = ibv_alloc_pd(context->as_ptr());
//         if (this->pd_ == nullptr) {
//             throw std::runtime_error("Failed to allocate protection domain");
//         }
//     }

//   public:
//     inline ibv_pd* as_ptr() {
//         return this->pd_;
//     }

//     static std::shared_ptr<ProtectionDomain> create(std::shared_ptr<Context> context) {
//         return std::make_shared<ProtectionDomain>(context);
//     }

//     ~ProtectionDomain() {
//         if (this->pd_) {
//             ibv_dealloc_pd(this->pd_);
//         }
//     }
// };

// class QueuePair {
//   private:
//     ibv_qp* qp_;

//     std::shared_ptr<ProtectionDomain> pd_;
//     std::shared_ptr<Context> context_;
// };

// class MemoryRegion {
//   private:
//     ibv_mr* mr_;

//     std::shared_ptr<ProtectionDomain> pd_;
//     std::shared_ptr<Context> context_;
// };

// class Identifer {
//   private:
//     rdma_cm_id* id_;

//     std::shared_ptr<Context> context_;
//     std::shared_ptr<QueuePair> qp_;

//     Identifer(std::shared_ptr<Context> context) : context_(context) {
//         rdma_create_id(nullptr, &this->id_, nullptr, RDMA_PS_TCP);
//     }
// };

}  // namespace rdma_util

#endif  // _RDMA_UTIL_H_