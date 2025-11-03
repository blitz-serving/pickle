#ifndef _RDMA_UTIL_H_
#define _RDMA_UTIL_H_

#include <infiniband/verbs.h>
#include <linux/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pickle_logger.h"

namespace rdma_util {

struct HandshakeData {
    ibv_gid gid;
    uint16_t lid;
    uint32_t qp_num;
};

class Context;
class ProtectionDomain;
class CompletionQueue;
class RcQueuePair;
class MemoryRegion;

struct DeviceInfo {
    std::string device_name;
    __be64 guid;

    template<typename T>
    explicit DeviceInfo(T&& device_name, __be64 guid) : device_name(std::forward<T>(device_name)), guid(guid) {}
};

class Context {
    friend class ProtectionDomain;
    friend class MemoryRegion;
    friend class RcQueuePair;
    friend class CompletionQueue;

private:
    ibv_context* inner;

    Context(const char* device_name) noexcept(false) {
        int num_devices = 0;
        auto device_list = ibv_get_device_list(&num_devices);
        if (device_list == nullptr) {
            throw std::runtime_error("Failed to get device list");
        }

        for (int i = 0; device_list[i] != nullptr; i++) {
            if (strcmp(ibv_get_device_name(device_list[i]), device_name) == 0) {
                ibv_context* context = ibv_open_device(device_list[i]);
                ibv_free_device_list(device_list);
                if (context == nullptr) {
                    throw std::runtime_error("Failed to open device");
                } else {
                    this->inner = context;
                }
                return;
            }
        }

        ibv_free_device_list(device_list);
        throw std::runtime_error("Device not found");
    }

public:
    static std::unique_ptr<Context> create(const char* device_name) noexcept(false) {
        DEBUG("rdma_util::Context::create() creating context using {}", device_name);
        return std::unique_ptr<Context>(new Context(device_name));
    }

    static std::vector<DeviceInfo> get_device_infos() noexcept(false) {
        int num_devices = 0;
        auto device_list = ibv_get_device_list(&num_devices);
        if (device_list == nullptr) {
            throw std::runtime_error("Failed to get device list");
        }
        std::vector<DeviceInfo> devices;
        devices.reserve(num_devices);
        for (int i = 0; i < num_devices; i++) {
            devices.emplace_back(ibv_get_device_name(device_list[i]), ibv_get_device_guid(device_list[i]));
        }
        ibv_free_device_list(device_list);
        return devices;
    }

    Context() = delete;
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;
    Context(Context&&) = delete;
    Context& operator=(Context&&) = delete;

    ~Context() {
        DEBUG("rdma_util::Context::~Context()");
        if (this->inner) {
            ibv_close_device(this->inner);
        }
    }
};

class ProtectionDomain {
    friend class Context;
    friend class MemoryRegion;
    friend class RcQueuePair;

private:
    ibv_pd* inner;

    std::shared_ptr<Context> context_;

    ProtectionDomain(std::shared_ptr<Context> context) noexcept(false) : context_(std::move(context)) {
        this->inner = ibv_alloc_pd(this->context_->inner);
        if (this->inner == nullptr) {
            throw std::runtime_error("Failed to allocate protection domain");
        }
    }

public:
    static std::unique_ptr<ProtectionDomain> create(std::shared_ptr<Context> context) noexcept(false) {
        return std::unique_ptr<ProtectionDomain>(new ProtectionDomain(std::move(context)));
    }

    std::shared_ptr<Context> get_context() const {
        return this->context_;
    }

    ProtectionDomain() = delete;
    ProtectionDomain(const ProtectionDomain&) = delete;
    ProtectionDomain& operator=(const ProtectionDomain&) = delete;
    ProtectionDomain(ProtectionDomain&&) = delete;
    ProtectionDomain& operator=(ProtectionDomain&&) = delete;

    ~ProtectionDomain() {
        DEBUG("rdma_util::ProtectionDomain::~ProtectionDomain()");
        if (this->inner) {
            ibv_dealloc_pd(this->inner);
        }
    }
};

class CompletionQueue {
    friend class RcQueuePair;

private:
    ibv_cq* inner;

    std::shared_ptr<Context> context_;

    CompletionQueue(std::shared_ptr<Context> context, int cqe) noexcept(false) : context_(std::move(context)) {
        this->inner = ibv_create_cq(this->context_->inner, cqe, nullptr, nullptr, 0);
        if (this->inner == nullptr) {
            throw std::runtime_error("Failed to create completion queue");
        }
    }

public:
    static std::shared_ptr<CompletionQueue> create(std::shared_ptr<Context> context, int cqe = 128) noexcept(false) {
        return std::shared_ptr<CompletionQueue>(new CompletionQueue(std::move(context), cqe));
    }

    CompletionQueue() = delete;
    CompletionQueue(const CompletionQueue&) = delete;
    CompletionQueue& operator=(const CompletionQueue&) = delete;
    CompletionQueue(CompletionQueue&&) = delete;
    CompletionQueue& operator=(CompletionQueue&&) = delete;

    ~CompletionQueue() {
        DEBUG("rdma_util::CompletionQueue::~CompletionQueue()");
        if (this->inner) {
            ibv_destroy_cq(this->inner);
        }
    }
};

class RcQueuePair {
    friend class Context;
    friend class MemoryRegion;
    friend class ProtectionDomain;

private:
    ibv_qp* inner;

    std::shared_ptr<ProtectionDomain> pd_;
    std::shared_ptr<Context> context_;
    std::shared_ptr<CompletionQueue> send_cq_;
    std::shared_ptr<CompletionQueue> recv_cq_;

    RcQueuePair(
        std::shared_ptr<ProtectionDomain> pd,
        std::shared_ptr<CompletionQueue> send_cq,
        std::shared_ptr<CompletionQueue> recv_cq
    ) noexcept(false);

public:
    RcQueuePair() = delete;
    RcQueuePair(const RcQueuePair&) = delete;
    RcQueuePair& operator=(const RcQueuePair&) = delete;
    RcQueuePair(RcQueuePair&&) = delete;
    RcQueuePair& operator=(RcQueuePair&&) = delete;

    ~RcQueuePair();

    static std::unique_ptr<RcQueuePair> create(const char* device_name) noexcept(false);

    static std::unique_ptr<RcQueuePair> create(std::shared_ptr<Context> context) noexcept(false);

    static std::unique_ptr<RcQueuePair> create(std::shared_ptr<ProtectionDomain> pd) noexcept(false);

    static std::unique_ptr<RcQueuePair> create(
        std::shared_ptr<ProtectionDomain> pd,
        std::shared_ptr<CompletionQueue> send_cq,
        std::shared_ptr<CompletionQueue> recv_cq
    ) noexcept(false);

    std::shared_ptr<ProtectionDomain> get_pd() const {
        return this->pd_;
    }

    std::shared_ptr<Context> get_context() const {
        return this->context_;
    }

    ibv_qp_state query_qp_state() noexcept(false);

    /**
     * @brief get the handshake data of the RC queue pair
     *
     * @param sgid_index index of the source GID to use. Manual setting is required for RoCE.
     */
    HandshakeData get_handshake_data(uint32_t gid_index = 0) noexcept(false);

    /**
     * @brief bring up an RC queue pair with the given handshake data
     * 
     * @param handshake_data handshake data from the remote RC queue pair
     * @param sgid_index index of the source GID to use. Manual setting is required for RoCE.
     * @param rate rate limit for the queue pair. Default is IBV_RATE_MAX
     */
    void bring_up(
        const HandshakeData& handshake_data,
        uint32_t gid_index = 0,
        ibv_rate rate = ibv_rate::IBV_RATE_MAX
    ) noexcept(false);

    int post_send_send(uint64_t wr_id, uint64_t laddr, uint32_t length, uint32_t lkey, bool signaled) noexcept;

    int post_send_send_with_imm(
        uint64_t wr_id,
        uint64_t laddr,
        uint32_t length,
        uint32_t lkey,
        __be32 imm_data,
        bool signaled
    ) noexcept;

    int post_send_read(
        uint64_t wr_id,
        uint64_t laddr,
        uint64_t raddr,
        uint32_t length,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled
    ) noexcept;

    int post_send_write(
        uint64_t wr_id,
        uint64_t laddr,
        uint64_t raddr,
        uint32_t length,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled
    ) noexcept;

    int post_send_write_with_imm(
        uint64_t wr_id,
        uint64_t laddr,
        uint64_t raddr,
        uint32_t length,
        __be32 imm_data,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled
    ) noexcept;

    /**
     * @brief fill the work request structure and scatter-gather 
     * entry for a send operation
     * 
     * @param wr_id work request id
     * @param laddr local address
     * @param length length of the data to send
     * @param lkey local memory region key
     * @param signaled whether the work request is signaled
     * @return wr work request structure to fill
     * @return sge scatter-gather entry to fill
     */
    static void fill_post_send_send_wr(
        uint64_t wr_id,
        uint64_t laddr,
        uint32_t length,
        uint32_t lkey,
        bool signaled,
        ibv_send_wr& wr,
        ibv_sge& sge
    ) noexcept;

    /**
     * @brief fill the work request structure and scatter-gather 
     * entry for a send operation with immediate data
     * 
     * @param wr_id work request id
     * @param laddr local address
     * @param length length of the data to send
     * @param lkey local memory region key
     * @param imm_data immediate data to send
     * @param signaled whether the work request is signaled
     * @return wr work request structure to fill
     * @return sge scatter-gather entry to fill
     */
    static void fill_post_send_send_with_imm_wr(
        uint64_t wr_id,
        uint64_t laddr,
        uint32_t length,
        uint32_t lkey,
        __be32 imm_data,
        bool signaled,
        ibv_send_wr& wr,
        ibv_sge& sge
    ) noexcept;

    /**
     * @brief fill the work request structure and scatter-gather 
     * entry for a read operation
     * 
     * @param wr_id work request id
     * @param laddr local address
     * @param raddr remote address
     * @param length length of the data to read
     * @param lkey local memory region key
     * @param rkey remote memory region key
     * @param signaled whether the work request is signaled
     * @return wr work request structure to fill
     * @return sge scatter-gather entry to fill
     */
    static void fill_post_send_read_wr(
        uint64_t wr_id,
        uint64_t laddr,
        uint64_t raddr,
        uint32_t length,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled,
        ibv_send_wr& wr,
        ibv_sge& sge
    ) noexcept;

    /**
     * @brief fill the work request structure and scatter-gather 
     * entry for a write operation
     * 
     * @param wr_id work request id
     * @param laddr local address
     * @param raddr remote address
     * @param length length of the data to write
     * @param lkey local memory region key
     * @param rkey remote memory region key
     * @param signaled whether the work request is signaled
     * @return wr work request structure to fill
     * @return sge scatter-gather entry to fill
     */
    static void fill_post_send_write_wr(
        uint64_t wr_id,
        uint64_t laddr,
        uint64_t raddr,
        uint32_t length,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled,
        ibv_send_wr& wr,
        ibv_sge& sge
    ) noexcept;

    /**
     * @brief fill the work request structure and scatter-gather 
     * entry for a write operation with immediate data
     * 
     * @param wr_id work request id
     * @param laddr local address
     * @param raddr remote address
     * @param length length of the data to write
     * @param imm_data immediate data to send
     * @param lkey local memory region key
     * @param rkey remote memory region key
     * @param signaled whether the work request is signaled
     * @return wr work request structure to fill
     * @return sge scatter-gather entry to fill
     */
    static void fill_post_send_write_with_imm_wr(
        uint64_t wr_id,
        uint64_t laddr,
        uint64_t raddr,
        uint32_t length,
        __be32 imm_data,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled,
        ibv_send_wr& wr,
        ibv_sge& sge
    ) noexcept;

    /**
     * @brief freeze the work request list by linking the work requests
     * together. The last work request's next pointer is set to nullptr.
     * 
     * @param wr_list work request list to freeze
     * @param length length of the work request list, must be greater than zero
     * 
     * @return return 0 on success and -1 when length is zero
     */
    static int freeze_wr_list(ibv_send_wr* wr_list, uint64_t length) noexcept;

    int post_send_wrs(ibv_send_wr* wr_list) noexcept;

    int post_recv(uint64_t wr_id, uint64_t laddr, uint32_t length, uint32_t lkey) noexcept;

    int post_recv_wrs(ibv_recv_wr* wr_list) noexcept;

    /**
     * @brief poll the send_cq until at least `num_expected_completions` 
     * work completions are polled or an error occurs
     * 
     * @param expected_num_wcs expected number of completions
     * 
     * @return polled_wcs store the polled wr_ids and status
     * @return return 0 on success and other values on error
     */
    int wait_until_send_completion(const int expected_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept;

    /**
     * @brief poll the recv_cq until at least `num_expected_completions` 
     * work completions are polled or an error occurs
     * 
     * @param expected_num_wcs expected number of completions
     * 
     * @return polled_wcs store the polled wr_ids and status
     * @return return 0 on success and other values on error
     */
    int wait_until_recv_completion(const int expected_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept;

    /**
     * @brief poll the recv_cq once and return the number of polled work completions on success.
     * Refer to https://www.rdmamojo.com/2013/02/15/ibv_poll_cq/ for more information.
     * 
     * @param max_num_wcs maximum number of work completions to poll
     *
     * @return return value:
     * - zero: The CQ is empty
     * - positive: Number of Work Completions that were read from the CQ
     * - negative: A failure occurred while trying to read Work Completions from the CQ
     * @return polled_wcs: store the polled wr_ids and status
     */
    int poll_send_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept;

    /**
     * @brief poll the recv_cq once and return the number of polled work completions on success.
     * Refer to https://www.rdmamojo.com/2013/02/15/ibv_poll_cq/ for more information.
     * 
     * @param max_num_wcs maximum number of work completions to poll
     *
     * @return return value:
     * - zero: The CQ is empty
     * - positive: Number of Work Completions that were read from the CQ
     * - negative: A failure occurred while trying to read Work Completions from the CQ
     * @return polled_wcs: store the polled wr_ids and status
     */
    int poll_recv_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs) noexcept;
};

class MemoryRegion {
    friend class Context;
    friend class ProtectionDomain;
    friend class RcQueuePair;

private:
    ibv_mr* inner;

    std::shared_ptr<Context> context_;
    std::shared_ptr<ProtectionDomain> pd_;

    // This could be nullptr if the buffer is created with a raw pointer
    std::shared_ptr<void> inner_buffer_with_deleter_;

    MemoryRegion(std::shared_ptr<ProtectionDomain> pd, std::shared_ptr<void> buffer_with_deleter, uint64_t length) :
        context_(pd->get_context()),
        pd_(std::move(pd)),
        inner_buffer_with_deleter_(std::move(buffer_with_deleter)) {
        this->inner = ibv_reg_mr(
            this->pd_->inner,
            this->inner_buffer_with_deleter_.get(),
            length,
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        );
        if (this->inner == nullptr) {
            throw std::runtime_error("Failed to register memory region");
        }
    }

    MemoryRegion(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) :
        context_(pd->get_context()),
        pd_(std::move(pd)),
        inner_buffer_with_deleter_(nullptr) {
        this->inner = ibv_reg_mr(
            this->pd_->inner,
            addr,
            length,
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        );
        if (this->inner == nullptr) {
            throw std::runtime_error("Failed to register memory region");
        }
    }

public:
    /**
     * @brief Create a MemoryRegion object with a buffer that has a deleter
     * 
     * SAFETY: The caller must ensure that the buffer is valid and it has a deleter that handles the buffer's deallocation
     * 
     * @param pd ProtectionDomain object
     * @param buffer_with_deleter buffer with a deleter
     * @param length length of the buffer
     */
    static std::unique_ptr<MemoryRegion>
    create(std::shared_ptr<ProtectionDomain> pd, std::shared_ptr<void> buffer_with_deleter, uint64_t length) {
        return std::unique_ptr<MemoryRegion>(new MemoryRegion(std::move(pd), std::move(buffer_with_deleter), length));
    }

    /**
     * @brief Create a MemoryRegion object with a buffer that has a deleter
     * 
     * SAFETY: The caller must ensure that the buffer has longer lifetime than the returned MemoryRegion object
     * 
     * @param pd ProtectionDomain object
     * @param addr address of the raw buffer
     * @param length length of the buffer
     */
    static std::unique_ptr<MemoryRegion> create(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) {
        return std::unique_ptr<MemoryRegion>(new MemoryRegion(std::move(pd), addr, length));
    }

    MemoryRegion() = delete;
    MemoryRegion(const MemoryRegion&) = delete;
    MemoryRegion& operator=(const MemoryRegion&) = delete;
    MemoryRegion(MemoryRegion&&) = delete;
    MemoryRegion& operator=(MemoryRegion&&) = delete;

    ~MemoryRegion() {
        DEBUG("rdma_util::MemoryRegion::~MemoryRegion()");
        if (this->inner) {
            ibv_dereg_mr(this->inner);
        }
    }

    uint32_t get_lkey() const {
        return this->inner->lkey;
    }

    uint32_t get_rkey() const {
        return this->inner->rkey;
    }

    void* get_addr() const {
        return this->inner->addr;
    }

    uint64_t get_length() const {
        return this->inner->length;
    }

    std::shared_ptr<ProtectionDomain> get_pd() const {
        return this->pd_;
    }
};

}  // namespace rdma_util

#endif  // _RDMA_UTIL_H_