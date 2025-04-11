#ifndef _RDMA_UTIL_H_
#define _RDMA_UTIL_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "concurrentqueue.h"

namespace rdma_util {

struct HandshakeData {
    ibv_gid gid;
    uint16_t lid;
    uint32_t qp_num;
};

struct WorkCompletion {
    uint64_t wr_id;

    // See ibv_wc_status for detailed meaining of status
    uint32_t status;

    uint32_t byte_len;

    // See ibv_wc_status for detailed meaining of status
    uint32_t opcode;
    uint32_t imm_data;

    inline std::string to_string() const {
        return "wr_id: " + std::to_string(wr_id) + ", status: " + std::to_string(status)
            + ", byte_len: " + std::to_string(byte_len) + ", opcode: " + std::to_string(opcode)
            + ", imm_data: " + std::to_string(imm_data);
    }
};

class Context;
class ProtectionDomain;
class CompletionQueue;
class RcQueuePair;
class MemoryRegion;

class Context {
    friend class ProtectionDomain;
    friend class MemoryRegion;
    friend class RcQueuePair;
    friend class CompletionQueue;

private:
    ibv_context* inner;

    Context() = delete;
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;
    Context(Context&&) = delete;
    Context& operator=(Context&&) = delete;

    Context(const char* dev_name) noexcept(false);

public:
    static std::unique_ptr<Context> create(const char* dev_name) noexcept(false);

    ~Context();
};

class ProtectionDomain {
    friend class Context;
    friend class MemoryRegion;
    friend class RcQueuePair;

private:
    ibv_pd* inner;

    std::shared_ptr<Context> context_;

    ProtectionDomain() = delete;
    ProtectionDomain(const ProtectionDomain&) = delete;
    ProtectionDomain& operator=(const ProtectionDomain&) = delete;
    ProtectionDomain(ProtectionDomain&&) = delete;
    ProtectionDomain& operator=(ProtectionDomain&&) = delete;

    ProtectionDomain(std::shared_ptr<Context> context) noexcept(false);

public:
    static std::unique_ptr<ProtectionDomain> create(std::shared_ptr<Context> context) noexcept(false);

    ~ProtectionDomain();

    inline std::shared_ptr<Context> get_context() const {
        return this->context_;
    }
};

class CompletionQueue {
    friend class RcQueuePair;

private:
    ibv_cq* inner;

    std::shared_ptr<Context> context_;

    CompletionQueue() = delete;
    CompletionQueue(const CompletionQueue&) = delete;
    CompletionQueue& operator=(const CompletionQueue&) = delete;
    CompletionQueue(CompletionQueue&&) = delete;
    CompletionQueue& operator=(CompletionQueue&&) = delete;

    CompletionQueue(std::shared_ptr<Context> context, int cqe) noexcept(false);

public:
    static std::shared_ptr<CompletionQueue> create(std::shared_ptr<Context> context, int cqe = 128) noexcept(false);

    ~CompletionQueue();
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

    RcQueuePair() = delete;
    RcQueuePair(const RcQueuePair&) = delete;
    RcQueuePair& operator=(const RcQueuePair&) = delete;
    RcQueuePair(RcQueuePair&&) = delete;
    RcQueuePair& operator=(RcQueuePair&&) = delete;

    RcQueuePair(
        std::shared_ptr<ProtectionDomain> pd,
        std::shared_ptr<CompletionQueue> send_cq,
        std::shared_ptr<CompletionQueue> recv_cq
    ) noexcept(false);

public:
    static std::unique_ptr<RcQueuePair> create(const char* dev_name) noexcept(false);
    static std::unique_ptr<RcQueuePair> create(std::shared_ptr<Context> context) noexcept(false);
    static std::unique_ptr<RcQueuePair> create(std::shared_ptr<ProtectionDomain> pd) noexcept(false);
    static std::unique_ptr<RcQueuePair> create(
        std::shared_ptr<ProtectionDomain> pd,
        std::shared_ptr<CompletionQueue> send_cq,
        std::shared_ptr<CompletionQueue> recv_cq
    ) noexcept(false);

    ~RcQueuePair();

    inline std::shared_ptr<ProtectionDomain> get_pd() const {
        return this->pd_;
    }

    inline std::shared_ptr<Context> get_context() const {
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
        uint32_t imm,
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
        uint32_t imm,
        uint32_t lkey,
        uint32_t rkey,
        bool signaled
    ) noexcept;

    int post_send_wrs(ibv_send_wr* wr_list) noexcept;

    int post_recv(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey) noexcept;

    int post_recv_wrs(ibv_recv_wr* wr_list) noexcept;

    /**
     * @brief poll the send_cq until at least `num_expected_completions` 
     * work completions are polled or an error occurs
     * 
     * @param expected_num_wcs expected number of completions
     * @param polled_wcs return value to store the polled wr_ids and status
     * @return int 0 on success, other values on error. Refer to
     * https://www.rdmamojo.com/2013/02/15/ibv_poll_cq/ for more information.
     */
    int wait_until_send_completion(const int expected_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept;

    /**
     * @brief poll the recv_cq until at least `num_expected_completions` 
     * work completions are polled or an error occurs
     * 
     * @param expected_num_wcs expected number of completions
     * @param polled_wcs return value to store the polled wr_ids and status
     * @return int 0 on success, other values on error. Refer to
     * https://www.rdmamojo.com/2013/02/15/ibv_poll_cq/ for more information.
     */
    int wait_until_recv_completion(const int expected_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept;

    /**
     * @brief poll the send_cq once and return the number of polled work completions on success
     * 
     * @param max_num_wcs maximum number of work completions to poll
     * @param polled_wcs return value to store the polled wr_ids and status
     */
    int poll_send_cq_once(const int max_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept;

    /**
     * @brief poll the recv_cq once and return the number of polled work completions on success
     * 
     * @param max_num_wcs maximum number of work completions to poll
     * @param polled_wcs return value to store the polled wr_ids and status
     */
    int poll_recv_cq_once(const int max_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept;

    int poll_send_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs);
    int poll_recv_cq_once(const int max_num_wcs, std::vector<ibv_wc>& polled_wcs);
};

class MemoryRegion {
    friend class Context;
    friend class ProtectionDomain;
    friend class RcQueuePair;

private:
    ibv_mr* inner;

    std::shared_ptr<ProtectionDomain> pd_;
    std::shared_ptr<Context> context_;

    // This could be nullptr if the buffer is created with a raw pointer
    std::shared_ptr<void> inner_buffer_with_deleter_;

    MemoryRegion() = delete;
    MemoryRegion(const MemoryRegion&) = delete;
    MemoryRegion& operator=(const MemoryRegion&) = delete;
    MemoryRegion(MemoryRegion&&) = delete;
    MemoryRegion& operator=(MemoryRegion&&) = delete;

    MemoryRegion(
        std::shared_ptr<ProtectionDomain> pd,
        std::shared_ptr<void> buffer_with_deleter,
        uint64_t length
    ) noexcept(false);

    MemoryRegion(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false);

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
    create(std::shared_ptr<ProtectionDomain> pd, std::shared_ptr<void> buffer_with_deleter, uint64_t length) noexcept(
        false
    );

    /**
     * @brief Create a MemoryRegion object with a buffer that has a deleter
     * 
     * SAFETY: The caller must ensure that the buffer has longer lifetime than the returned MemoryRegion object
     * 
     * @param pd ProtectionDomain object
     * @param addr address of the raw buffer
     * @param length length of the buffer
     */
    static std::unique_ptr<MemoryRegion>
    create(std::shared_ptr<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false);

    ~MemoryRegion();

    inline uint32_t get_lkey() const {
        return this->inner->lkey;
    }

    inline uint32_t get_rkey() const {
        return this->inner->rkey;
    }

    inline void* get_addr() const {
        return this->inner->addr;
    }

    inline uint64_t get_length() const {
        return this->inner->length;
    }

    inline std::shared_ptr<ProtectionDomain> get_pd() const {
        return this->pd_;
    }
};

template<typename T>
using Queue = moodycamel::ConcurrentQueue<T>;

struct Ticket {
    uint32_t stream_id;
    uint32_t length;
    uint64_t addr;
    uint32_t key;
    uint32_t padding_;

    inline std::string to_string() const {
        std::stringstream ss;
        ss << "stream_id: " << stream_id << std::hex << std::uppercase << ", length: " << length << ", addr: " << addr
           << ", key: " << key << ", padding: " << padding_;
        return ss.str();
    }
};

}  // namespace rdma_util

#endif  // _RDMA_UTIL_H_