#ifndef _RDMA_UTIL_H_
#define _RDMA_UTIL_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace rdma_util {

struct HandshakeData {
    ibv_gid gid;
    uint16_t lid;
    uint32_t qp_num;
};

enum QueuePairState {
    RESET = 0,
    INIT = 1,
    RTR = 2,
    RTS = 3,
    UNKNOWN = 4,
};

struct WorkCompletion {
    // See ibv_wc_status for detailed meaining of status
    int32_t status;
    uint64_t wr_id;
    uint32_t bytes_transferred;

    inline std::string to_string() const {
        return "WorkCompletion { status: " + std::to_string(status) + ", wr_id: " + std::to_string(wr_id)
            + ", bytes_transferred: " + std::to_string(bytes_transferred) + " }";
    }
};

class Context;
class ProtectionDomain;
class RcQueuePair;
class MemoryRegion;

class Context {
    friend class ProtectionDomain;
    friend class MemoryRegion;
    friend class RcQueuePair;

  private:
    ibv_context* inner;

    Context(const char* dev_name) noexcept(false);

  public:
    Context() = delete;
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;

    ~Context();

    static std::shared_ptr<Context> create(const char* dev_name) noexcept(false);
};

class ProtectionDomain {
    friend class Context;
    friend class MemoryRegion;
    friend class RcQueuePair;

  private:
    ibv_pd* inner;

    std::shared_ptr<Context> context_;

    ProtectionDomain(std::shared_ptr<Context> context) noexcept(false);

  public:
    ProtectionDomain() = delete;
    ProtectionDomain(const ProtectionDomain&) = delete;
    ProtectionDomain& operator=(const ProtectionDomain&) = delete;

    ~ProtectionDomain();

    static std::shared_ptr<ProtectionDomain> create(std::shared_ptr<Context> context) noexcept(false);

    inline std::shared_ptr<Context> get_context() const {
        return this->context_;
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

    RcQueuePair(std::shared_ptr<ProtectionDomain> pd) noexcept(false);

  public:
    RcQueuePair() = delete;
    RcQueuePair(const RcQueuePair&) = delete;
    RcQueuePair& operator=(const RcQueuePair&) = delete;

    ~RcQueuePair();

    static std::shared_ptr<RcQueuePair> create(const char* dev_name) noexcept(false);
    static std::shared_ptr<RcQueuePair> create(std::shared_ptr<Context> context) noexcept(false);
    static std::shared_ptr<RcQueuePair> create(std::shared_ptr<ProtectionDomain> pd) noexcept(false);

    inline std::shared_ptr<ProtectionDomain> get_pd() const {
        return this->pd_;
    }

    inline std::shared_ptr<Context> get_context() const {
        return this->context_;
    }

    QueuePairState query_qp_state() noexcept(false);

    HandshakeData get_handshake_data() noexcept(false);

    void bring_up(const HandshakeData& handshake_data) noexcept(false);

    int post_send_send(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey, bool signaled) noexcept;

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

    int post_recv(uint64_t wr_id, uint64_t addr, uint32_t length, uint32_t lkey) noexcept;

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

    int poll_send_cq_once(const int max_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept;

    int poll_recv_cq_once(const int max_num_wcs, std::vector<WorkCompletion>& polled_wcs) noexcept;
};

class MemoryRegion {
    friend class Context;
    friend class ProtectionDomain;
    friend class RcQueuePair;

  private:
    ibv_mr* inner;

    std::shared_ptr<ProtectionDomain> pd_;
    std::shared_ptr<Context> context_;

    MemoryRegion(std::shared_ptr<ProtectionDomain> pd, void* addr, size_t length) noexcept(false);

  public:
    MemoryRegion() = delete;
    MemoryRegion(const MemoryRegion&) = delete;
    MemoryRegion& operator=(const MemoryRegion&) = delete;

    ~MemoryRegion();

    static std::shared_ptr<MemoryRegion>
    create(std::shared_ptr<ProtectionDomain> pd, void* addr, size_t length) noexcept(false);

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
};

}  // namespace rdma_util

#endif  // _RDMA_UTIL_H_