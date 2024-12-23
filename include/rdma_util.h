#ifndef _RDMA_UTIL_H_
#define _RDMA_UTIL_H_

#include <infiniband/verbs.h>

#include <map>
#include <memory>
#include <queue>
#include <sstream>

#include "concurrentqueue.h"

namespace rdma_util {

template<typename T>
using Arc = std::shared_ptr<T>;

template<typename T>
using Box = std::unique_ptr<T>;

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

    static Box<Context> create(const char* dev_name) noexcept(false);
};

class ProtectionDomain {
    friend class Context;
    friend class MemoryRegion;
    friend class RcQueuePair;

  private:
    ibv_pd* inner;

    Arc<Context> context_;

    ProtectionDomain(Arc<Context> context) noexcept(false);

  public:
    ProtectionDomain() = delete;
    ProtectionDomain(const ProtectionDomain&) = delete;
    ProtectionDomain& operator=(const ProtectionDomain&) = delete;

    ~ProtectionDomain();

    static Box<ProtectionDomain> create(Arc<Context> context) noexcept(false);

    inline Arc<Context> get_context() const {
        return this->context_;
    }
};

class RcQueuePair {
    friend class Context;
    friend class MemoryRegion;
    friend class ProtectionDomain;

  private:
    ibv_qp* inner;

    Arc<ProtectionDomain> pd_;
    Arc<Context> context_;

    RcQueuePair(Arc<ProtectionDomain> pd) noexcept(false);

  public:
    RcQueuePair() = delete;
    RcQueuePair(const RcQueuePair&) = delete;
    RcQueuePair& operator=(const RcQueuePair&) = delete;

    ~RcQueuePair();

    static Box<RcQueuePair> create(const char* dev_name) noexcept(false);
    static Box<RcQueuePair> create(Arc<Context> context) noexcept(false);
    static Box<RcQueuePair> create(Arc<ProtectionDomain> pd) noexcept(false);

    inline Arc<ProtectionDomain> get_pd() const {
        return this->pd_;
    }

    inline Arc<Context> get_context() const {
        return this->context_;
    }

    QueuePairState query_qp_state() noexcept(false);

    HandshakeData get_handshake_data() noexcept(false);

    void bring_up(const HandshakeData& handshake_data, ibv_rate rate = ibv_rate::IBV_RATE_MAX) noexcept(false);

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

    /**
     * UNSAFE: This function panics if the size of `wc_buffer` is less than the sizeof `ibv_wc[max_num_wcs]`
     * @brief poll the send_cq once and return the number of polled work completions on success
     * 
     * @param max_num_wcs maximum number of work completions to poll
     * @param wc_buffer work completion buffer. You must ensure that the buffer is larger than the sizeof `ibv_wc[max_num_wcs]`
     * @param polled_wcs return value to store the polled wr_ids and status
     */
    int poll_send_cq_once(const int max_num_wcs, ibv_wc* wc_buffer, std::vector<WorkCompletion>& polled_wcs);

    /**
     * UNSAFE: This function panics if the size of `wc_buffer` is less than the sizeof `ibv_wc[max_num_wcs]`
     * @brief poll the recv_cq once and return the number of polled work completions on success
     * 
     * @param max_num_wcs maximum number of work completions to poll
     * @param wc_buffer work completion buffer. You must ensure that the buffer is larger than the sizeof `ibv_wc[max_num_wcs]`
     * @param polled_wcs return value to store the polled wr_ids and status
     */
    int poll_recv_cq_once(const int max_num_wcs, ibv_wc* wc_buffer, std::vector<WorkCompletion>& polled_wcs);
};

class MemoryRegion {
    friend class Context;
    friend class ProtectionDomain;
    friend class RcQueuePair;

  private:
    ibv_mr* inner;

    Arc<ProtectionDomain> pd_;
    Arc<Context> context_;

    // This could be nullptr if the buffer is created with a raw pointer
    Arc<void> inner_buffer_with_deleter_;

    MemoryRegion(Arc<ProtectionDomain> pd, Arc<void> buffer_with_deleter, uint64_t length) noexcept(false);

    MemoryRegion(Arc<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false);

  public:
    MemoryRegion() = delete;
    MemoryRegion(const MemoryRegion&) = delete;
    MemoryRegion& operator=(const MemoryRegion&) = delete;

    ~MemoryRegion();

    /**
     * @brief Create a MemoryRegion object with a buffer that has a deleter
     * 
     * SAFETY: The caller must ensure that the buffer is valid and it has a deleter that handles the buffer's deallocation
     * 
     * @param pd ProtectionDomain object
     * @param buffer_with_deleter buffer with a deleter
     * @param length length of the buffer
     */
    static Box<MemoryRegion>
    create(Arc<ProtectionDomain> pd, Arc<void> buffer_with_deleter, uint64_t length) noexcept(false);

    /**
     * @brief Create a MemoryRegion object with a buffer that has a deleter
     * 
     * SAFETY: The caller must ensure that the buffer has longer lifetime than the returned MemoryRegion object
     * 
     * @param pd ProtectionDomain object
     * @param addr address of the raw buffer
     * @param length length of the buffer
     */
    static Box<MemoryRegion> create(Arc<ProtectionDomain> pd, void* addr, uint64_t length) noexcept(false);

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

using Command = std::tuple<Ticket, Arc<std::atomic<bool>>>;

template<typename T>
using MultiMap = std::map<uint32_t, std::queue<T>>;

class Handle {
  private:
    Arc<std::atomic<bool>> finished_;

  public:
    Handle() : finished_(std::make_shared<std::atomic<bool>>(true)) {}

    Handle(const Arc<std::atomic<bool>>& finished) : finished_(finished) {}

    /**
     * @brief Check if the send/recv is finished.
     */
    inline bool is_finished() const {
        return this->finished_->load(std::memory_order_relaxed);
    }

    /**
     * @brief Wait until the operation is finished in a busy looping way.
     */
    inline void wait() const {
        while (!this->is_finished()) {
            std::this_thread::yield();
        }
    }
};

class TcclContext {
  private:
    uint64_t dop_;

    Arc<RcQueuePair> qp_;

    std::vector<ibv_wc> send_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_send_wcs_;

    std::vector<ibv_wc> recv_ibv_wc_buffer_;
    std::vector<WorkCompletion> polled_recv_wcs_;

    Queue<Command> send_request_command_queue_;
    Queue<Command> recv_request_command_queue_;

    Box<MemoryRegion> host_send_buffer_;
    uint64_t send_buffer_addr_;
    uint32_t send_buffer_lkey_;

    Box<MemoryRegion> host_recv_buffer_;
    uint64_t recv_buffer_addr_;
    uint32_t recv_buffer_lkey_;

    Queue<Ticket> local_recv_request_queue_;
    Queue<Ticket> remote_recv_request_queue_;

    // Used in send_one_round
    std::queue<Ticket> pending_local_recv_request_queue_;
    MultiMap<Ticket> pending_remote_recv_request_map_;
    MultiMap<Ticket> pending_local_send_request_map_;
    MultiMap<Arc<std::atomic<bool>>> pending_local_send_flag_map_;
    std::queue<uint64_t> free_post_send_send_slots_;
    uint64_t post_send_write_slot_available_;
    uint64_t post_send_send_slot_available_;

    // Used in recv_one_round
    uint64_t pending_recv_request_count_;
    MultiMap<Arc<std::atomic<bool>>> pending_local_recv_request_map_;

    // Background polling
    bool background_polling_;
    std::thread polling_thread_;
    std::atomic<bool> polling_stopped_;

    TcclContext(const TcclContext&) = delete;
    TcclContext& operator=(const TcclContext&) = delete;

    void poll_both_inner() noexcept(false);
    void poll_send_one_round_inner() noexcept(false);
    void poll_recv_one_round_inner() noexcept(false);

  public:
    ~TcclContext();

    inline uint64_t get_dop() const {
        return this->dop_;
    }

    /**
     * @brief Poll both send and recv tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_both() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_recv_one_round_inner();
        this->poll_send_one_round_inner();
    }

    /**
     * @brief Poll send tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_send_one_round() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_send_one_round_inner();
    }

    /**
     * @brief Poll recv tasks
     * SAFETY: !! This function is not thread-safe !!
     */
    inline void poll_recv_one_round() noexcept(false) {
        assert(this->background_polling_ == false);
        this->poll_recv_one_round_inner();
    }

    static Arc<TcclContext>
    create(Box<RcQueuePair> qp, bool spawn_polling_thread = true, uint64_t dop = 16) noexcept(false);
    [[nodiscard]] Handle send(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t lkey, uint32_t padding = 0);
    [[nodiscard]] Handle recv(uint32_t stream_id, uint64_t addr, uint32_t length, uint32_t rkey, uint32_t padding = 0);

  private:
    TcclContext() = default;
    void initialize(Box<RcQueuePair> qp, uint64_t dop) noexcept(false);
};

}  // namespace rdma_util

#endif  // _RDMA_UTIL_H_