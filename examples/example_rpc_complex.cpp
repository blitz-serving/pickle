#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "cuda_util.h"
#include "pickle.h"
#include "pickle_logger.h"
#include "rdma_util.h"
#include "rpc_core.h"

constexpr const char* kClientRNIC = "mlx5_1";
constexpr const uint32_t kClientGPU = 0;

constexpr const char* kServerRNIC = "mlx5_2";
constexpr const uint32_t kServerGPU = 2;

constexpr const uint32_t kDataBufferSize = 1ull * 1024 * 1024 * 1024;

enum class rpc_type_t {
    RPC_TYPE_DEFAULT = 0,
    RPC_TYPE_HANDSHAKE = 1,
    RPC_TYPE_SEND_RECV = 2,
    RPC_TYPE_ERROR = 4,
    RPC_TYPE_SUCCESS = 5,
};

struct rpc_data_t {
    rpc_type_t type;

    union {
        rdma_util::HandshakeData handshake_data;
        uint64_t send_recv_size;
    } data;

    rpc_data_t() = default;

    std::vector<char> into_bytes() const {
        std::vector<char> buffer(sizeof(rpc_data_t));
        std::memcpy(buffer.data(), this, sizeof(rpc_data_t));
        return buffer;
    }

    static rpc_data_t from_bytes(const std::vector<char>& buffer) {
        rpc_data_t rpc_data;
        assert(buffer.size() == sizeof(rpc_data_t));
        std::memcpy(&rpc_data, buffer.data(), sizeof(rpc_data_t));
        return rpc_data;
    }
};

class rpc_handle_t: public rpc_core::RpcHandle {
private:
    std::shared_ptr<rdma_util::MemoryRegion> mr_;
    std::shared_ptr<pickle::Flusher> flusher_;
    std::shared_ptr<std::atomic_bool> stop_flag_;
    std::thread flusher_thread_;

    mutable std::mutex qp_mutex_;
    mutable std::unique_ptr<rdma_util::RcQueuePair> qp_;

public:
    rpc_handle_t() = delete;
    rpc_handle_t(const rpc_handle_t&) = delete;
    rpc_handle_t& operator=(const rpc_handle_t&) = delete;
    rpc_handle_t(rpc_handle_t&&) = delete;
    rpc_handle_t& operator=(rpc_handle_t&&) = delete;

    rpc_handle_t(std::shared_ptr<rdma_util::MemoryRegion> mr, std::unique_ptr<pickle::Flusher> flusher) :
        mr_(std::move(mr)),
        flusher_(std::move(flusher)),
        stop_flag_(std::make_shared<std::atomic_bool>(false)) {
        flusher_thread_ = std::thread([flusher = this->flusher_, stop_flag = this->stop_flag_]() {
            while (!stop_flag->load(std::memory_order_relaxed)) {
                flusher->poll();
                std::this_thread::yield();
            }
        });
    }

    ~rpc_handle_t() override {
        stop_flag_->store(true, std::memory_order_relaxed);
        if (flusher_thread_.joinable()) {
            flusher_thread_.join();
        }
        INFO("rpc_handle_t stopped");
    }

    std::vector<char> handle(std::vector<char>&& data) const override {
        try {
            auto request = rpc_data_t::from_bytes(data);
            if (request.type == rpc_type_t::RPC_TYPE_HANDSHAKE) {
                INFO("Handling handshake request");
                auto qp = rdma_util::RcQueuePair::create(this->mr_->get_pd());
                qp->bring_up(request.data.handshake_data, 3);
                auto handshake_data = qp->get_handshake_data(3);
                {
                    std::lock_guard<std::mutex> lock(this->qp_mutex_);
                    if (this->qp_ != nullptr) {
                        ERROR("Queue pair already exists");
                        return rpc_data_t {rpc_type_t::RPC_TYPE_ERROR, {}}.into_bytes();
                    }
                    this->qp_ = std::move(qp);
                }
                return rpc_data_t {rpc_type_t::RPC_TYPE_HANDSHAKE, {.handshake_data = handshake_data}}.into_bytes();
            } else if (request.type == rpc_type_t::RPC_TYPE_SEND_RECV) {
                INFO("Handling send/recv request");
                auto size = request.data.send_recv_size;
                if (size > this->mr_->get_length()) {
                    ERROR("Requested size exceeds buffer size");
                    return rpc_data_t {rpc_type_t::RPC_TYPE_ERROR, {}}.into_bytes();
                }
                std::shared_ptr<pickle::PickleRecver> recver {nullptr};
                {
                    std::lock_guard<std::mutex> lock(this->qp_mutex_);
                    recver = pickle::PickleRecver::create(std::move(this->qp_), this->flusher_);
                }
                auto handle =
                    recver->recv(0, reinterpret_cast<uint64_t>(this->mr_->get_addr()), size, this->mr_->get_lkey());
                std::thread([handle, recver]() {
                    while (!handle.is_finished()) {
                        recver->poll();
                        std::this_thread::yield();
                    }
                }).detach();
                return rpc_data_t {rpc_type_t::RPC_TYPE_SUCCESS, {.send_recv_size = size}}.into_bytes();
            } else {
                ERROR("Unknown RPC request type");
                return rpc_data_t {rpc_type_t::RPC_TYPE_ERROR, {}}.into_bytes();
            }
        } catch (const std::exception& e) {
            ERROR("Error handling RPC request: {}", e.what());
            return rpc_data_t {rpc_type_t::RPC_TYPE_ERROR, {}}.into_bytes();
        }
    }
};

void client(const char* ip, int port) {
    // Prepare the RDMA handshake data
    auto qp = rdma_util::RcQueuePair::create(kClientRNIC);
    auto mr = rdma_util::MemoryRegion::create(
        qp->get_pd(),
        std::shared_ptr<void>(
            cuda_util::malloc_gpu_buffer(kDataBufferSize, kClientGPU),
            [](void* p) { cuda_util::free_gpu_buffer(p); }
        ),
        kDataBufferSize
    );

    // Call rpc to get remote handshake data
    auto response = rpc_data_t::from_bytes(rpc_core::rpc_call(
        ip,
        port,
        rpc_data_t {rpc_type_t::RPC_TYPE_HANDSHAKE, {.handshake_data = qp->get_handshake_data(3)}}.into_bytes()
    ));
    assert(response.type == rpc_type_t::RPC_TYPE_HANDSHAKE);

    // Bring up the connection
    qp->bring_up(response.data.handshake_data, 3);
    // The sender creation must be done after the handshake is completed and before the sending rpc
    auto sender = pickle::PickleSender::create(std::move(qp));
    // Send
    auto handle = sender->send(0, reinterpret_cast<uint64_t>(mr->get_addr()), kDataBufferSize, mr->get_lkey());
    response = rpc_data_t::from_bytes(rpc_core::rpc_call(
        "127.0.0.1",
        port,
        rpc_data_t {rpc_type_t::RPC_TYPE_SEND_RECV, {.send_recv_size = kDataBufferSize}}.into_bytes()
    ));
    assert(response.type == rpc_type_t::RPC_TYPE_SUCCESS);
    while (!handle.is_finished()) {
        sender->poll();
    }
}

int main() {
    auto mr = rdma_util::MemoryRegion::create(
        rdma_util::ProtectionDomain::create(rdma_util::Context::create(kServerRNIC)),
        std::shared_ptr<void>(
            cuda_util::malloc_gpu_buffer(kDataBufferSize, kServerGPU),
            [](void* p) { cuda_util::free_gpu_buffer(p); }
        ),
        kDataBufferSize
    );
    auto flusher = pickle::Flusher::create(mr->get_pd());
    auto rpc_handle = std::make_shared<const rpc_handle_t>(std::move(mr), std::move(flusher));
    auto server = rpc_core::RpcServer("0.0.0.0", 12345, rpc_handle);
    server.start();
    client("127.0.0.1", 12345);
    client("127.0.0.1", 12345);
    return 0;
}
