#include <cassert>
#include <memory>
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
constexpr const ibv_rate kRate = ibv_rate::IBV_RATE_MAX;

enum class rpc_type_t {
    RPC_TYPE_DEFAULT = 0,
    RPC_TYPE_HANDSHAKE = 1,
    RPC_TYPE_SEND_RECV = 2,
    RPC_TYPE_ERROR = 42,
};

struct rpc_data_t {
    rpc_type_t type;

    union {
        rdma_util::HandshakeData handshake_data;

        struct {
            uint64_t address;
            uint64_t size;
        } send_recv_data;
    } data;

    rpc_data_t() = default;

    std::vector<char> to_bytes() const {
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

    // FIXME:
    // Here we use a mutable pointer to RcQueuePair, which is not thread-safe.
    // Some concurrency control is needed!
    mutable std::unique_ptr<rdma_util::RcQueuePair> qp_;

public:
    rpc_handle_t(std::shared_ptr<rdma_util::MemoryRegion> mr, std::shared_ptr<pickle::Flusher> flusher) :
        mr_(std::move(mr)),
        flusher_(std::move(flusher)) {}

    std::vector<char> handle(std::vector<char>&& data) const override {
        auto request = rpc_data_t::from_bytes(data);
        if (request.type == rpc_type_t::RPC_TYPE_HANDSHAKE) {
            INFO("Handling handshake request");
            auto qp = rdma_util::RcQueuePair::create(this->mr_->get_pd());
            qp->bring_up(request.data.handshake_data, 3, kRate);
            this->qp_ = std::move(qp);
            return rpc_data_t {
                rpc_type_t::RPC_TYPE_HANDSHAKE,
                {.handshake_data = this->qp_->get_handshake_data(3)}
            }.to_bytes();
        } else if (request.type == rpc_type_t::RPC_TYPE_SEND_RECV) {
            INFO("Handling send/recv request");
            auto size = request.data.send_recv_data.size;
            auto recver = pickle::PickleRecver::create(std::move(this->qp_), this->flusher_);
            auto handle =
                recver->recv(0, reinterpret_cast<uint64_t>(this->mr_->get_addr()), size, this->mr_->get_lkey());

            std::thread([recver = std::move(recver), handle = std::move(handle)]() {
                do {
                    recver->poll();
                    std::this_thread::yield();
                } while (!handle.is_finished());
                INFO("Recv finished");
            }).detach();

            return rpc_data_t {
                rpc_type_t::RPC_TYPE_SEND_RECV,
                {.send_recv_data = {reinterpret_cast<uint64_t>(this->mr_->get_addr()), size}}
            }.to_bytes();
        } else {
            ERROR("Unknown RPC request type");
            return rpc_data_t {rpc_type_t::RPC_TYPE_ERROR, {}}.to_bytes();
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
        rpc_data_t {rpc_type_t::RPC_TYPE_HANDSHAKE, {.handshake_data = qp->get_handshake_data(3)}}.to_bytes()
    ));
    assert(response.type == rpc_type_t::RPC_TYPE_HANDSHAKE);

    // Bring up the connection
    qp->bring_up(response.data.handshake_data, 3, kRate);
    // The sender creation must be done after the handshake is completed and before the sending rpc
    auto sender = pickle::PickleSender::create(std::move(qp));

    rpc_core::rpc_call(
        "127.0.0.1",
        8080,
        rpc_data_t {
            rpc_type_t::RPC_TYPE_SEND_RECV,
            {.send_recv_data = {reinterpret_cast<uint64_t>(mr->get_addr()), kDataBufferSize}}
        }.to_bytes()
    );

    // Send
    auto handle = sender->send(0, reinterpret_cast<uint64_t>(mr->get_addr()), kDataBufferSize, mr->get_lkey());
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
    auto rpc_handle = std::make_shared<rpc_handle_t>(std::move(mr), std::move(flusher));
    auto server = rpc_core::RpcServer("0.0.0.0", 8080, rpc_handle);
    server.start();
    client("127.0.0.1", 8080);
    client("127.0.0.1", 8080);
    return 0;
}
