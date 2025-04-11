#include <thread>

#include "rpc_core.h"

struct rpc_request_t {
    int64_t a;
    int64_t b;

    rpc_request_t() = default;

    std::vector<char> to_bytes() const {
        std::vector<char> data(sizeof(rpc_request_t));
        std::memcpy(data.data(), this, sizeof(rpc_request_t));
        return data;
    }

    static rpc_request_t from_bytes(const std::vector<char>& data) {
        rpc_request_t request;
        std::memcpy(&request, data.data(), sizeof(rpc_request_t));
        return request;
    }
};

struct rpc_response_t {
    int64_t result;

    rpc_response_t() = default;

    std::vector<char> to_bytes() const {
        std::vector<char> data(sizeof(rpc_response_t));
        std::memcpy(data.data(), this, sizeof(rpc_response_t));
        return data;
    }

    static rpc_response_t from_bytes(const std::vector<char>& data) {
        rpc_response_t response;
        std::memcpy(&response, data.data(), sizeof(rpc_response_t));
        return response;
    }
};

class rpc_handle_t: public rpc_core::RpcHandle {
public:
    std::vector<char> handle(std::vector<char>&& data) const override {
        auto request = rpc_request_t::from_bytes(data);
        INFO("Received request: a={}, b={}", request.a, request.b);
        rpc_response_t response;
        response.result = request.a + request.b;
        INFO("Sending response: result={}", response.result);
        return response.to_bytes();
    }
};

int main() {
    auto rpc = rpc_core::RpcServer("0.0.0.0", 8080, std::make_shared<rpc_handle_t>());
    rpc.start();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto response = rpc_response_t::from_bytes(rpc_core::rpc_call("127.0.0.1", 8080, rpc_request_t {1, 2}.to_bytes()));
    INFO("Received response: result={}", response.result);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    return 0;
}