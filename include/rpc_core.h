#ifndef _RPC_CORE_H_
#define _RPC_CORE_H_

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <thread>

#include "pickle_logger.h"

namespace rpc_core {

class RpcHandle {
public:
    virtual ~RpcHandle() = default;

    virtual std::vector<char> handle(std::vector<char>&&) const = 0;
};

/// This function will never close the client_fd even on error.
/// It is the caller's responsibility to close the client_fd.
inline std::vector<char> recv_data(int fd) noexcept(false) {
    size_t data_size;
    size_t total_received = 0;
    char* size_buffer = reinterpret_cast<char*>(&data_size);

    while (total_received < sizeof(size_t)) {
        ssize_t bytes = recv(fd, size_buffer + total_received, sizeof(size_t) - total_received, 0);
        DEBUG("Receive data: {} bytes", bytes);
        if (bytes < 0) {
            ERROR("Receive size failed");
            throw std::runtime_error("Receive size failed");
        }
        total_received += bytes;
    }

    std::vector<char> data(data_size);
    total_received = 0;
    while (total_received < data_size) {
        ssize_t bytes = recv(fd, data.data() + total_received, data_size - total_received, 0);
        DEBUG("Receive data: {} bytes", bytes);
        if (bytes <= 0) {
            ERROR("Receive data failed");
            throw std::runtime_error("Receive data failed");
        }
        total_received += bytes;
    }
    return data;
}

inline void send_data(int fd, std::vector<char>&& data) noexcept(false) {
    size_t data_size = data.size();
    size_t total_sent = 0;
    const char* size_buffer = reinterpret_cast<const char*>(&data_size);

    while (total_sent < sizeof(size_t)) {
        ssize_t bytes = send(fd, size_buffer + total_sent, sizeof(size_t) - total_sent, 0);
        DEBUG("Send size: {} bytes", bytes);
        if (bytes <= 0) {
            ERROR("Send size failed");
            throw std::runtime_error("Send size failed");
        }
        total_sent += bytes;
    }

    total_sent = 0;
    while (total_sent < data_size) {
        ssize_t bytes = send(fd, data.data() + total_sent, data_size - total_sent, 0);
        DEBUG("Send data: {} bytes", bytes);
        if (bytes <= 0) {
            ERROR("Send data failed");
            throw std::runtime_error("Send data failed");
        }
        total_sent += bytes;
    }
}

std::vector<char> inline rpc_call(const std::string& ip, int port, std::vector<char>&& request) noexcept(false) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("Socket creation failed");
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        throw std::runtime_error("Invalid address");
    }

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        throw std::runtime_error("Connection failed");
    }

    send_data(fd, std::move(request));
    auto response = recv_data(fd);
    close(fd);
    return response;
}

class RpcServer {
private:
    int server_fd;
    std::string ip;
    int port;
    std::atomic_bool stop_flag;
    std::shared_ptr<RpcHandle> rpc_handle;

    std::thread server_thread;

    inline void accept_connections() noexcept(false) {
        while (stop_flag.load(std::memory_order_relaxed) == false) {
            sockaddr_in client_addr {};
            socklen_t len = sizeof(client_addr);
            // server_fd is non-blocking, so we can use accept() without blocking
            int client_fd = accept(server_fd, (sockaddr*)&client_addr, &len);
            if (client_fd >= 0) {
                std::thread([client_fd, rpc_handle = this->rpc_handle]() {
                    try {
                        DEBUG("Client fd: {}", client_fd);
                        send_data(client_fd, rpc_handle->handle(recv_data(client_fd)));
                    } catch (const std::exception& e) {
                        ERROR("Error handling client: {}", e.what());
                    }
                    close(client_fd);
                }).detach();
            } else if (errno == EWOULDBLOCK || errno == EAGAIN) {
                // No incoming connections, continue to check
                std::this_thread::yield();
            } else {
                ERROR("Accept failed");
                throw std::runtime_error("Accept failed");
            }
        }
    }

public:
    RpcServer(const std::string& ip, int port, std::shared_ptr<RpcHandle> handle) :
        server_fd(-1),
        ip(ip),
        port(port),
        stop_flag(true),
        rpc_handle(std::move(handle)) {}

    inline void start() noexcept(false) {
        stop_flag.store(false, std::memory_order_relaxed);

        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            ERROR("Socket creation failed");
            throw std::runtime_error("Socket creation failed");
        }

        sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        if (bind(server_fd, (sockaddr*)&addr, sizeof(addr))) {
            close(server_fd);
            server_fd = -1;
            ERROR("Bind failed");
            throw std::runtime_error("Bind failed");
        }

        if (listen(server_fd, 5) < 0) {
            close(server_fd);
            server_fd = -1;
            ERROR("Listen failed");
            throw std::runtime_error("Listen failed");
        }

        if (fcntl(server_fd, F_SETFL, O_NONBLOCK) < 0) {
            close(server_fd);
            server_fd = -1;
            ERROR("Set non-blocking failed");
            throw std::runtime_error("Set non-blocking failed");
        }

        server_thread = std::thread([this]() {
            try {
                accept_connections();
            } catch (const std::exception& e) {
                ERROR("Error in server thread: {}", e.what());
            }
        });

        INFO("Server listening on {}:{}", ip, port);
    }

    inline void stop() {
        stop_flag.store(true, std::memory_order_relaxed);
        if (server_thread.joinable()) {
            server_thread.join();
        }
        if (server_fd >= 0) {
            close(server_fd);
            server_fd = -1;
        }
        DEBUG("Server stopped");
    }

    ~RpcServer() {
        stop();
    }
};

}  // namespace rpc_core

#endif