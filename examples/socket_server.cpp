#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#define PORT 8080

void handle_client(int client_socket) {
    char buffer[1024] = {0};
    int valread = read(client_socket, buffer, 1024);
    if (valread > 0) {
        std::cout << "Received: " << buffer << std::endl;
        send(client_socket, buffer, strlen(buffer), 0);
        std::cout << "Echo message sent back to client" << std::endl;
    }
    close(client_socket);
}

int main() {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    std::vector<std::thread> threads;

    // 创建 socket 文件描述符
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 附加 socket 到端口
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // 绑定 socket 到端口
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 监听端口
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    std::cout << "Server is listening on port " << PORT << std::endl;

    // 接受客户端连接
    while (true) {
        int new_socket;
        if ((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }
        std::cout << "New connection accepted" << std::endl;

        // 为每个客户端连接创建一个新线程
        threads.emplace_back(std::thread(handle_client, new_socket));
    }

    // 等待所有线程完成
    for (auto& th : threads) {
        if (th.joinable()) {
            th.join();
        }
    }

    close(server_fd);
    return 0;
}
