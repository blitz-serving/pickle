#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

#define PORT 8080

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;
    char* hello = "Hello from client";
    char buffer[1024] = {0};

    // 创建 socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cout << "Socket creation error" << std::endl;
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // 将地址转换为二进制格式
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        std::cout << "Invalid address/ Address not supported" << std::endl;
        return -1;
    }

    // 连接到服务器
    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cout << "Connection Failed" << std::endl;
        return -1;
    }

    // 发送数据到服务器
    send(sock, hello, strlen(hello), 0);
    std::cout << "Hello message sent" << std::endl;

    // 接收服务器返回的数据
    int valread = read(sock, buffer, 1024);
    std::cout << "Echo from server: " << buffer << std::endl;

    close(sock);
    return 0;
}
