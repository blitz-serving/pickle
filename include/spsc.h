#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>

#include "result.h"

namespace ipc {

enum class Error {
    ShmOpenFailed,
    FtruncateFailed,
    MmapFailed,
    FstatFailed,
    NotInitialized,  // magic / header 无效
    RoleAlreadyAttached,
    ElementSizeMismatch,
    NotEnoughSpace,  // shm 太小 / 对齐失败 / capacity 太小
    InvalidCapacity,
    Timeout,
    QueueFull,
    QueueEmpty,
};

inline std::ostream& operator<<(std::ostream& os, const Error& err) {
    switch (err) {
        case Error::ShmOpenFailed:
            os << "ShmOpenFailed";
            break;
        case Error::FtruncateFailed:
            os << "FtruncateFailed";
            break;
        case Error::MmapFailed:
            os << "MmapFailed";
            break;
        case Error::FstatFailed:
            os << "FstatFailed";
            break;
        case Error::NotInitialized:
            os << "NotInitialized";
            break;
        case Error::RoleAlreadyAttached:
            os << "RoleAlreadyAttached";
            break;
        case Error::ElementSizeMismatch:
            os << "ElementSizeMismatch";
            break;
        case Error::NotEnoughSpace:
            os << "NotEnoughSpace";
            break;
        case Error::InvalidCapacity:
            os << "InvalidCapacity";
            break;
        case Error::Timeout:
            os << "Timeout";
            break;
        case Error::QueueFull:
            os << "QueueFull";
            break;
        case Error::QueueEmpty:
            os << "QueueEmpty";
            break;
    }
    return os;
}

template<typename T>
    requires std::is_trivially_copyable_v<T>
class SPSCQueue {
public:
    enum class Role {
        Producer,
        Consumer,
    };

private:
    // 整个共享内存的 header，负责跨进程生命周期管理 + 角色管理
    struct alignas(64) SharedMemoryHeader {
        std::atomic<std::uint32_t> ref_count;  // 总引用计数
        std::atomic<std::uint32_t> magic;  // 魔数校验
        std::atomic<std::size_t> user_size;  // header 之后可用的数据区域大小

        std::atomic<std::uint32_t> producer_attached;  // 0 or 1
        std::atomic<std::uint32_t> consumer_attached;  // 0 or 1
    };

    // 队列本身的 header，位于 SharedMemoryHeader 之后
    struct alignas(64) QueueHeader {
        std::atomic<std::size_t> head;  // consumer 写
        std::atomic<std::size_t> tail;  // producer 写
        std::size_t capacity;  // 环形缓冲区槽位数
        std::size_t element_size;  // 元素大小（用于类型校验）
    };

    static constexpr std::uint32_t kMagic = 0x53505343;  // 'SPSC'

public:
    using value_type = T;

    SPSCQueue() = default;

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;

    SPSCQueue(SPSCQueue&& other) noexcept :
        fd_(other.fd_),
        shm_header_(other.shm_header_),
        header_(other.header_),
        data_(other.data_),
        total_size_(other.total_size_),
        name_(std::move(other.name_)),
        role_(other.role_) {
        other.fd_ = -1;
        other.shm_header_ = nullptr;
        other.header_ = nullptr;
        other.data_ = nullptr;
        other.total_size_ = 0;
        other.name_.clear();
        other.role_.reset();
    }

    SPSCQueue& operator=(SPSCQueue&& other) noexcept {
        if (this != &other) {
            release();

            fd_ = other.fd_;
            shm_header_ = other.shm_header_;
            header_ = other.header_;
            data_ = other.data_;
            total_size_ = other.total_size_;
            name_ = std::move(other.name_);
            role_ = other.role_;

            other.fd_ = -1;
            other.shm_header_ = nullptr;
            other.header_ = nullptr;
            other.data_ = nullptr;
            other.total_size_ = 0;
            other.name_.clear();
            other.role_.reset();
        }
        return *this;
    }

    ~SPSCQueue() {
        release();
    }

    // ==================== 共享内存工厂接口 ====================

    // 创建新的共享内存，并在其中构造 SPSCQueue。
    // size_bytes 参数为「队列数据区」大小（不包含 SharedMemoryHeader），
    // 内部会自动在前面预留 SharedMemoryHeader 空间。
    static result::Result<SPSCQueue, Error>
    create(const std::string& name, std::size_t size_bytes, Role role) noexcept {
        const std::size_t total_size = sizeof(SharedMemoryHeader) + size_bytes;

        int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd == -1) {
            return result::Err(Error::ShmOpenFailed);
        }

        if (::ftruncate(fd, static_cast<off_t>(total_size)) == -1) {
            ::close(fd);
            ::shm_unlink(name.c_str());
            return result::Err(Error::FtruncateFailed);
        }

        void* ptr = ::mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            ::close(fd);
            ::shm_unlink(name.c_str());
            return result::Err(Error::MmapFailed);
        }

        auto* shm_header = static_cast<SharedMemoryHeader*>(ptr);
        auto* user_data = reinterpret_cast<std::byte*>(ptr) + sizeof(SharedMemoryHeader);

        // 初始化 header（这里没有并发，直接写即可）
        shm_header->ref_count.store(1, std::memory_order_release);
        shm_header->user_size.store(size_bytes, std::memory_order_release);
        shm_header->magic.store(kMagic, std::memory_order_release);
        shm_header->producer_attached.store(role == Role::Producer ? 1u : 0u, std::memory_order_release);
        shm_header->consumer_attached.store(role == Role::Consumer ? 1u : 0u, std::memory_order_release);

        SPSCQueue q(fd, shm_header, total_size, name, role);

        TRY(q.init_layout(user_data, size_bytes, /*initialize=*/true));
        return result::Ok(std::move(q));
    }

    // 打开已有共享内存，并以给定角色附着到队列
    static result::Result<SPSCQueue, Error> open(const std::string& name, Role role) noexcept {
        int fd = ::shm_open(name.c_str(), O_RDWR, 0600);
        if (fd == -1) {
            return result::Err(Error::ShmOpenFailed);
        }

        struct ::stat st {};
        if (::fstat(fd, &st) == -1) {
            ::close(fd);
            return result::Err(Error::FstatFailed);
        }
        std::size_t total_size = static_cast<std::size_t>(st.st_size);
        if (total_size < sizeof(SharedMemoryHeader)) {
            ::close(fd);
            return result::Err(Error::NotEnoughSpace);
        }

        void* ptr = ::mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            ::close(fd);
            return result::Err(Error::MmapFailed);
        }

        auto* shm_header = static_cast<SharedMemoryHeader*>(ptr);

        auto magic = shm_header->magic.load(std::memory_order_acquire);
        if (magic != kMagic) {
            ::munmap(ptr, total_size);
            ::close(fd);
            return result::Err(Error::NotInitialized);
        }

        // 尝试占用角色
        auto role_res = acquire_role(shm_header, role);
        if (!role_res.is_ok()) {
            ::munmap(ptr, total_size);
            ::close(fd);
            return result::Err(role_res.unwrap_err());
        }

        // 引用计数 +1
        shm_header->ref_count.fetch_add(1, std::memory_order_acq_rel);

        std::size_t user_size = shm_header->user_size.load(std::memory_order_acquire);
        auto* user_data = reinterpret_cast<std::byte*>(ptr) + sizeof(SharedMemoryHeader);

        SPSCQueue q(fd, shm_header, total_size, name, role);
        TRY(q.init_layout(user_data, user_size, /*initialize=*/false));
        return result::Ok(std::move(q));
    }

    // 等待共享内存创建并初始化完成后再打开（按角色附着）
    static result::Result<SPSCQueue, Error> wait(
        const std::string& name,
        Role role,
        std::chrono::milliseconds timeout,
        std::chrono::milliseconds poll_interval = std::chrono::milliseconds(10)
    ) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (true) {
            int fd = ::shm_open(name.c_str(), O_RDWR, 0600);
            if (fd == -1) {
                if (errno != ENOENT) {
                    return result::Err(Error::ShmOpenFailed);
                }
                // 还没创建好，等一会儿重试
            } else {
                struct ::stat st {};
                if (::fstat(fd, &st) == -1) {
                    ::close(fd);
                    return result::Err(Error::FstatFailed);
                }
                std::size_t total_size = static_cast<std::size_t>(st.st_size);
                if (total_size < sizeof(SharedMemoryHeader)) {
                    ::close(fd);
                    return result::Err(Error::NotEnoughSpace);
                }

                void* ptr = ::mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (ptr == MAP_FAILED) {
                    ::close(fd);
                    return result::Err(Error::MmapFailed);
                }

                auto* shm_header = static_cast<SharedMemoryHeader*>(ptr);
                auto magic = shm_header->magic.load(std::memory_order_acquire);

                if (magic != kMagic) {
                    // 创建方还没初始化好，直接视为未就绪
                    ::munmap(ptr, total_size);
                    ::close(fd);
                } else {
                    // 尝试占用角色
                    auto role_res = acquire_role(shm_header, role);
                    if (!role_res.is_ok()) {
                        ::munmap(ptr, total_size);
                        ::close(fd);
                        return result::Err(role_res.unwrap_err());
                    }

                    // 引用计数 +1
                    shm_header->ref_count.fetch_add(1, std::memory_order_acq_rel);

                    std::size_t user_size = shm_header->user_size.load(std::memory_order_acquire);
                    auto* user_data = reinterpret_cast<std::byte*>(ptr) + sizeof(SharedMemoryHeader);

                    SPSCQueue q(fd, shm_header, total_size, name, role);
                    TRY(q.init_layout(user_data, user_size, /*initialize=*/false));
                    return result::Ok(std::move(q));
                }
            }

            if (std::chrono::steady_clock::now() >= deadline) {
                return result::Err(Error::Timeout);
            }

            std::this_thread::sleep_for(poll_interval);
        }
    }

    // 一些简单的便捷封装

    static result::Result<SPSCQueue, Error> create_producer(const std::string& name, std::size_t size_bytes) noexcept {
        return create(name, size_bytes, Role::Producer);
    }

    static result::Result<SPSCQueue, Error> create_consumer(const std::string& name, std::size_t size_bytes) noexcept {
        return create(name, size_bytes, Role::Consumer);
    }

    static result::Result<SPSCQueue, Error> open_producer(const std::string& name) noexcept {
        return open(name, Role::Producer);
    }

    static result::Result<SPSCQueue, Error> open_consumer(const std::string& name) noexcept {
        return open(name, Role::Consumer);
    }

    static result::Result<SPSCQueue, Error> wait_producer(
        const std::string& name,
        std::chrono::milliseconds timeout,
        std::chrono::milliseconds poll_interval = std::chrono::milliseconds(10)
    ) noexcept {
        return wait(name, Role::Producer, timeout, poll_interval);
    }

    static result::Result<SPSCQueue, Error> wait_consumer(
        const std::string& name,
        std::chrono::milliseconds timeout,
        std::chrono::milliseconds poll_interval = std::chrono::milliseconds(10)
    ) noexcept {
        return wait(name, Role::Consumer, timeout, poll_interval);
    }

    // ==================== 队列接口 ====================

    std::size_t capacity() const noexcept {
        // 实际可放元素数，环形缓冲使用一格作为哨兵避免额外 size 计数
        return header_->capacity - 1;
    }

    result::Result<void, Error> try_push(const T& value) noexcept {
        auto tail = header_->tail.load(std::memory_order_relaxed);
        auto head = header_->head.load(std::memory_order_acquire);

        const auto next_tail = increment(tail);
        if (next_tail == head) {
            return result::Err(Error::QueueFull);
        }

        data_[tail] = value;
        header_->tail.store(next_tail, std::memory_order_release);
        return result::Ok();
    }

    result::Result<T, Error> try_pop() noexcept {
        auto head = header_->head.load(std::memory_order_relaxed);
        auto tail = header_->tail.load(std::memory_order_acquire);

        if (head == tail) {
            return result::Err(Error::QueueEmpty);
        }

        T value = data_[head];
        auto next_head = increment(head);
        header_->head.store(next_head, std::memory_order_release);
        return result::Ok(value);
    }

private:
    int fd_ {-1};
    SharedMemoryHeader* shm_header_ {nullptr};  // mmap 的起始地址
    QueueHeader* header_ {nullptr};
    T* data_ {nullptr};
    std::size_t total_size_ {0};  // 整个 shm 的大小（包含 SharedMemoryHeader + 队列区域）
    std::string name_;
    std::optional<Role> role_;  // 当前实例持有的角色（P or C）

    SPSCQueue(int fd, SharedMemoryHeader* shm_header, std::size_t total_size, std::string name, Role role) noexcept :
        fd_(fd),
        shm_header_(shm_header),
        header_(nullptr),
        data_(nullptr),
        total_size_(total_size),
        name_(std::move(name)),
        role_(role) {}

    std::size_t increment(std::size_t idx) const noexcept {
        const auto cap = header_->capacity;
        ++idx;
        if (idx == cap) {
            idx = 0;
        }
        return idx;
    }

    // 在 [user_data, user_data + user_size) 区域中布局 QueueHeader 和元素数组
    result::Result<void, Error> init_layout(void* user_data, std::size_t user_size, bool initialize) noexcept {
        // [QueueHeader][padding][T array ...]

        auto* raw = static_cast<std::byte*>(user_data);
        std::size_t total = user_size;

        if (total < sizeof(QueueHeader)) {
            return result::Err(Error::NotEnoughSpace);
        }

        header_ = reinterpret_cast<QueueHeader*>(raw);

        // data 区域起点
        void* data_ptr = raw + sizeof(QueueHeader);
        std::size_t data_space = total - sizeof(QueueHeader);

        // 对齐到 alignof(T)
        void* aligned_ptr = std::align(alignof(T), sizeof(T), data_ptr, data_space);
        if (!aligned_ptr) {
            return result::Err(Error::NotEnoughSpace);
        }

        data_ = static_cast<T*>(aligned_ptr);
        std::size_t capacity = data_space / sizeof(T);

        // 环形缓冲需要至少 2 个槽位才能区分空/满
        if (capacity < 2) {
            return result::Err(Error::NotEnoughSpace);
        }

        if (initialize) {
            new (&header_->head) std::atomic<std::size_t>(0);
            new (&header_->tail) std::atomic<std::size_t>(0);
            header_->capacity = capacity;
            header_->element_size = sizeof(T);
        } else {
            if (header_->element_size != sizeof(T)) {
                return result::Err(Error::ElementSizeMismatch);
            }
            if (header_->capacity < 2 || header_->capacity > capacity) {
                return result::Err(Error::InvalidCapacity);
            }
        }

        return result::Ok();  // Ok<void>
    }

    static result::Result<void, Error> acquire_role(SharedMemoryHeader* shm_header, Role role) noexcept {
        if (role == Role::Producer) {
            std::uint32_t expected = 0;
            if (!shm_header->producer_attached
                     .compare_exchange_strong(expected, 1u, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return result::Err(Error::RoleAlreadyAttached);
            }
        } else {
            std::uint32_t expected = 0;
            if (!shm_header->consumer_attached
                     .compare_exchange_strong(expected, 1u, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return result::Err(Error::RoleAlreadyAttached);
            }
        }
        return result::Ok();
    }

    void release_role() noexcept {
        if (!shm_header_ || !role_.has_value()) {
            return;
        }
        if (*role_ == Role::Producer) {
            shm_header_->producer_attached.store(0u, std::memory_order_release);
        } else {
            shm_header_->consumer_attached.store(0u, std::memory_order_release);
        }
        role_.reset();
    }

    void release() noexcept {
        if (!shm_header_) {
            return;
        }

        // 先释放角色，再减引用计数
        release_role();

        std::uint32_t old = shm_header_->ref_count.fetch_sub(1, std::memory_order_acq_rel);
        bool last = (old == 1);

        ::munmap(static_cast<void*>(shm_header_), total_size_);
        if (fd_ >= 0) {
            ::close(fd_);
        }
        if (last && !name_.empty()) {
            ::shm_unlink(name_.c_str());
        }

        shm_header_ = nullptr;
        header_ = nullptr;
        data_ = nullptr;
        total_size_ = 0;
        fd_ = -1;
        name_.clear();
    }
};

}  // namespace ipc
