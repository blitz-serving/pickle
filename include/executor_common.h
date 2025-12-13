#pragma once

#include <x86intrin.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <queue>

#include "concurrentqueue.h"

namespace pickle {

// Shared constant controlling batch sizes across executors.
static const uint64_t kMagic = 32;

template<typename T>
using Queue = moodycamel::ConcurrentQueue<T>;

template<typename T>
using MultiMap = std::map<uint32_t, std::queue<T>>;

class Pollable {
public:
    virtual void poll() noexcept(false) = 0;
    virtual ~Pollable() = default;
};

class Event {
private:
    std::atomic<bool> finished_ = false;

public:
    Event() = default;
    Event(const Event&) = delete;
    Event& operator=(const Event&) = delete;
    Event(Event&&) = delete;
    Event& operator=(Event&&) = delete;
    ~Event() = default;

    static std::shared_ptr<Event> create() {
        return std::make_shared<Event>();
    }

    bool is_notified() const {
        return this->finished_.load(std::memory_order_acquire);
    }

    void notify() {
        this->finished_.store(true, std::memory_order_release);
#ifndef BUSY_WAIT
        this->finished_.notify_all();
#endif
    }

    void wait() {
#ifndef BUSY_WAIT
        this->finished_.wait(false, std::memory_order_acquire);
#else
        while (!this->finished_.load(std::memory_order_acquire)) {
            _mm_pause();
        }
#endif
    }
};

}  // namespace pickle
