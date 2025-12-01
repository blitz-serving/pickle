#pragma once

#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <cstdlib>
#include <string>

// 日志级别（constexpr 更安全）
constexpr int kLogLevelError = 0;
constexpr int kLogLevelWarn = 1;
constexpr int kLogLevelInfo = 2;
constexpr int kLogLevelDebug = 3;
constexpr int kLogLevelTrace = 4;

// 线程安全、延迟初始化、避免静态初始化顺序问题
inline int get_log_level() {
    static const int level = []() -> int {
        const char* env = std::getenv("PICKLE_LOG_LEVEL");
        if (!env)
            return kLogLevelInfo;

        std::string s(env);
        // 统一转为小写比较（可选，简化逻辑）
        for (auto& c : s)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));

        if (s == "trace")
            return kLogLevelTrace;
        if (s == "debug")
            return kLogLevelDebug;
        if (s == "warn")
            return kLogLevelWarn;
        if (s == "error")
            return kLogLevelError;
        // "info" 或其他默认
        return kLogLevelInfo;
    }();
    return level;
}

static inline auto _pickle_format_() -> decltype("") {
    return "";
}

template<typename T>
static inline auto _pickle_format_(T&& f) -> decltype(std::forward<T>(f)) {
    return std::forward<T>(f);
}

template<typename T, typename... Args>
static inline auto _pickle_format_(T&& f, Args&&... args)
    -> decltype(::fmt::vformat(std::forward<T>(f), ::fmt::make_format_args(args...))) {
    return ::fmt::vformat(std::forward<T>(f), ::fmt::make_format_args(args...));
}

#define TRACE(f, ...)                              \
    do {                                           \
        if (get_log_level() >= kLogLevelTrace) {   \
            ::fmt::println(                        \
                "{:%H:%M:%S} [TRACE]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    } while (0)

#define DEBUG(f, ...)                              \
    do {                                           \
        if (get_log_level() >= kLogLevelDebug) {   \
            ::fmt::println(                        \
                "{:%H:%M:%S} [DEBUG]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    } while (0)

#define INFO(f, ...)                               \
    do {                                           \
        if (get_log_level() >= kLogLevelInfo) {    \
            ::fmt::println(                        \
                "{:%H:%M:%S} [INFO ]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    } while (0)

#define WARN(f, ...)                               \
    do {                                           \
        if (get_log_level() >= kLogLevelWarn) {    \
            ::fmt::println(                        \
                "{:%H:%M:%S} [WARN ]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    } while (0)

#define ERROR(f, ...)                              \
    do {                                           \
        if (get_log_level() >= kLogLevelError) {   \
            ::fmt::println(                        \
                "{:%H:%M:%S} [ERROR]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    } while (0)

#define PICKLE_ASSERT(expr, ...)                                                                                \
    do {                                                                                                        \
        if (!(expr)) {                                                                                          \
            throw std::runtime_error(                                                                           \
                ::fmt::format("Assertion failed. {} ({}:{})", _pickle_format_(__VA_ARGS__), __FILE__, __LINE__) \
            );                                                                                                  \
        }                                                                                                       \
    } while (0)
