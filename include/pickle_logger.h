#ifndef _LOGGER_H_
#define _LOGGER_H_

#include <fmt/chrono.h>
#include <fmt/core.h>

#include <cstdarg>
#include <cstdio>
#include <cstring>

static int get_log_level();

static const int gLogLevelEnv = get_log_level();

static const int kLogLevelError = 0;
static const int kLogLevelWarn = 1;
static const int kLogLevelInfo = 2;
static const int kLogLevelDebug = 3;
static const int kLogLevelTrace = 4;

static int get_log_level() {
    const char* log_level = std::getenv("PICKLE_LOG_LEVEL");
    if (log_level == nullptr) {
        return kLogLevelInfo;
    } else {
        std::string log_level_str(log_level);
        if (log_level_str == "TRACE" || log_level_str == "trace" || log_level_str == "Trace") {
            return kLogLevelTrace;
        } else if (log_level_str == "DEBUG" || log_level_str == "debug" || log_level_str == "Debug") {
            return kLogLevelDebug;
        } else if (log_level_str == "INFO" || log_level_str == "info" || log_level_str == "Info") {
            return kLogLevelInfo;
        } else if (log_level_str == "WARN" || log_level_str == "warn" || log_level_str == "Warn") {
            return kLogLevelWarn;
        } else if (log_level_str == "ERROR" || log_level_str == "error" || log_level_str == "Error") {
            return kLogLevelError;
        } else {
            return kLogLevelInfo;
        }
    }
}

#define TRACE(f, ...)                              \
    {                                              \
        if (gLogLevelEnv >= kLogLevelTrace) {      \
            ::fmt::println(                        \
                "{:%H:%M:%S} [TRACE]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    }

#define DEBUG(f, ...)                              \
    {                                              \
        if (gLogLevelEnv >= kLogLevelDebug) {      \
            ::fmt::println(                        \
                "{:%H:%M:%S} [DEBUG]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    }

#define INFO(f, ...)                               \
    {                                              \
        if (gLogLevelEnv >= kLogLevelInfo) {       \
            ::fmt::println(                        \
                "{:%H:%M:%S} [INFO ]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    }

#define WARN(f, ...)                               \
    {                                              \
        if (gLogLevelEnv >= kLogLevelWarn) {       \
            ::fmt::println(                        \
                "{:%H:%M:%S} [WARN ]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    }

#define ERROR(f, ...)                              \
    {                                              \
        if (gLogLevelEnv >= kLogLevelError) {      \
            ::fmt::println(                        \
                "{:%H:%M:%S} [ERROR]  {} ({}:{})", \
                std::chrono::system_clock::now(),  \
                _pickle_format_(f, ##__VA_ARGS__), \
                __FILE__,                          \
                __LINE__                           \
            );                                     \
        }                                          \
    }

#define PICKLE_ASSERT(expr, ...)                                                                                \
    {                                                                                                           \
        if (!(expr)) {                                                                                          \
            throw std::runtime_error(                                                                           \
                ::fmt::format("Assertion failed. {} ({}:{})", _pickle_format_(__VA_ARGS__), __FILE__, __LINE__) \
            );                                                                                                  \
        }                                                                                                       \
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

#endif