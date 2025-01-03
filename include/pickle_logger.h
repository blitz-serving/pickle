#ifndef _LOGGER_H_
#define _LOGGER_H_

#define FMT_HEADER_ONLY

#include <fmt/chrono.h>
#include <fmt/core.h>

#include <cstdarg>
#include <cstdio>

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
        switch (log_level[0]) {
            case 'T':
            case 't':
                return kLogLevelTrace;
            case 'D':
            case 'd':
                return kLogLevelDebug;
            case 'I':
            case 'i':
                return kLogLevelInfo;
            case 'W':
            case 'w':
                return kLogLevelWarn;
            case 'E':
            case 'e':
                return kLogLevelError;
            default:
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

static inline const char* _pickle_format_() {
    return "";
}

template<typename T>
static inline T&& _pickle_format_(T&& f) {
    return std::forward<T>(f);
}

template<typename T, typename... Args>
static inline std::string _pickle_format_(T&& f, Args&&... args) {
    return ::fmt::vformat(f, ::fmt::make_format_args(args...));
}

#endif