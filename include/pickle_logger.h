#ifndef _LOGGER_H_
#define _LOGGER_H_

#define FMT_HEADER_ONLY

#include <fmt/chrono.h>
#include <fmt/core.h>

#include <cstdarg>
#include <cstdio>

const int kLogLevelError = 0;
const int kLogLevelWarn = 1;
const int kLogLevelInfo = 2;
const int kLogLevelDebug = 3;
const int kLogLevelTrace = 4;

inline int get_log_level() {
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

static int gEnvLogLevel = get_log_level();

#define TRACE(f, ...) \
    { \
        if (gEnvLogLevel >= kLogLevelTrace) { \
            ::fmt::println( \
                "{:%H:%M:%S} [TRACE]  {} ({}:{})", \
                std::chrono::system_clock::now(), \
                _format_valist(f, ##__VA_ARGS__), \
                __FILE__, \
                __LINE__ \
            ); \
        } \
    }

#define DEBUG(f, ...) \
    { \
        if (gEnvLogLevel >= kLogLevelDebug) { \
            ::fmt::println( \
                "{:%H:%M:%S} [DEBUG]  {} ({}:{})", \
                std::chrono::system_clock::now(), \
                _format_valist(f, ##__VA_ARGS__), \
                __FILE__, \
                __LINE__ \
            ); \
        } \
    }

#define INFO(f, ...) \
    { \
        if (gEnvLogLevel >= kLogLevelInfo) { \
            ::fmt::println( \
                "{:%H:%M:%S} [INFO ]  {} ({}:{})", \
                std::chrono::system_clock::now(), \
                _format_valist(f, ##__VA_ARGS__), \
                __FILE__, \
                __LINE__ \
            ); \
        } \
    }

#define WARN(f, ...) \
    { \
        if (gEnvLogLevel >= kLogLevelWarn) { \
            ::fmt::println( \
                "{:%H:%M:%S} [WARN ]  {} ({}:{})", \
                std::chrono::system_clock::now(), \
                _format_valist(f, ##__VA_ARGS__), \
                __FILE__, \
                __LINE__ \
            ); \
        } \
    }

#define ERROR(f, ...) \
    { \
        if (gEnvLogLevel >= kLogLevelError) { \
            ::fmt::println( \
                "{:%H:%M:%S} [ERROR]  {} ({}:{})", \
                std::chrono::system_clock::now(), \
                _format_valist(f, ##__VA_ARGS__), \
                __FILE__, \
                __LINE__ \
            ); \
        } \
    }

template<typename T>
inline T&& _format_valist(T&& f) {
    return std::forward<T>(f);
}

template<typename T, typename... Args>
inline std::string _format_valist(T&& f, Args&&... args) {
    return ::fmt::vformat(f, ::fmt::make_format_args(args...));
}

#endif