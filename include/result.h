#include <cstdio>
#include <cstdlib>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

namespace result {

template<typename E>
    requires(!std::is_void_v<E>)
struct Err {
    E error_;

    explicit Err(const E& error) noexcept(std::is_nothrow_copy_constructible_v<E>)
        requires std::is_copy_constructible_v<E>
        : error_(error) {}

    explicit Err(E&& error) noexcept(std::is_nothrow_move_constructible_v<E>)
        requires std::is_move_constructible_v<E>
        : error_(std::move(error)) {}
};

template<typename T>
struct Ok {
    T value_;

    explicit Ok(const T& value) noexcept(std::is_nothrow_copy_constructible_v<T>)
        requires std::is_copy_constructible_v<T>
        : value_(value) {}

    explicit Ok(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>)
        requires std::is_move_constructible_v<T>
        : value_(std::move(value)) {}
};

template<>
struct Ok<void> {
    Ok() noexcept {}
};

// Class Template Argument Deduction since cpp17
Ok() -> Ok<void>;

template<typename T, typename E>
    requires(!std::is_void_v<E>)
struct Result {
private:
    std::variant<T, E> storage_;

    inline void ensure_ok() const noexcept {
        if (!is_ok()) {
            std::fprintf(stderr, "Called unwrap on an Err value\n");
            std::abort();
        }
    }

    inline void ensure_err() const noexcept {
        if (!is_err()) {
            std::fprintf(stderr, "Called unwrap_err on an Ok value\n");
            std::abort();
        }
    }

public:
    Result(const Ok<T>& ok) noexcept(std::is_nothrow_copy_constructible_v<T>)
        requires std::is_copy_constructible_v<T>
        : storage_(ok.value_) {}

    Result(Ok<T>&& ok) noexcept(std::is_nothrow_move_constructible_v<T>)
        requires std::is_move_constructible_v<T>
        : storage_(std::move(ok.value_)) {}

    Result(const Err<E>& err) noexcept(std::is_nothrow_copy_constructible_v<E>)
        requires std::is_copy_constructible_v<E>
        : storage_(err.error_) {}

    Result(Err<E>&& err) noexcept(std::is_nothrow_move_constructible_v<E>)
        requires std::is_move_constructible_v<E>
        : storage_(std::move(err.error_)) {}

    bool is_ok() const noexcept {
        return std::holds_alternative<T>(storage_);
    }

    bool is_err() const noexcept {
        return std::holds_alternative<E>(storage_);
    }

    T& unwrap() & noexcept {
        ensure_ok();
        return std::get<T>(storage_);
    }

    T&& unwrap() && noexcept {
        ensure_ok();
        return std::move(std::get<T>(storage_));
    }

    E& unwrap_err() & noexcept {
        ensure_err();
        return std::get<E>(storage_);
    }

    E&& unwrap_err() && noexcept {
        ensure_err();
        return std::move(std::get<E>(storage_));
    }
};

template<typename T>
    requires(!std::is_void_v<T>)
struct Result<T, T> {
private:
    bool ok_;
    T storage_;

    inline void ensure_ok() const noexcept {
        if (!is_ok()) {
            std::fprintf(stderr, "Called unwrap on an Err value\n");
            std::abort();
        }
    }

    inline void ensure_err() const noexcept {
        if (!is_err()) {
            std::fprintf(stderr, "Called unwrap_err on an Ok value\n");
            std::abort();
        }
    }

public:
    Result(const Ok<T>& ok) noexcept(std::is_nothrow_copy_constructible_v<T>)
        requires std::is_copy_constructible_v<T>
        : ok_(true), storage_(ok.value_) {}

    Result(Ok<T>&& ok) noexcept(std::is_nothrow_move_constructible_v<T>)
        requires std::is_move_constructible_v<T>
        : ok_(true), storage_(std::move(ok.value_)) {}

    Result(const Err<T>& err) noexcept(std::is_nothrow_copy_constructible_v<T>)
        requires std::is_copy_constructible_v<T>
        : ok_(false), storage_(err.error_) {}

    Result(Err<T>&& err) noexcept(std::is_nothrow_move_constructible_v<T>)
        requires std::is_move_constructible_v<T>
        : ok_(false), storage_(std::move(err.error_)) {}

    bool is_ok() const noexcept {
        return ok_;
    }

    bool is_err() const noexcept {
        return !ok_;
    }

    T& unwrap() & noexcept {
        ensure_ok();
        return storage_;
    }

    T&& unwrap() && noexcept {
        ensure_ok();
        return std::move(storage_);
    }

    T& unwrap_err() & noexcept {
        ensure_err();
        return storage_;
    }

    T&& unwrap_err() && noexcept {
        ensure_err();
        return std::move(storage_);
    }
};

template<typename E>
    requires(!std::is_void_v<E>)
struct Result<void, E> {
private:
    std::optional<E> error_;

    inline void ensure_ok() const noexcept {
        if (is_err()) {
            std::fprintf(stderr, "Called unwrap on an Err value\n");
            std::abort();
        }
    }

    inline void ensure_err() const noexcept {
        if (is_ok()) {
            std::fprintf(stderr, "Called unwrap_err on an Ok value\n");
            std::abort();
        }
    }

public:
    Result(Ok<void>) noexcept : error_(std::nullopt) {}

    Result(const Err<E>& err) noexcept(std::is_nothrow_copy_constructible_v<E>)
        requires std::is_copy_constructible_v<E>
        : error_(err.error_) {}

    Result(Err<E>&& err) noexcept(std::is_nothrow_move_constructible_v<E>)
        requires std::is_move_constructible_v<E>
        : error_(std::move(err.error_)) {}

    bool is_ok() const noexcept {
        return !error_.has_value();
    }

    bool is_err() const noexcept {
        return error_.has_value();
    }

    void unwrap() const noexcept {
        ensure_ok();
    }

    E& unwrap_err() & noexcept {
        ensure_err();
        return *error_;
    }

    E&& unwrap_err() && noexcept {
        ensure_err();
        return std::move(*error_);
    }
};

}  // namespace result

#define TRY(...)                                             \
    __extension__({                                          \
        auto res = __VA_ARGS__;                              \
        if (!res.is_ok()) {                                  \
            return result::Err(std::move(res).unwrap_err()); \
        }                                                    \
        std::move(res).unwrap();                             \
    })
