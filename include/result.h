#pragma once

#include <cstdlib>
#include <iostream>
#include <optional>
#include <source_location>
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

template<typename T>
concept Streamable = requires(std::ostream& os, T&& t) { os << std::forward<T>(t); };

template<typename Derived>
class ResultBase {
private:
    const Derived& as_derived_ref() const noexcept {
        return static_cast<const Derived&>(*this);
    }

protected:
    void ensure_ok(const std::source_location& loc) const noexcept {
        if (!as_derived_ref().is_ok()) {
            std::cerr << loc.file_name() << ":" << loc.line() << ":" << loc.column()
                      << ": unwrap on an Err value: " << as_derived_ref().get_error() << std::endl;
            std::abort();
        }
    }

    void ensure_err(const std::source_location& loc) const noexcept {
        if (!as_derived_ref().is_err()) {
            std::cerr << loc.file_name() << ":" << loc.line() << ":" << loc.column() << ": unwrap_err on an Ok value"
                      << std::endl;
            std::abort();
        }
    }
};

template<typename T, typename E>
    requires(!std::is_void_v<E>)
class Result: ResultBase<Result<T, E>> {
    friend class ResultBase<Result<T, E>>;

private:
    std::variant<T, E> storage_;

    const E& get_error() const noexcept {
        return std::get<E>(storage_);
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

    T& unwrap(const std::source_location& loc = std::source_location::current()) & noexcept
        requires Streamable<E>
    {
        this->ensure_ok(loc);
        return std::get<T>(storage_);
    }

    T&& unwrap(const std::source_location& loc = std::source_location::current()) && noexcept
        requires Streamable<E>
    {
        this->ensure_ok(loc);
        return std::move(std::get<T>(storage_));
    }

    E& unwrap_err(const std::source_location& loc = std::source_location::current()) & noexcept {
        this->ensure_err(loc);
        return std::get<E>(storage_);
    }

    E&& unwrap_err(const std::source_location& loc = std::source_location::current()) && noexcept {
        this->ensure_err(loc);
        return std::move(std::get<E>(storage_));
    }
};

template<typename T>
    requires(!std::is_void_v<T>)
class Result<T, T>: ResultBase<Result<T, T>> {
    friend class ResultBase<Result<T, T>>;

private:
    bool ok_;
    T storage_;

    const T& get_error() const noexcept {
        return storage_;
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

    T& unwrap(const std::source_location& loc = std::source_location::current()) & noexcept
        requires Streamable<T>
    {
        this->ensure_ok(loc);
        return storage_;
    }

    T&& unwrap(const std::source_location& loc = std::source_location::current()) && noexcept
        requires Streamable<T>
    {
        this->ensure_ok(loc);
        return std::move(storage_);
    }

    T& unwrap_err(const std::source_location& loc = std::source_location::current()) & noexcept {
        this->ensure_err(loc);
        return storage_;
    }

    T&& unwrap_err(const std::source_location& loc = std::source_location::current()) && noexcept {
        this->ensure_err(loc);
        return std::move(storage_);
    }
};

template<typename E>
    requires(!std::is_void_v<E>)
class Result<void, E>: ResultBase<Result<void, E>> {
    friend class ResultBase<Result<void, E>>;

private:
    std::optional<E> error_;

    const E& get_error() const noexcept {
        return *error_;
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

    void unwrap(const std::source_location& loc = std::source_location::current()) const noexcept
        requires Streamable<E>
    {
        this->ensure_ok(loc);
    }

    E& unwrap_err(const std::source_location& loc = std::source_location::current()) & noexcept {
        this->ensure_err(loc);
        return *error_;
    }

    E&& unwrap_err(const std::source_location& loc = std::source_location::current()) && noexcept {
        this->ensure_err(loc);
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
