#include <cassert>
#include <ostream>
#include <string>

#include "result.h"

// Helper to check noexcept
template<typename F>
constexpr bool is_nothrow_invocable = noexcept(std::declval<F>()());

// Dummy non-copyable type
struct NonCopyable {
    int val;

    NonCopyable(int v) : val(v) {}

    NonCopyable(const NonCopyable&) = delete;
    NonCopyable& operator=(const NonCopyable&) = delete;

    NonCopyable(NonCopyable&&) noexcept : val(0) {}
};

// Test: Result<int, std::string>
void test_basic_result() {
    using R = result::Result<int, std::string>;

    // Ok path
    R ok_res = result::Ok<int>(42);
    assert(ok_res.is_ok());
    assert(!ok_res.is_err());
    assert(ok_res.unwrap() == 42);

    // Err path
    R err_res = result::Err<std::string>("error");
    assert(!err_res.is_ok());
    assert(err_res.is_err());
    assert(err_res.unwrap_err() == "error");

    // Move semantics
    R moved_ok = result::Ok<int>(99);
    int val = std::move(moved_ok).unwrap();
    assert(val == 99);
}

// Test: Result<T, T>
void test_same_type_result() {
    using R = result::Result<std::string, std::string>;

    R ok_res = result::Ok<std::string>("success");
    assert(ok_res.is_ok());
    assert(ok_res.unwrap() == "success");

    R err_res = result::Err<std::string>("failure");
    assert(err_res.is_err());
    assert(err_res.unwrap_err() == "failure");

    // Move
    R moved = result::Ok<std::string>("moved");
    std::string s = std::move(moved).unwrap();
    assert(s == "moved");
}

// Test: Result<void, E>
void test_void_result() {
    using R = result::Result<void, std::string>;

    R ok_res = result::Ok<void> {};
    assert(ok_res.is_ok());
    assert(!ok_res.is_err());
    ok_res.unwrap();  // no-op

    R err_res = result::Err<std::string>("void error");
    assert(err_res.is_err());
    assert(err_res.unwrap_err() == "void error");
}

// Test: Non-copyable types (move-only)
void test_move_only() {
    using R = result::Result<NonCopyable, std::string>;

    R ok_res = result::Ok<NonCopyable>(NonCopyable {42});
    assert(ok_res.is_ok());
    assert(std::move(ok_res).unwrap().val == 0);  // moved-from value is 0

    R err_res = result::Err(std::string("err"));
    assert(err_res.is_err());
    assert(std::move(err_res).unwrap_err() == "err");
}

// Test: noexcept correctness
void test_noexcept() {
    // Ok construction
    static_assert(noexcept(std::string("err")) == false);
    static_assert(noexcept(result::Ok(42)));
    static_assert(noexcept(result::Ok<std::string>("hello")) == false);

    // Err construction
    static_assert(noexcept(result::Err(42)));

    // Result construction from Ok/Err
    using R = result::Result<int, std::string>;
    static_assert(noexcept(R(result::Ok(42))));
    static_assert(noexcept(R(result::Err(std::string("err")))) == false);
}

// Test: TRY macro
result::Result<int, std::string> returns_ok(int x) {
    return result::Ok(x * 2);
}

result::Result<int, std::string> returns_err() {
    return result::Err(std::string("inner error"));
}

result::Result<int, std::string> try_ok() {
    int a = TRY(returns_ok(10));
    int b = TRY(returns_ok(20));
    return result::Ok(a + b);
}

result::Result<int, std::string> try_err() {
    int a = TRY(returns_ok(10));
    int b = TRY(returns_err());  // should short-circuit
    (void)a;
    (void)b;
    return result::Ok(0);  // unreachable
}

void test_try_macro() {
    auto res1 = try_ok();
    assert(res1.is_ok());
    assert(res1.unwrap() == 60);

    auto res2 = try_err();
    assert(res2.is_err());
    assert(res2.unwrap_err() == "inner error");
}

// Test: Type deduction for Ok<void>
void test_ok_void_deduction() {
    auto ok = result::Ok {};
    static_assert(std::is_same_v<decltype(ok), result::Ok<void>>);
}

// Test: Const correctness (not implemented but worth noting)
// Your current design lacks const overloads for unwrap.
// If needed, add const & / const && overloads.

struct MyError {
    std::string message;

    MyError(std::string msg) : message(std::move(msg)) {}
};

std::ostream& operator<<(std::ostream& os, const MyError& err) {
    return os << "MyError: " << err.message;
}

// Main
int main() {
    test_basic_result();
    test_same_type_result();
    test_void_result();
    test_move_only();
    test_noexcept();
    test_try_macro();
    test_ok_void_deduction();
    result::Result<void, MyError>(result::Err(MyError("custom error"))).unwrap();
    std::printf("All tests passed!\n");
    return 0;
}