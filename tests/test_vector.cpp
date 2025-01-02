#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

using namespace std;

struct Element {
    uint32_t v1;
    uint64_t v2;
    uint32_t v3;
};

void debug_print(const Element& e) {
    cout << "{ v1 = " << e.v1 << ", v2 = " << e.v2 << ", v3 = " << e.v3 << " }" << endl;
}

TEST(Vector, Resize) {
    vector<Element> vec;

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }

    vec.resize(10);
    for (int i = 0; i < 10; ++i) {
        vec[i].v1 = i;
        vec[i].v2 = i;
        vec[i].v3 = i;
    }

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }

    vec.resize(5);

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }

    vec.resize(10);

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }
}

TEST(Vector, ResizeToZero) {
    vector<Element> vec;

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }

    vec.resize(10);
    for (int i = 0; i < 10; ++i) {
        vec[i].v1 = i;
        vec[i].v2 = i;
        vec[i].v3 = i;
    }

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }

    vec.resize(0);

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }

    vec.clear();

    cout << "vec.size() = " << vec.size() << endl;
    cout << "vec.capacity() = " << vec.capacity() << endl;
    for (const auto& e : vec) {
        debug_print(e);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}