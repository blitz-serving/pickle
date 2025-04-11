#include <gtest/gtest.h>

class Base {
public:
    virtual ~Base() = default;
    virtual int getValue() const = 0;
};

class Derived: public Base {
public:
    Derived(int value) : value(value) {}

    int getValue() const override {
        return value;
    }

private:
    int value;
};

TEST(PolymorphismTest, DerivedValue) {
    Base* base = new Derived(42);
    EXPECT_EQ(base->getValue(), 42);
    delete base;
}

TEST(PolymorphismTest, SmartPointer) {
    std::unique_ptr<Base> base = std::make_unique<Derived>(100);
    EXPECT_EQ(base->getValue(), 100);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}