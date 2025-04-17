#include <gtest/gtest.h>

class Base {
public:
    virtual ~Base() = default;
    virtual int getValue() const = 0;
    virtual void setValue(int val) = 0;
};

class Derived: public Base {
private:
    int value = 0;

public:
    Derived() = default;

    int getValue() const override {
        return value;
    }

    void setValue(int val) override {
        value = val;
    }
};

TEST(PolymorphismTest, DerivedValue) {
    Base* base = new Derived();
    base->setValue(42);
    EXPECT_EQ(base->getValue(), 42);
    delete base;
}

TEST(PolymorphismTest, SmartPointer) {
    std::unique_ptr<Base> base = std::make_unique<Derived>();
    base->setValue(42);
    EXPECT_EQ(base->getValue(), 42);
}

TEST(PolymorphismTest, Constness) {
    std::shared_ptr<const Base> base = std::make_shared<const Derived>();
    EXPECT_EQ(base->getValue(), 0);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}