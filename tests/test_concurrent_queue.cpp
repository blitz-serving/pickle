#include <concurrentqueue.h>
#include <gtest/gtest.h>

#include <vector>

class Object {
  public:
    Object() = default;
    Object(const Object&) = default;
    Object& operator=(const Object&) = default;

    Object(int v) : value(v) {}

    int value;
};

TEST(Dequeue, DequeueBulk) {
    std::vector<Object> objects(5);

    moodycamel::ConcurrentQueue<Object> queue;
    queue.enqueue(Object(1));
    queue.enqueue(Object(2));
    queue.enqueue(Object(3));
    queue.enqueue(Object(4));
    queue.enqueue(Object(5));

    ASSERT_EQ(queue.size_approx(), 5);

    ASSERT_EQ(queue.try_dequeue_bulk(objects.begin(), 3), 3);

    ASSERT_EQ(queue.size_approx(), 2);

    ASSERT_EQ(objects[0].value, 1);
    ASSERT_EQ(objects[1].value, 2);
    ASSERT_EQ(objects[2].value, 3);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}