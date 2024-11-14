#include <gtest/gtest.h>

#include "rdma_util.h"

TEST(OpenDevice, GetDevices) {
    ibv_device** dev_list = ibv_get_device_list(nullptr);
    ASSERT_NE(dev_list, nullptr);
    for (int i = 0; dev_list[i] != nullptr; i++) {
        const char* dev_name = ibv_get_device_name(dev_list[i]);
        ASSERT_NE(dev_name, nullptr);
        printf("Device %d: %s\n", i, dev_name);
    }
    ibv_free_device_list(dev_list);
}

TEST(OpenDevice, OpenNonexistentDevice) {
    try {
        const char* dev_name = "nonexistent";
        auto context = rdma_util::Context::create(dev_name);
        FAIL();
    } catch (std::runtime_error& e) {
        printf("Caught exception: %s\n", e.what());
    }
}

TEST(OpenDevice, OpenIBDevice) {
    const char* dev_name = "mlx5_0";
    auto context = rdma_util::Context::create(dev_name);
    ASSERT_NE(context, nullptr);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}