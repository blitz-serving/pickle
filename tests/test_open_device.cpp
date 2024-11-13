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
    const char* dev_name = "nonexistent";
    ibv_context* context;
    auto result = rdma_util::open_ib_device(&context, dev_name);
    ASSERT_EQ(context, nullptr);
}

TEST(OpenDevice, OpenIBDevice) {
    const char* dev_name = "mlx5_0";
    ibv_context* context;
    auto result = rdma_util::open_ib_device(&context, dev_name);
    ASSERT_NE(context, nullptr);
    ASSERT_EQ(rdma_util::close_ib_device(context), rdma_util::Result_t::SUCCESS);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}