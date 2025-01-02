#include "pickle_logger.h"
#include "rdma_util.h"

int main() {
    auto mr = rdma_util::Context::create("mlx5_0");

    TRACE("trace");
    DEBUG("debug");
    INFO("info");
    WARN("warn");
    ERROR("error");
    return 0;
}