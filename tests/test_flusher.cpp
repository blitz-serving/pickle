#include <fmt/format.h>

#include <memory>

#include "pickle.h"
#include "pickle_logger.h"
#include "rdma_util.h"

using namespace pickle;
using namespace rdma_util;

int main() {
    std::shared_ptr<Context> context = Context::create("mlx5_0");
    std::shared_ptr<ProtectionDomain> pd = ProtectionDomain::create(std::move(context));
    std::shared_ptr<LoopbackFlusher> flusher = LoopbackFlusher::create(pd);
    std::shared_ptr<MemoryRegion> mr = MemoryRegion::create(
        pd,
        std::shared_ptr<void>(new uint64_t[16], [](uint64_t* p) { delete[] p; }),
        sizeof(uint64_t) * 16
    );

    INFO("Base addr {}", fmt::ptr(mr->get_addr()));

    std::shared_ptr<std::atomic<bool>> flag;

    for (int i = 0; i < 16; ++i) {
        flag = std::make_shared<std::atomic<bool>>(false);
        auto addr = uint64_t(mr->get_addr()) + i * sizeof(uint64_t);
        INFO("Flush addr {}", fmt::ptr((void*)(addr)));
        flusher->append(mr->get_rkey(), addr, flag);
    }

    while (!flag->load()) {
        flusher->poll();
    }
}