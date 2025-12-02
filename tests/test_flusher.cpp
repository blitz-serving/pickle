#include <fmt/format.h>

#include <memory>

#include "executor_rdma.h"
#include "pickle_logger.h"
#include "rdma_util.h"

using namespace pickle;
using namespace rdma_util;

int main() {
    std::shared_ptr<Context> context = Context::create("mlx5_0");
    std::shared_ptr<ProtectionDomain> pd = ProtectionDomain::create(std::move(context));
    std::shared_ptr<Flusher> flusher = Flusher::create(pd);
    std::shared_ptr<MemoryRegion> mr = MemoryRegion::create(
        pd,
        std::shared_ptr<void>(new uint64_t[16], [](uint64_t* p) { delete[] p; }),
        sizeof(uint64_t) * 16
    );

    INFO("Base addr {}", fmt::ptr(mr->get_addr()));

    std::shared_ptr<Event> event;

    for (int i = 0; i < 16; ++i) {
        event = Event::create();
        auto addr = uint64_t(mr->get_addr()) + i * sizeof(uint64_t);
        INFO("Flush addr {}", fmt::ptr((void*)(addr)));
        flusher->append(mr->get_rkey(), addr, event);
    }

    while (!event->is_notified()) {
        flusher->poll();
    }
}
