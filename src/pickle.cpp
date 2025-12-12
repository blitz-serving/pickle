#include "pickle.h"

#include <utility>

#include "executor_nvlink.h"

namespace pickle {

std::shared_ptr<PickleSender> PickleSender::create_with_rdma(std::shared_ptr<RdmaSender> rdma_sender) {
    PICKLE_ASSERT(rdma_sender != nullptr, "rdma_sender should not be null");
    return std::shared_ptr<PickleSender>(new PickleSender(std::move(rdma_sender)));
}

std::shared_ptr<PickleSender> PickleSender::create_with_nvlink(std::shared_ptr<NvlinkSender> nvlink_sender) {
    PICKLE_ASSERT(nvlink_sender != nullptr, "nvlink_sender should not be null");
    return std::shared_ptr<PickleSender>(new PickleSender(std::move(nvlink_sender)));
}

PickleSender::PickleSender(std::shared_ptr<RdmaSender> rdma_sender) :
    backend_type_(BackendType::kRdma),
    rdma_sender_(std::move(rdma_sender)) {}

PickleSender::PickleSender(std::shared_ptr<NvlinkSender> nvlink_sender) :
    backend_type_(BackendType::kNvlink),
    nvlink_sender_(std::move(nvlink_sender)) {}

uint32_t PickleSender::lookup_lkey(uint64_t addr, uint64_t length) const {
    uint64_t end_addr = addr + length;
    PICKLE_ASSERT(end_addr >= addr, "Address overflow when looking up lkey");

    for (const auto& mr : this->memory_regions_) {
        PICKLE_ASSERT(mr != nullptr, "Encountered null MemoryRegion in registry");
        uint64_t base_addr = reinterpret_cast<uint64_t>(mr->get_addr());
        uint64_t limit = base_addr + mr->get_length();
        if (addr >= base_addr && end_addr <= limit) {
            return mr->get_lkey();
        }
    }

    PICKLE_ASSERT(false, "No MemoryRegion covers addr=0x{:x}, length=0x{:x}", addr, length);
    return 0;
}

NvlinkHandle PickleSender::lookup_ipc_handle(uint64_t addr, uint64_t length) const {
    uint64_t end_addr = addr + length;

    for (const auto& handle : this->nvlink_handles_) {
        uint64_t base_addr = handle.addr;
        uint64_t limit = base_addr + handle.length;
        if (addr >= base_addr && end_addr <= limit) {
            return handle;
        }
    }

    PICKLE_ASSERT(false, "No NVLink IPC handle covers addr=0x{:x}, length=0x{:x}", addr, length);
    return NvlinkHandle {};
}

std::shared_ptr<Event> PickleSender::send(uint32_t unique_id, uint64_t addr, uint64_t length) {
    if (this->backend_type_ == BackendType::kNvlink) {
        auto nvlink_handle = this->lookup_ipc_handle(addr, length);
        return this->nvlink_sender_->send(unique_id, addr - nvlink_handle.addr, length, nvlink_handle.handle);
    } else {
        return this->rdma_sender_->send(unique_id, addr, length, this->lookup_lkey(addr, length));
    }
}

void PickleSender::poll() {
    if (this->backend_type_ == BackendType::kNvlink) {
        this->nvlink_sender_->poll();
    } else {
        this->rdma_sender_->poll();
    }
}

// ==================== PickleRecver ====================

std::shared_ptr<PickleRecver> PickleRecver::create_with_rdma(std::shared_ptr<RdmaRecver> rdma_recver) {
    PICKLE_ASSERT(rdma_recver != nullptr, "rdma_recver should not be null");
    return std::shared_ptr<PickleRecver>(new PickleRecver(std::move(rdma_recver)));
}

std::shared_ptr<PickleRecver> PickleRecver::create_with_nvlink(std::shared_ptr<NvlinkRecver> nvlink_recver) {
    PICKLE_ASSERT(nvlink_recver != nullptr, "nvlink_recver should not be null");
    return std::shared_ptr<PickleRecver>(new PickleRecver(std::move(nvlink_recver)));
}

PickleRecver::PickleRecver(std::shared_ptr<RdmaRecver> rdma_recver) :
    backend_type_(BackendType::kRdma),
    rdma_recver_(std::move(rdma_recver)) {}

PickleRecver::PickleRecver(std::shared_ptr<NvlinkRecver> nvlink_recver) :
    backend_type_(BackendType::kNvlink),
    nvlink_recver_(std::move(nvlink_recver)) {}

void PickleRecver::register_memory_region(std::shared_ptr<rdma_util::MemoryRegion> mr) {
    PICKLE_ASSERT(mr != nullptr, "MemoryRegion should not be null");
    this->memory_regions_.push_back(std::move(mr));
}

uint32_t PickleRecver::lookup_rkey(uint64_t addr, uint64_t length) const {
    uint64_t end_addr = addr + length;
    PICKLE_ASSERT(end_addr >= addr, "Address overflow when looking up rkey");

    for (const auto& mr : this->memory_regions_) {
        PICKLE_ASSERT(mr != nullptr, "Encountered null MemoryRegion in registry");
        uint64_t base_addr = reinterpret_cast<uint64_t>(mr->get_addr());
        uint64_t limit = base_addr + mr->get_length();
        if (addr >= base_addr && end_addr <= limit) {
            return mr->get_rkey();
        }
    }

    PICKLE_ASSERT(false, "No MemoryRegion covers addr=0x{:x}, length=0x{:x}", addr, length);
    return 0;
}

std::shared_ptr<Event> PickleRecver::recv(uint32_t unique_id, uint64_t addr, uint64_t length) {
    if (this->backend_type_ == BackendType::kNvlink) {
        return this->nvlink_recver_->recv(unique_id, addr, length);
    } else {
        return this->rdma_recver_->recv(unique_id, addr, length, this->lookup_rkey(addr, length));
    }
}

void PickleRecver::poll() {
    if (this->backend_type_ == BackendType::kNvlink) {
        this->nvlink_recver_->poll();
    } else {
        this->rdma_recver_->poll();
    }
}

}  // namespace pickle
