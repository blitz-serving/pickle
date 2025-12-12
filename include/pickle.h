#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "executor_nvlink.h"
#include "executor_rdma.h"
#include "rdma_util.h"

namespace pickle {

class PickleSender {
public:
    enum class BackendType { kRdma, kNvlink };

    static std::shared_ptr<PickleSender> create_with_rdma(std::shared_ptr<RdmaSender> rdma_sender);

    static std::shared_ptr<PickleSender> create_with_nvlink(std::shared_ptr<NvlinkSender> nvlink_sender);

    // 注册一个用于 RDMA lkey 查找的 MemoryRegion
    void register_memory_region(std::shared_ptr<rdma_util::MemoryRegion> mr) {
        this->memory_regions_.push_back(std::move(mr));
    }

    void register_nvlink_handle(NvlinkHandle handle) {
        this->nvlink_handles_.push_back(handle);
    }

    [[nodiscard]] std::shared_ptr<Event> send(uint32_t unique_id, uint64_t addr, uint64_t length);

    void poll();

    BackendType backend_type() const {
        return this->backend_type_;
    }

private:
    explicit PickleSender(std::shared_ptr<RdmaSender> rdma_sender);
    explicit PickleSender(std::shared_ptr<NvlinkSender> nvlink_sender);

    uint32_t lookup_lkey(uint64_t addr, uint64_t length) const;
    NvlinkHandle lookup_ipc_handle(uint64_t addr, uint64_t length) const;

    BackendType backend_type_;
    std::shared_ptr<RdmaSender> rdma_sender_;
    std::shared_ptr<NvlinkSender> nvlink_sender_;
    std::vector<std::shared_ptr<rdma_util::MemoryRegion>> memory_regions_;
    std::vector<NvlinkHandle> nvlink_handles_;
};

class PickleRecver {
public:
    enum class BackendType { kRdma, kNvlink };

    static std::shared_ptr<PickleRecver> create_with_rdma(std::shared_ptr<RdmaRecver> rdma_recver);

    static std::shared_ptr<PickleRecver> create_with_nvlink(std::shared_ptr<NvlinkRecver> nvlink_recver);

    // 注册一个用于 RDMA rkey 查找的 MemoryRegion
    void register_memory_region(std::shared_ptr<rdma_util::MemoryRegion> mr);

    [[nodiscard]] std::shared_ptr<Event> recv(uint32_t unique_id, uint64_t addr, uint64_t length);

    void poll();

    BackendType backend_type() const {
        return this->backend_type_;
    }

private:
    explicit PickleRecver(std::shared_ptr<RdmaRecver> rdma_recver);
    explicit PickleRecver(std::shared_ptr<NvlinkRecver> nvlink_recver);

    uint32_t lookup_rkey(uint64_t addr, uint64_t length) const;

    BackendType backend_type_;
    std::shared_ptr<RdmaRecver> rdma_recver_;
    std::shared_ptr<NvlinkRecver> nvlink_recver_;
    std::vector<std::shared_ptr<rdma_util::MemoryRegion>> memory_regions_;
};

}  // namespace pickle
