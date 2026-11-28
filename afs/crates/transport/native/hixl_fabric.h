#pragma once

#include <cstddef>
#include <cstdint>
#include <list>

namespace afs_hixl_fabric {
// Private sentinel: a vendor call or rollback left uncertain ownership.
constexpr uint32_t kOwnershipUnknown = UINT32_MAX;
constexpr size_t kAllocationAlignment = 2U * 1024U * 1024U;

// Wire keys are stable even in installed SDK headers that omit the constants.
inline constexpr char kEnableFabricMem[] = "EnableUseFabricMem";
inline constexpr char kGlobalResourceConfig[] = "GlobalResourceConfig";

// Used serially on the lane's ACL owner thread. Its lifetime covers every slab,
// including resources retained when that lane is quarantined.
class Provider {
public:
    Provider() = default;
    ~Provider();
    Provider(const Provider &) = delete;
    Provider &operator=(const Provider &) = delete;
    const char *placement_source() const noexcept { return vmm_ ? "acl-vmm" : "sdk-managed"; }
    uint32_t open(char *error, size_t error_len);
    uint32_t allocate(size_t bytes, void **address, char *error, size_t error_len);
    uint32_t free(void *address, char *error, size_t error_len);
private:
    struct Mapping {
        void *base = nullptr;
        void *physical = nullptr;
        void *retained = nullptr;
        bool mapped = false;
        bool reserved = false;
    };
    uint32_t release_mapping(Mapping &mapping, char *error, size_t error_len);
    uint32_t allocate_vmm(size_t bytes, void **address, char *error, size_t error_len);
    std::list<Mapping> mappings_;
    void *library_ = nullptr;
    using Allocate = uint32_t (*)(size_t, void **);
    using Free = uint32_t (*)(void *);
    Allocate allocate_ = nullptr;
    Free free_ = nullptr;
    bool vmm_ = false;
};
}  // namespace afs_hixl_fabric
