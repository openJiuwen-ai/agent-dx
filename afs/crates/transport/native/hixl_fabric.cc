#include "hixl_fabric.h"

#include <hixl/hixl.h>
#include <dlfcn.h>
#include <cstdio>
#include <cstring>
#include <limits>
#include <new>
#include <string>

#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
#include <acl/acl_rt.h>
#endif

namespace afs_hixl_fabric {
namespace {
// dladdr must identify the loaded libafs_hixl.so, even when AFS_HIXL_MODULE
// points outside the executable directory. Never search the working directory.
const char module_anchor = 0;
constexpr const char *adapter = "libafs_hixl_fabric.so";
uint32_t fail(uint32_t status, char *out, size_t cap, const char *detail) {
    if (out != nullptr && cap != 0) { std::snprintf(out, cap, "%s", detail); }
    return status;
}

template <typename Function>
Function symbol(void *library, const char *name) {
    return reinterpret_cast<Function>(dlsym(library, name));
}

#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
struct VmmApi {
#define AFS_LOAD_ACL(name) decltype(&::name) name = symbol<decltype(&::name)>(RTLD_DEFAULT, #name)
    AFS_LOAD_ACL(aclrtGetDevice);
    AFS_LOAD_ACL(aclrtGetPhyDevIdByLogicDevId);
    AFS_LOAD_ACL(aclrtGetLogicDevIdByUserDevId);
    AFS_LOAD_ACL(aclrtMallocPhysical);
    AFS_LOAD_ACL(aclrtReserveMemAddress);
    AFS_LOAD_ACL(aclrtMapMem);
    AFS_LOAD_ACL(aclrtMemSetAccess);
    AFS_LOAD_ACL(aclrtMemGetAddressRange);
    AFS_LOAD_ACL(aclrtMemRetainAllocationHandle);
    AFS_LOAD_ACL(aclrtUnmapMem);
    AFS_LOAD_ACL(aclrtReleaseMemAddress);
    AFS_LOAD_ACL(aclrtFreePhysical);
#undef AFS_LOAD_ACL

    bool available() const {
        return aclrtGetDevice && aclrtGetPhyDevIdByLogicDevId && aclrtGetLogicDevIdByUserDevId &&
               aclrtMallocPhysical && aclrtReserveMemAddress && aclrtMapMem && aclrtMemSetAccess &&
               aclrtMemGetAddressRange && aclrtMemRetainAllocationHandle && aclrtUnmapMem &&
               aclrtReleaseMemAddress && aclrtFreePhysical;
    }
};

const VmmApi &vmm_api() {
    static const VmmApi api;
    return api;
}

// Public ACL runtime error codes. The separate rt_error_codes.h is not
// installed in every toolkit. For known page capability/pressure failures
// only, step down to the next page policy; a generic driver error is terminal.
constexpr aclError kRtFeatureNotSupported = 207000;
constexpr aclError kRtMemoryAllocation = 207001;
constexpr aclError kRtResourceAllocation = 207005;
constexpr aclError kRtDriverTimeout = 207020;

bool feature_unsupported(aclError status) {
    return status == ACL_ERROR_FEATURE_UNSUPPORTED || status == kRtFeatureNotSupported;
}

bool retry_page_policy(aclError status) {
    return feature_unsupported(status) || status == ACL_ERROR_BAD_ALLOC ||
           status == kRtMemoryAllocation || status == kRtResourceAllocation;
}

uint32_t acl_failure(aclError status, char *out, size_t cap, const char *operation) {
    if (out != nullptr && cap != 0) {
        std::snprintf(out, cap, "%s failed with ACL status %d", operation, status);
    }
    return status == kRtDriverTimeout ? kOwnershipUnknown : static_cast<uint32_t>(status);
}
#endif
}

Provider::~Provider() {
    if (library_ != nullptr) { dlclose(library_); }
}

uint32_t Provider::open(char *error, size_t cap) {
    Dl_info owner{};
    if (library_ != nullptr || vmm_ || !dladdr(&module_anchor, &owner) ||
        owner.dli_fname == nullptr || owner.dli_fname[0] != '/') {
        return fail(hixl::FAILED, error, cap, "cannot locate the absolute path of the FabricMem module");
    }
    std::string path(owner.dli_fname);
    path.resize(path.rfind('/') + 1);
    path += adapter;
    void *handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        const char *reason = dlerror();
        constexpr char missing[] = ": cannot open shared object file: No such file or directory";
        const auto *suffix = reason == nullptr ? nullptr : std::strstr(reason, missing);
        const bool adapter_missing = suffix != nullptr && path.size() == static_cast<size_t>(suffix - reason) &&
                                     path.compare(0, path.size(), reason, path.size()) == 0;
        return fail(adapter_missing ? hixl::UNSUPPORTED : hixl::FAILED, error, cap,
                    reason != nullptr ? reason : "failed to load FabricMem adapter");
    }
    const auto available = symbol<int (*)()>(handle, "afs_fabric_adxl_available_v1");
    const auto alloc = symbol<Allocate>(handle, "afs_fabric_alloc_v1");
    const auto release = symbol<Free>(handle, "afs_fabric_free_v1");
    if (available == nullptr || alloc == nullptr || release == nullptr) {
        dlclose(handle);
        return fail(hixl::FAILED, error, cap, "incompatible FabricMem adapter: missing v1 allocation API");
    }
    const int availability = available();
    if (availability > 0) {
        allocate_ = alloc;
        free_ = release;
        library_ = handle;
        return hixl::SUCCESS;
    }
    dlclose(handle);
    if (availability < 0) {
        return fail(hixl::FAILED, error, cap, "failed to load the CANN ADXL library or one of its dependencies");
    }
#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
    if (vmm_api().available()) {
        vmm_ = true;
        return hixl::SUCCESS;
    }
#endif
    return fail(hixl::UNSUPPORTED, error, cap, "ADXL allocation pair absent and ACL external FabricMem registration APIs unavailable");
}

uint32_t Provider::allocate(size_t bytes, void **address, char *error, size_t cap) {
    if (address == nullptr || bytes == 0 || (!vmm_ && allocate_ == nullptr)) {
        return fail(hixl::PARAM_INVALID, error, cap, "invalid FabricMem allocation");
    }
    *address = nullptr;
#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
    if (vmm_) { return allocate_vmm(bytes, address, error, cap); }
#endif
    const uint32_t status = allocate_(bytes, address);
    if (status == kOwnershipUnknown) { return fail(status, error, cap, "ADXL MallocMem threw; allocation ownership unknown"); }
    if (status != hixl::SUCCESS) { return fail(status, error, cap, "ADXL MallocMem(MEM_HOST) failed"); }
    return *address != nullptr ? status : fail(kOwnershipUnknown, error, cap, "ADXL MallocMem returned null");
}

uint32_t Provider::free(void *address, char *error, size_t cap) {
    if (address == nullptr) { return fail(hixl::PARAM_INVALID, error, cap, "invalid FabricMem release"); }
#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
    if (vmm_) {
        for (auto it = mappings_.begin(); it != mappings_.end(); ++it) {
            if (it->base != address) { continue; }
            const auto status = release_mapping(*it, error, cap);
            if (status == hixl::SUCCESS) { mappings_.erase(it); }
            return status;
        }
        return fail(hixl::PARAM_INVALID, error, cap, "unknown ACL FabricMem allocation");
    }
#endif
    if (free_ == nullptr) { return fail(hixl::PARAM_INVALID, error, cap, "FabricMem allocator not open"); }
    const uint32_t status = free_(address);
    return status == hixl::SUCCESS ? status : fail(status, error, cap, "ADXL FreeMem failed");
}

#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
uint32_t Provider::release_mapping(Mapping &mapping, char *error, size_t cap) {
    const auto &api = vmm_api();
    if (mapping.mapped) {
        const auto result = api.aclrtUnmapMem(mapping.base);
        if (result != ACL_SUCCESS) { return acl_failure(result, error, cap, "aclrtUnmapMem"); }
        mapping.mapped = false;
    }
    if (mapping.reserved) {
        const auto result = api.aclrtReleaseMemAddress(mapping.base);
        if (result != ACL_SUCCESS) { return acl_failure(result, error, cap, "aclrtReleaseMemAddress"); }
        mapping.reserved = false;
    }
    if (mapping.physical != nullptr) {
        const auto result = api.aclrtFreePhysical(mapping.physical);
        if (result != ACL_SUCCESS) { return acl_failure(result, error, cap, "aclrtFreePhysical"); }
        mapping.physical = nullptr;
    }
    return hixl::SUCCESS;
}

uint32_t Provider::allocate_vmm(size_t bytes, void **address, char *error, size_t cap) {
    if (bytes % kAllocationAlignment != 0) {
        return fail(hixl::PARAM_INVALID, error, cap, "ACL FabricMem allocation requires a multiple of 2 MiB");
    }
    // Allocate tracking metadata before calling ACL; no allocation may throw
    // once the first vendor resource is acquired.
    try {
        mappings_.emplace_front();
    } catch (const std::bad_alloc &) {
        return fail(hixl::RESOURCE_EXHAUSTED, error, cap, "cannot track ACL FabricMem allocation");
    }
    Mapping &mapping = mappings_.front();
    const auto &api = vmm_api();
    const auto rollback = [&](uint32_t status) {
        if (status == kOwnershipUnknown) { return status; }
        if (release_mapping(mapping, error, cap) != hixl::SUCCESS) {
            // The bridge keeps the entire runtime when startup ownership is uncertain.
            return kOwnershipUnknown;
        }
        mappings_.pop_front();
        return status;
    };
    const auto acl_failed = [&](aclError status, const char *operation) {
        return rollback(acl_failure(status, error, cap, operation));
    };
    int32_t user = -1;
    int32_t physical = -1;
    int32_t driver = -1;
    auto result = api.aclrtGetDevice(&user);
    if (result != ACL_SUCCESS) { return acl_failed(result, "aclrtGetDevice"); }
    result = api.aclrtGetPhyDevIdByLogicDevId(user, &physical);
    if (result != ACL_SUCCESS) { return acl_failed(result, "aclrtGetPhyDevIdByLogicDevId"); }
    result = api.aclrtGetLogicDevIdByUserDevId(user, &driver);
    if (result != ACL_SUCCESS) { return acl_failed(result, "aclrtGetLogicDevIdByUserDevId"); }
    if (user < 0 || physical < 0 || driver < 0) {
        return rollback(fail(hixl::FAILED, error, cap, "invalid ACL FabricMem device mapping"));
    }

    aclrtPhysicalMemProp prop{};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_MEM_P2P_HUGE1G;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    // Mooncake classic's A3 mapping: every four physical device IDs share an
    // even-numbered Fabric host NUMA node.
    prop.location.id = static_cast<uint32_t>((physical / 4) * 2);
    for (unsigned attempt = 0; ; ++attempt) {
        result = api.aclrtMallocPhysical(&mapping.physical, bytes, &prop, 0);
        if (result == ACL_SUCCESS) { break; }
        if (mapping.physical != nullptr) {
            return fail(kOwnershipUnknown, error, cap, "aclrtMallocPhysical failed with an ambiguous output handle");
        }
        if (attempt == 2 || !retry_page_policy(result)) {
            return acl_failed(result, "aclrtMallocPhysical");
        }
        prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
        prop.location.id = 0;
        if (attempt == 1) { prop.memAttr = ACL_MEM_P2P_HUGE; }
    }
    if (mapping.physical == nullptr) {
        return fail(kOwnershipUnknown, error, cap, "aclrtMallocPhysical returned a null handle");
    }
    result = api.aclrtReserveMemAddress(&mapping.base, bytes, 0, nullptr, 1);
    if (result != ACL_SUCCESS) {
        if (mapping.base != nullptr) {
            return fail(kOwnershipUnknown, error, cap, "aclrtReserveMemAddress failed with an ambiguous output address");
        }
        return acl_failed(result, "aclrtReserveMemAddress");
    }
    if (mapping.base == nullptr) {
        return fail(kOwnershipUnknown, error, cap, "aclrtReserveMemAddress returned null");
    }
    mapping.reserved = true;
    result = api.aclrtMapMem(mapping.base, bytes, 0, mapping.physical, 0);
    if (result != ACL_SUCCESS) { return acl_failed(result, "aclrtMapMem"); }
    mapping.mapped = true;

    aclrtMemAccessDesc desc{};
    desc.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    desc.location.id = static_cast<uint32_t>(driver);
    desc.flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
    result = api.aclrtMemSetAccess(mapping.base, bytes, &desc, 1);
    if (result != ACL_SUCCESS) { return acl_failed(result, "aclrtMemSetAccess"); }

    // External HIXL registration needs a real address range and retainable
    // handle from the running driver, including drivers with stub symbols.
    // HIXL RegisterMem still performs the actual export/import; the lane must
    // not be published until that call succeeds.
    void *base = nullptr;
    size_t extent = 0;
    result = api.aclrtMemGetAddressRange(mapping.base, &base, &extent);
    if (feature_unsupported(result)) {
        return rollback(fail(hixl::UNSUPPORTED, error, cap, "aclrtMemGetAddressRange cannot query external FabricMem allocations"));
    }
    if (result != ACL_SUCCESS) { return acl_failed(result, "aclrtMemGetAddressRange"); }
    if (base != mapping.base || extent != bytes ||
        extent > std::numeric_limits<uintptr_t>::max() - reinterpret_cast<uintptr_t>(base)) {
        return rollback(fail(hixl::UNSUPPORTED, error, cap, "ACL cannot report the mapped FabricMem allocation range"));
    }
    result = api.aclrtMemRetainAllocationHandle(mapping.base, &mapping.retained);
    if (result != ACL_SUCCESS) {
        if (mapping.retained != nullptr) {
            return fail(kOwnershipUnknown, error, cap, "aclrtMemRetainAllocationHandle failed with an ambiguous output handle");
        }
        if (feature_unsupported(result)) {
            return rollback(fail(hixl::UNSUPPORTED, error, cap, "ACL cannot retain external FabricMem allocations"));
        }
        return acl_failed(result, "aclrtMemRetainAllocationHandle");
    }
    if (mapping.retained == nullptr) {
        return fail(kOwnershipUnknown, error, cap, "aclrtMemRetainAllocationHandle returned null");
    }
    result = api.aclrtFreePhysical(mapping.retained);
    if (result != ACL_SUCCESS) {
        if (error != nullptr && cap != 0) {
            std::snprintf(error, cap, "aclrtFreePhysical(retained handle) failed with ACL status %d", result);
        }
        return kOwnershipUnknown;
    }
    mapping.retained = nullptr;
    *address = mapping.base;
    return hixl::SUCCESS;
}
#endif
}  // namespace afs_hixl_fabric
