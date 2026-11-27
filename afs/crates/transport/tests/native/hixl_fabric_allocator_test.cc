#include "../../native/hixl_fabric.h"

#include <hixl/hixl_types.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
#include <acl/acl_rt.h>
#endif

namespace {
const char *scenario;
std::vector<std::string> calls;
constexpr size_t extent = afs_hixl_fabric::kAllocationAlignment;
bool is(const char *name) { return std::strcmp(scenario, name) == 0; }
#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
void *const base = reinterpret_cast<void *>(0x10000000);
void *const physical = reinterpret_cast<void *>(0x20000000);
void *const retained = reinterpret_cast<void *>(0x30000000);
constexpr aclError denied = 505555;
constexpr aclError rt_feature_not_supported = 207000;
#endif
}

#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
extern "C" aclError aclrtGetDevice(int32_t *device) {
    *device = 2;
    return ACL_SUCCESS;
}
extern "C" aclError aclrtGetPhyDevIdByLogicDevId(int32_t device, int32_t *physical_id) {
    assert(device == 2);
    *physical_id = 9;
    return ACL_SUCCESS;
}
extern "C" aclError aclrtGetLogicDevIdByUserDevId(int32_t device, int32_t *driver_id) {
    assert(device == 2);
    *driver_id = 5;
    return ACL_SUCCESS;
}
extern "C" aclError aclrtMallocPhysical(aclrtDrvMemHandle *handle, size_t bytes,
                                        const aclrtPhysicalMemProp *prop, uint64_t flags) {
    assert(bytes == extent && flags == 0);
    if (prop->location.type == ACL_MEM_LOCATION_TYPE_HOST_NUMA) {
        assert(prop->location.id == 4 && prop->memAttr == ACL_MEM_P2P_HUGE1G);
        calls.emplace_back("numa1g");
        if (is("numa-success")) { *handle = physical; return ACL_SUCCESS; }
        if (is("physical-denied")) { return denied; }
        return ACL_ERROR_FEATURE_UNSUPPORTED;
    }
    assert(prop->location.type == ACL_MEM_LOCATION_TYPE_HOST && prop->location.id == 0);
    if (prop->memAttr == ACL_MEM_P2P_HUGE1G) {
        calls.emplace_back("host1g");
        return ACL_ERROR_BAD_ALLOC;
    }
    assert(prop->memAttr == ACL_MEM_P2P_HUGE);
    calls.emplace_back("host2m");
    *handle = physical;
    return ACL_SUCCESS;
}
extern "C" aclError aclrtReserveMemAddress(void **address, size_t bytes, size_t alignment,
                                           void *expected, uint64_t flags) {
    assert(bytes == extent && alignment == 0 && expected == nullptr && flags == 1);
    calls.emplace_back("reserve");
    if (is("reserve-failure")) { return denied; }
    *address = base;
    return ACL_SUCCESS;
}
extern "C" aclError aclrtMapMem(void *address, size_t bytes, size_t offset,
                                aclrtDrvMemHandle handle, uint64_t flags) {
    assert(address == base && bytes == extent && offset == 0 && handle == physical && flags == 0);
    calls.emplace_back("map");
    return is("map-failure") ? denied : ACL_SUCCESS;
}
extern "C" aclError aclrtMemSetAccess(void *address, size_t bytes, aclrtMemAccessDesc *desc, size_t count) {
    assert(address == base && bytes == extent && count == 1);
    assert(desc->location.type == ACL_MEM_LOCATION_TYPE_DEVICE && desc->location.id == 5);
    assert(desc->flags == ACL_RT_MEM_ACCESS_FLAGS_READWRITE);
    calls.emplace_back("access");
    return is("access-failure") || is("rollback-stuck") ? denied : ACL_SUCCESS;
}
extern "C" aclError aclrtMemGetAddressRange(void *address, void **out, size_t *bytes) {
    assert(address == base);
    calls.emplace_back("range");
    if (is("range-unsupported") || is("range-rollback-stuck")) { return ACL_ERROR_FEATURE_UNSUPPORTED; }
    if (is("range-rt-unsupported")) { return rt_feature_not_supported; }
    if (is("range-denied")) { return denied; }
    *out = base;
    *bytes = is("short-range") ? extent / 2 : extent;
    return ACL_SUCCESS;
}
#ifndef AFS_FABRIC_TEST_WITHOUT_RETAIN
extern "C" aclError aclrtMemRetainAllocationHandle(void *address, aclrtDrvMemHandle *handle) {
    assert(address == base);
    calls.emplace_back("retain");
    if (is("retain-failure")) { return ACL_ERROR_FEATURE_UNSUPPORTED; }
    *handle = retained;
    return ACL_SUCCESS;
}
#endif
extern "C" aclError aclrtUnmapMem(void *address) {
    assert(address == base);
    calls.emplace_back("unmap");
    return is("rollback-stuck") || is("range-rollback-stuck") ? denied : ACL_SUCCESS;
}
extern "C" aclError aclrtReleaseMemAddress(void *address) {
    assert(address == base);
    const bool fail_once = is("release-retry") && calls.back() == "unmap";
    calls.emplace_back("release");
    return fail_once ? denied : ACL_SUCCESS;
}
extern "C" aclError aclrtFreePhysical(aclrtDrvMemHandle handle) {
    assert(handle == physical || handle == retained);
    calls.emplace_back(handle == retained ? "drop-retain" : "free");
    return handle == retained && is("drop-retain-failure") ? denied : ACL_SUCCESS;
}

#endif

int main(int argc, char **argv) {
    assert(argc == 2);
    scenario = argv[1];
    char error[512]{};
    afs_hixl_fabric::Provider provider;
    const auto opened = provider.open(error, sizeof(error));
    if (is("broken-adxl-library")) {
        assert(opened == hixl::FAILED && calls.empty());
        return 0;
    }
    if (is("missing-retain") || is("no-vmm-headers")) {
        assert(opened == hixl::UNSUPPORTED && calls.empty());
        return 0;
    }
    if (opened != hixl::SUCCESS) { std::fprintf(stderr, "%s\n", error); }
    assert(opened == hixl::SUCCESS);
    void *address = nullptr;
    const auto status = provider.allocate(extent, &address, error, sizeof(error));
    if (is("adxl-success") || is("adxl-free-failure")) {
        assert(status == hixl::SUCCESS && address == reinterpret_cast<void *>(0x40000000));
        assert(provider.free(address, error, sizeof(error)) == (is("adxl-success") ? hixl::SUCCESS : 203900U));
        assert(calls.empty());
        return 0;
    }
    if (is("adxl-alloc-failure") || is("adxl-alloc-exception")) {
        assert(status == (is("adxl-alloc-failure") ? 203900U : afs_hixl_fabric::kOwnershipUnknown));
        assert(address == nullptr && calls.empty());
        return 0;
    }
#ifdef AFS_HIXL_HAVE_ACL_FABRIC_VMM
    std::vector<std::string> expected{"numa1g"};
    if (is("physical-denied")) {
        assert(status == denied && address == nullptr && calls == expected);
        return 0;
    }
    if (!is("numa-success")) { expected.insert(expected.end(), {"host1g", "host2m"}); }
    expected.emplace_back("reserve");
    if (is("reserve-failure")) {
        expected.emplace_back("free");
        assert(status == denied && address == nullptr && calls == expected);
        return 0;
    }
    expected.emplace_back("map");
    if (is("map-failure")) {
        expected.insert(expected.end(), {"release", "free"});
        assert(status == denied && address == nullptr && calls == expected);
        return 0;
    }
    expected.emplace_back("access");
    if (is("rollback-stuck")) {
        expected.emplace_back("unmap");
        assert(status == afs_hixl_fabric::kOwnershipUnknown && address == nullptr && calls == expected);
        return 0;
    }
    if (is("access-failure")) {
        expected.insert(expected.end(), {"unmap", "release", "free"});
        assert(status == denied && address == nullptr && calls == expected);
        return 0;
    }
    expected.emplace_back("range");
    if (is("range-rollback-stuck")) {
        expected.emplace_back("unmap");
        assert(status == afs_hixl_fabric::kOwnershipUnknown && address == nullptr && calls == expected);
        assert(std::strstr(error, "aclrtUnmapMem") != nullptr);
        return 0;
    }
    if (is("range-unsupported") || is("range-rt-unsupported") || is("range-denied")) {
        expected.insert(expected.end(), {"unmap", "release", "free"});
        assert(status == (is("range-denied") ? denied : hixl::UNSUPPORTED));
        assert(address == nullptr && calls == expected);
        assert(std::strstr(error, "aclrtMemGetAddressRange") != nullptr);
        return 0;
    }
    if (!is("short-range")) { expected.emplace_back("retain"); }
    if (is("short-range") || is("retain-failure")) {
        expected.insert(expected.end(), {"unmap", "release", "free"});
        assert(status == hixl::UNSUPPORTED && address == nullptr && calls == expected);
        return 0;
    }
    expected.emplace_back("drop-retain");
    if (is("drop-retain-failure")) {
        assert(status == afs_hixl_fabric::kOwnershipUnknown && address == nullptr && calls == expected);
        return 0;
    }
    assert(status == hixl::SUCCESS && address == base);
    if (is("release-retry")) {
        expected.insert(expected.end(), {"unmap", "release"});
        assert(provider.free(base, error, sizeof(error)) == denied && calls == expected);
        expected.insert(expected.end(), {"release", "free"});
    } else {
        expected.insert(expected.end(), {"unmap", "release", "free"});
    }
    assert(provider.free(base, error, sizeof(error)) == hixl::SUCCESS && calls == expected);
#else
    std::fprintf(stderr, "unexpected scenario without ACL VMM: %s\n", scenario);
    return 1;
#endif
}
