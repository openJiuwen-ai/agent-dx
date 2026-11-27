// Shared declaration probe for the production module and native allocator gate.
#include <acl/acl_rt.h>
using GetDevice = decltype(&aclrtGetDevice);
using GetPhysicalDevice = decltype(&aclrtGetPhyDevIdByLogicDevId);
using GetLogicalDevice = decltype(&aclrtGetLogicDevIdByUserDevId);
using Allocate = decltype(&aclrtMallocPhysical);
using Reserve = decltype(&aclrtReserveMemAddress);
using Map = decltype(&aclrtMapMem);
using SetAccess = decltype(&aclrtMemSetAccess);
using GetRange = decltype(&aclrtMemGetAddressRange);
using Retain = decltype(&aclrtMemRetainAllocationHandle);
using Unmap = decltype(&aclrtUnmapMem);
using Release = decltype(&aclrtReleaseMemAddress);
using Free = decltype(&aclrtFreePhysical);
void probe() {
    aclrtPhysicalMemProp prop{};
    prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
    prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
    prop.memAttr = ACL_MEM_P2P_HUGE1G;
    prop.memAttr = ACL_MEM_P2P_HUGE;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST_NUMA;
    prop.location.type = ACL_MEM_LOCATION_TYPE_HOST;
    aclrtMemAccessDesc desc{};
    desc.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
    desc.flags = ACL_RT_MEM_ACCESS_FLAGS_READWRITE;
    (void)prop;
    (void)desc;
}
