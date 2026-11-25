// LD_PRELOAD fixture for an idle Node. The production C++ bridge still owns
// allocation, registration and shutdown ordering. There is no device or peer.
#include <acl/acl.h>
#include <acl/acl_rt.h>
#include <hixl/hixl.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

namespace {
void mark(const char *name) {
    FILE *file = std::fopen(name, "w");
    if (file == nullptr || std::fclose(file) != 0) {
        std::abort();
    }
}

void block_at_exit() {
    mark("c-exit-entered");
    for (;;) {
        pause();
    }
}
}  // namespace

aclError aclInit(const char *) {
    const char *mode = std::getenv("AFS_TEST_HIXL_SHUTDOWN");
    if (mode != nullptr && std::strcmp(mode, "exit-blocked") == 0 && std::atexit(block_at_exit) != 0) {
        std::abort();
    }
    return ACL_SUCCESS;
}

aclError aclFinalize() {
    mark("native-entered");
    while (access("native-unblock", F_OK) != 0) {
        usleep(10000);
    }
    mark("native-finished");
    return ACL_SUCCESS;
}

aclError aclrtGetLogicDevIdByUserDevId(int32_t, int32_t *) { return ACL_ERROR_FEATURE_UNSUPPORTED; }
aclError aclrtSetDevice(int32_t) { return ACL_SUCCESS; }
aclError aclrtResetDevice(int32_t) { return ACL_SUCCESS; }
aclError aclrtSetCurrentContext(aclrtContext) { return ACL_SUCCESS; }

aclError aclrtCreateContext(aclrtContext *context, int32_t) {
    *context = std::malloc(1);
    return *context == nullptr ? ACL_ERROR_BAD_ALLOC : ACL_SUCCESS;
}

aclError aclrtDestroyContext(aclrtContext context) {
    std::free(context);
    return ACL_SUCCESS;
}

aclError aclrtMallocHost(void **ptr, size_t length) {
    *ptr = std::malloc(length);
    return *ptr == nullptr ? ACL_ERROR_BAD_ALLOC : ACL_SUCCESS;
}

aclError aclrtFreeHost(void *ptr) {
    std::free(ptr);
    return ACL_SUCCESS;
}

namespace hixl {
Status Hixl::Initialize(const AscendString &, const std::map<AscendString, AscendString> &) { return SUCCESS; }
void Hixl::Finalize() {}

Status Hixl::RegisterMem(const MemDesc &memory, MemType, MemHandle &handle) {
    handle = reinterpret_cast<MemHandle>(memory.addr);
    return SUCCESS;
}

Status Hixl::DeregisterMem(MemHandle) { return SUCCESS; }
}  // namespace hixl
