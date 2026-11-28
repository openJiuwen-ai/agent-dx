// Keep the base HIXL module independent of optional ADXL entrypoints. The
// classic ADXL allocator records the original VA for HIXL RegisterMem.
#include "hixl_fabric.h"
#include <dlfcn.h>
#include <cstring>

#ifdef AFS_HIXL_HAVE_ADXL_HEADER
#include <adxl/adxl_engine.h>
#else
// Public ADXL ABI from adxl/adxl_types.h in CANN 9.0.0 and later.
namespace adxl {
enum MemType { MEM_DEVICE, MEM_HOST };
}
#endif

namespace {
using Allocate = uint32_t (*)(adxl::MemType, size_t, void **);
using Free = uint32_t (*)(void *);

struct AdxlApi {
    void *library = nullptr;
    Allocate allocate = nullptr;
    Free release = nullptr;
    bool broken = false;

    AdxlApi() noexcept {
        library = dlopen("libllm_datadist.so", RTLD_NOW | RTLD_LOCAL);
        if (library == nullptr) {
            const char *reason = dlerror();
            // A missing dependency or an ABI/loader error is not evidence that
            // the vendor pair is absent. Fail closed in those cases.
            constexpr char missing[] = "libllm_datadist.so: cannot open shared object file: No such file or directory";
            broken = reason == nullptr || std::strcmp(reason, missing) != 0;
            return;
        }
        // Public Itanium C++ ABI spellings (CANN Linux aarch64 and x86_64).
        allocate = reinterpret_cast<Allocate>(dlsym(library, "_ZN4adxl10AdxlEngine9MallocMemENS_7MemTypeEmPPv"));
        release = reinterpret_cast<Free>(dlsym(library, "_ZN4adxl10AdxlEngine7FreeMemEPv"));
    }
    ~AdxlApi() {
        if (library != nullptr) { dlclose(library); }
    }
};

const AdxlApi &api() noexcept {
    static const AdxlApi loaded;
    return loaded;
}
}

// Positive: the complete pair is usable; zero: absent; negative: broken loader.
extern "C" int afs_fabric_adxl_available_v1() noexcept {
    const auto &loaded = api();
    return loaded.broken ? -1 : (loaded.allocate != nullptr && loaded.release != nullptr ? 1 : 0);
}

extern "C" uint32_t afs_fabric_alloc_v1(size_t bytes, void **address) noexcept {
    try {
        const auto &loaded = api();
        if (loaded.allocate == nullptr || loaded.release == nullptr) { return afs_hixl_fabric::kOwnershipUnknown; }
        return loaded.allocate(adxl::MEM_HOST, bytes, address);
    } catch (...) {
        return afs_hixl_fabric::kOwnershipUnknown;
    }
}

extern "C" uint32_t afs_fabric_free_v1(void *address) noexcept {
    try {
        const auto &loaded = api();
        if (loaded.allocate == nullptr || loaded.release == nullptr) { return afs_hixl_fabric::kOwnershipUnknown; }
        return loaded.release(address);
    } catch (...) {
        return afs_hixl_fabric::kOwnershipUnknown;
    }
}
