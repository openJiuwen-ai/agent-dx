#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace adxl {
enum MemType { MEM_DEVICE, MEM_HOST };
class AdxlEngine {
public:
    static uint32_t MallocMem(MemType type, size_t bytes, void **address);
    static uint32_t FreeMem(void *address);
};

uint32_t AdxlEngine::MallocMem(MemType type, size_t bytes, void **address) {
    if (type != MEM_HOST || bytes != 2U * 1024U * 1024U) { std::abort(); }
    const char *scenario = std::getenv("AFS_FABRIC_ALLOCATOR_SCENARIO");
    if (scenario == nullptr) { std::abort(); }
    if (std::strcmp(scenario, "adxl-alloc-failure") == 0) { return 203900U; }
    if (std::strcmp(scenario, "adxl-alloc-exception") == 0) { throw 1; }
    *address = reinterpret_cast<void *>(0x40000000);
    return 0;
}

#ifndef AFS_FABRIC_TEST_PARTIAL_ADXL
uint32_t AdxlEngine::FreeMem(void *address) {
    if (address != reinterpret_cast<void *>(0x40000000)) { std::abort(); }
    const char *scenario = std::getenv("AFS_FABRIC_ALLOCATOR_SCENARIO");
    if (scenario == nullptr) { std::abort(); }
    return std::strcmp(scenario, "adxl-free-failure") == 0 ? 203900U : 0;
}
#endif
}  // namespace adxl
