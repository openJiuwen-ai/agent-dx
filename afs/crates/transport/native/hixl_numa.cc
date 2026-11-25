#include "hixl_numa.h"

#include <acl/acl_rt.h>
#include <linux/mempolicy.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <array>
#include <charconv>
#include <cstdio>
#include <filesystem>
#include <fstream>

#ifndef AFS_HIXL_TEST_FAULT_INJECTION
#include <dlfcn.h>
#include <driver/dsmi_common_interface.h>
#endif

namespace afs_hixl_numa {
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
const char *pci_root();
const char *cpu_root();
bool owner_cpus(cpu_set_t &mask);
bool pci_address(int32_t logic_id, unsigned &bus, unsigned &device, unsigned &function);
long get_policy(int *mode);
long set_policy(int mode, const unsigned long *mask, unsigned long maxnode);
#else
static const char *pci_root() { return "/sys/bus/pci/devices"; }
static const char *cpu_root() { return "/sys/devices/system/cpu"; }
static bool owner_cpus(cpu_set_t &mask) {
    // pid 0 selects the calling lane thread, including its cpuset restrictions.
    return sched_getaffinity(0, sizeof(mask), &mask) == 0;
}

static bool pci_address(int32_t logic_id, unsigned &bus, unsigned &device, unsigned &function) {
    // DSMI is supplied by the installed host driver, not the build-time SDK.
    void *library = dlopen("libdrvdsmi_host.so", RTLD_LAZY | RTLD_LOCAL);
    if (!library) {
        library = dlopen("/usr/local/Ascend/driver/lib64/driver/libdrvdsmi_host.so", RTLD_LAZY | RTLD_LOCAL);
    }
    if (!library) { return false; }
    const auto query = reinterpret_cast<decltype(&dsmi_get_pcie_info)>(dlsym(library, "dsmi_get_pcie_info"));
    tag_pcie_idinfo info{};
    const bool ok = query && query(logic_id, &info) == 0;
    dlclose(library);
    bus = info.bdf_busid;
    device = info.bdf_deviceid;
    function = info.bdf_funcid;
    return ok;
}

static long get_policy(int *mode) {
    return syscall(SYS_get_mempolicy, mode, nullptr, 0UL, nullptr, 0UL);
}
static long set_policy(int mode, const unsigned long *mask, unsigned long maxnode) {
    return syscall(SYS_set_mempolicy, mode, mask, maxnode);
}
#endif

constexpr unsigned kMaxNodes = 4096;

static int pci_node(int32_t user_device) {
    int32_t logic_id = -1;
    unsigned bus, device, function;
    if (aclrtGetLogicDevIdByUserDevId(user_device, &logic_id) != ACL_SUCCESS || logic_id < 0 ||
        !pci_address(logic_id, bus, device, function) || bus > 255 || device > 31 || function > 7) {
        return -1;
    }
    // DSMI omits the PCI domain. Never assume domain 0000 or use an ambiguous BDF.
    char suffix[10];
    std::snprintf(suffix, sizeof(suffix), ":%02x:%02x.%x", bus, device, function);
    std::filesystem::path match;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(pci_root(), ec), end; !ec && it != end; it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (name.size() == 12 && name.compare(4, 8, suffix) == 0) {
            if (!match.empty()) { return -1; }
            match = it->path();
        }
    }
    if (ec || match.empty()) { return -1; }
    std::ifstream value(match / "numa_node");
    int node;
    char extra;
    return (value >> node) && !(value >> extra) && node >= 0 && node < static_cast<int>(kMaxNodes) ? node : -1;
}

static int cpu_node(int cpu) {
    const auto path = std::filesystem::path(cpu_root()) / ("cpu" + std::to_string(cpu));
    std::error_code ec;
    int node = -1;
    for (std::filesystem::directory_iterator it(path, ec), end; !ec && it != end; it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (name.compare(0, 4, "node") != 0) { continue; }
        int id;
        const char *last = name.data() + name.size();
        const auto parsed = std::from_chars(name.data() + 4, last, id);
        if (parsed.ec != std::errc{} || parsed.ptr != last || id < 0 || id >= static_cast<int>(kMaxNodes) ||
            node >= 0 || !it->is_directory(ec)) {
            return -1;
        }
        node = id;
    }
    return ec ? -1 : node;
}

HostNode preferred_node(int32_t user_device) {
    const int device_node = pci_node(user_device);
    if (device_node >= 0) { return {device_node, "pci"}; }

    cpu_set_t mask{};
    if (owner_cpus(mask)) {
        int shared_node = -1;
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (!CPU_ISSET(cpu, &mask)) { continue; }
            const int node = cpu_node(cpu);
            if (node < 0 || (shared_node >= 0 && node != shared_node)) { return {-1, "unknown"}; }
            shared_node = node;
        }
        if (shared_node >= 0) { return {shared_node, "worker_cpu"}; }
    }
    return {-1, "unknown"};
}

ScopedPreference::ScopedPreference(int node) {
    int mode;
    if (node < 0 || node >= static_cast<int>(kMaxNodes) || get_policy(&mode) != 0 || mode != MPOL_DEFAULT) { return; }
    constexpr unsigned bits = sizeof(unsigned long) * 8;
    std::array<unsigned long, kMaxNodes / bits + 1> mask{};
    mask[node / bits] = 1UL << (node % bits);
    // Linux subtracts one from maxnode before copying the mask. Include bit 'node'.
    applied_ = set_policy(MPOL_PREFERRED, mask.data(), static_cast<unsigned long>(node) + 2) == 0;
}

bool ScopedPreference::restore() {
    if (applied_ && set_policy(MPOL_DEFAULT, nullptr, 0) != 0) { return false; }
    applied_ = false;
    return true;
}

ScopedPreference::~ScopedPreference() { restore(); }
}  // namespace afs_hixl_numa
