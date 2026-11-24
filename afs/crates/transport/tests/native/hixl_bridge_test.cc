// Exercises the actual C ABI bridge against a fake external CANN boundary.
// Official SDK headers remain the ABI contract; no device is initialized.
#include "../../native/hixl.h"
#include <acl/acl_rt.h>
#include <hixl/hixl.h>
#include <cassert>
#include <cerrno>
#include <climits>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <linux/mempolicy.h>
#include <sched.h>
#include <set>
#include <string>
#include <stdexcept>
#include <sys/resource.h>
#include <thread>
#include <vector>

static_assert(sizeof(afs_hixl_batch_window_t) == 3 * sizeof(uint64_t),
              "batch ABI carries only slot, remote address and length");

namespace {
size_t allocations = 0;
bool device_roce = false;
bool write_mode = false;
std::set<uintptr_t> registered_addresses;
size_t registrations = 0;
bool retain_failed_start = false;
bool relay_mode = false;
uint32_t sync_status = 0;
size_t sync_calls = 0;
size_t finalizations = 0;
size_t acl_init_calls = 0;
size_t acl_finalize_calls = 0;
size_t device_resets[16] = {};
bool external_acl_init = false;
bool fail_device_reset = false;
bool fail_acl_finalize = false;
bool throw_sync = false;
bool batch_mode = false;
size_t batch_descriptors = 0;
size_t batch_bytes = 0;
const char *soc_name = "Ascend910_9391";
bool fail_domain = false;
int64_t domain_id = 7;
uint32_t expected_listen_port = 0;

const char *numa_pci_root = nullptr;
const char *numa_cpu_root = nullptr;
cpu_set_t numa_owner_mask;
bool numa_owner_mask_available = false;
size_t owner_cpus_calls = 0;
constexpr int32_t numa_user_id = 2;
constexpr int32_t numa_logic_id = 11;
unsigned expected_numa_node = 3;
size_t user_to_logic_calls = 0;
size_t pci_address_calls = 0;
size_t preferred_attempts = 0;
size_t default_restores = 0;
size_t fail_host_alloc_at = 0;
bool reject_preferred = false;
thread_local int current_policy = MPOL_DEFAULT;
int initial_policy = MPOL_DEFAULT;
std::vector<int> host_alloc_policies;
}

namespace afs_hixl_numa {
const char *pci_root() { return numa_pci_root; }
const char *cpu_root() { return numa_cpu_root; }

bool owner_cpus(cpu_set_t &mask) {
    ++owner_cpus_calls;
    if (!numa_owner_mask_available) { return false; }
    mask = numa_owner_mask;
    return true;
}

bool pci_address(int32_t logic_id, unsigned &bus, unsigned &device, unsigned &function) {
    assert(numa_pci_root != nullptr);
    assert(logic_id == numa_logic_id);
    ++pci_address_calls;
    bus = 0x3a;
    device = 0x0b;
    function = 2;
    return true;
}

long get_policy(int *mode) {
    assert(numa_pci_root != nullptr);
    *mode = current_policy;
    return 0;
}

long set_policy(int mode, const unsigned long *mask, unsigned long maxnode) {
    assert(numa_pci_root != nullptr);
    if (mode == MPOL_PREFERRED) {
        assert(current_policy == MPOL_DEFAULT);
        assert(mask != nullptr && maxnode > expected_numa_node);
        constexpr unsigned long word_bits = CHAR_BIT * sizeof(unsigned long);
        const unsigned long words = 1 + (maxnode - 1) / word_bits;
        for (unsigned long i = 0; i < words; ++i) {
            const unsigned long expected = i == expected_numa_node / word_bits
                ? 1UL << (expected_numa_node % word_bits) : 0;
            assert(mask[i] == expected);
        }
        ++preferred_attempts;
        if (reject_preferred) {
            errno = EPERM;
            return -1;
        }
    } else {
        assert(mode == MPOL_DEFAULT && mask == nullptr && maxnode == 0);
        assert(current_policy == MPOL_PREFERRED);
        ++default_restores;
    }
    current_policy = mode;
    return 0;
}
} // namespace afs_hixl_numa

extern "C" aclError aclrtGetLogicDevIdByUserDevId(int32_t user_id, int32_t *logic_id) {
    // All existing non-NUMA scenarios simulate unavailable topology.
    if (numa_pci_root == nullptr) { return 1; }
    assert(user_id == numa_user_id);
    ++user_to_logic_calls;
    *logic_id = numa_logic_id;
    return ACL_SUCCESS;
}

extern "C" aclError aclInit(const char *) {
    ++acl_init_calls;
    return external_acl_init ? ACL_ERROR_REPEAT_INITIALIZE : ACL_SUCCESS;
}
extern "C" aclError aclFinalize() {
    ++acl_finalize_calls;
    if (fail_acl_finalize) { fail_acl_finalize = false; return 1; }
    return ACL_SUCCESS;
}
extern "C" aclError aclrtSetDevice(int32_t) { return ACL_SUCCESS; }
extern "C" aclError aclrtResetDevice(int32_t device_id) {
    assert(device_id >= 0 && device_id < 16);
    ++device_resets[device_id];
    return fail_device_reset ? 1 : ACL_SUCCESS;
}
extern "C" aclError aclrtCreateContext(aclrtContext *context, int32_t) { *context = reinterpret_cast<void *>(1); return ACL_SUCCESS; }
extern "C" aclError aclrtSetCurrentContext(aclrtContext) { return ACL_SUCCESS; }
extern "C" aclError aclrtDestroyContext(aclrtContext) { return ACL_SUCCESS; }
extern "C" const char *aclrtGetSocName() { return soc_name; }
extern "C" aclError aclrtGetDeviceInfo(uint32_t, aclrtDevAttr attr, int64_t *value) {
    assert(attr == ACL_DEV_ATTR_SUPER_POD_ID);
    if (fail_domain) { return 1; }
    *value = domain_id; return ACL_SUCCESS;
}
extern "C" aclError aclrtMallocHost(void **address, size_t length) {
    if (numa_pci_root != nullptr) {
        host_alloc_policies.push_back(current_policy);
        if (host_alloc_policies.size() == fail_host_alloc_at) {
            *address = nullptr;
            return 1;
        }
    }
    *address = std::malloc(length);
    assert(*address != nullptr);
    std::memset(*address, 0xa5, length);
    ++allocations;
    return ACL_SUCCESS;
}
extern "C" aclError aclrtFreeHost(void *address) { std::free(address); --allocations; return ACL_SUCCESS; }

namespace hixl {
class Hixl::HixlImpl {};
Hixl::Hixl() = default;
Hixl::~Hixl() = default;
Status Hixl::Initialize(const AscendString &, const std::map<AscendString, AscendString> &options) {
    assert(std::string(options.at(AscendString(OPTION_AUTO_CONNECT)).GetString()) == "0");
    assert(options.count(AscendString(OPTION_BUFFER_POOL)) == 1);
    const auto pool = std::string(options.at(AscendString(OPTION_BUFFER_POOL)).GetString());
    assert(pool == (relay_mode ? "4:8" : "0:0"));
    auto global = options.find(AscendString(OPTION_GLOBAL_RESOURCE_CONFIG));
    // protocol_desc would select HixlCS, whose async/direct path cannot relay.
    if (relay_mode) {
        const auto *inter = std::getenv("HCCL_INTER_HCCS_DISABLE");
        if (inter != nullptr && std::strcmp(inter, "TRUE") == 0) {
            assert(std::strcmp(std::getenv("HCCL_INTRA_ROCE_ENABLE"), "1") == 0);
            assert(std::strcmp(std::getenv("HCCL_INTRA_PCIE_ENABLE"), "0") == 0);
        }
        if (expected_listen_port == 0) {
            assert(global == options.end());
        } else {
            const std::string expected =
                "{\"comm_resource_config.listen_port\":\"" + std::to_string(expected_listen_port) + "\"}";
            assert(global != options.end());
            assert(std::string(global->second.GetString()) == expected);
        }
    }
    // The official parser reads literal dotted keys, rather than nested JSON.
    if (!relay_mode) {
        const std::string expected = expected_listen_port == 0
            ? "{\"comm_resource_config.protocol_desc\":[\"roce:device\"]}"
            : "{\"comm_resource_config.protocol_desc\":[\"roce:device\"],\"comm_resource_config.listen_port\":\"" +
                  std::to_string(expected_listen_port) + "\"}";
        assert(global != options.end());
        assert(std::string(global->second.GetString()) == expected);
        device_roce = true;
    }
    return SUCCESS;
}
void Hixl::Finalize() { ++finalizations; }
Status Hixl::RegisterMem(const MemDesc &desc, MemType type, MemHandle &handle) {
    assert(current_policy == initial_policy && "HIXL RegisterMem must not inherit the slot allocation policy");
    ++registrations;
    assert(!relay_mode);
    if (retain_failed_start && registrations == 2) { return UNSUPPORTED; }
    assert(type == MEM_HOST); assert(desc.addr != 0);
    registered_addresses.insert(desc.addr);
    handle = reinterpret_cast<void *>(desc.addr); return SUCCESS;
}
Status Hixl::DeregisterMem(MemHandle) { return retain_failed_start ? FAILED : SUCCESS; }
Status Hixl::Connect(const AscendString &, int32_t) { return SUCCESS; }
Status Hixl::Disconnect(const AscendString &, int32_t) { return SUCCESS; }
Status Hixl::TransferSync(const AscendString &, TransferOp operation, const std::vector<TransferOpDesc> &ops, int32_t timeout) {
    assert(operation == (write_mode ? WRITE : READ)); assert(!ops.empty());
    assert(timeout > 0 && timeout <= 123);
    for (const auto &op : ops) {
        assert(registered_addresses.count(op.local_addr) == (relay_mode ? 0 : 1));
        if (write_mode) { assert(registered_addresses.count(op.remote_addr) == (relay_mode ? 0 : 1)); }
    }
    if (batch_mode) {
        batch_descriptors = ops.size();
        batch_bytes = 0;
        for (const auto &op : ops) { batch_bytes += op.len; }
    } else {
        assert(ops.size() == 1);
    }
    ++sync_calls;
    if (throw_sync) { throw std::runtime_error("uncertain native submission"); }
    if (sync_status != SUCCESS) { return sync_status; }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    for (const auto &op : ops) {
        if (write_mode) {
            std::memcpy(reinterpret_cast<void *>(op.remote_addr), reinterpret_cast<void *>(op.local_addr), op.len);
        } else {
            std::memcpy(reinterpret_cast<void *>(op.local_addr), reinterpret_cast<void *>(op.remote_addr), op.len);
        }
    }
    return SUCCESS;
}
}

// The Rust side copies pinned slots itself; these helpers do the same so each
// scenario can keep asserting on the bytes a caller ends up with.
static void copy_and_release(void *runtime, const afs_hixl_batch_window_t *windows, size_t count,
                             const std::vector<uint64_t> &addresses, uint8_t *output, size_t output_len,
                             uint64_t *output_copy_ns, char *error, size_t error_len) {
    const auto copy_start = std::chrono::steady_clock::now();
    std::vector<uint64_t> slot_ids(count);
    size_t copied_bytes = 0;
    for (size_t index = 0; index < count; ++index) {
        assert(windows[index].length <= output_len - copied_bytes);
        std::memcpy(output + copied_bytes,
                    reinterpret_cast<const void *>(static_cast<uintptr_t>(addresses[index])), windows[index].length);
        copied_bytes += windows[index].length;
        slot_ids[index] = windows[index].slot_id;
    }
    assert(copied_bytes == output_len);
    const auto copied = std::chrono::steady_clock::now() - copy_start;
    *output_copy_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(copied).count());
    assert(afs_hixl_release_receives(runtime, slot_ids.data(), count, error, error_len) == 0);
}

static uint32_t read_sync_into(void *runtime, const char *peer, const afs_hixl_batch_window_t *windows, size_t count,
                               uint8_t *output, size_t output_len, uint32_t timeout_ms, uint32_t *dispatch_started,
                               uint64_t *sdk_transfer_ns, uint64_t *output_copy_ns, char *error, size_t error_len) {
    std::vector<uint64_t> addresses(count + 1);
    *output_copy_ns = 0;
    const uint32_t status = afs_hixl_read_sync_batch(runtime, peer, windows, count, addresses.data(), timeout_ms,
                                                     dispatch_started, sdk_transfer_ns, error, error_len);
    if (status == 0) {
        copy_and_release(runtime, windows, count, addresses, output, output_len, output_copy_ns, error, error_len);
    }
    return status;
}

static uint32_t read_sync_one(void *runtime, uint64_t slot_id, const char *peer, uint64_t remote_addr, uint8_t *output,
                              size_t output_len, uint32_t timeout_ms, uint32_t *dispatch_started,
                              uint64_t *sdk_transfer_ns, uint64_t *output_copy_ns, char *error, size_t error_len) {
    afs_hixl_batch_window_t window{slot_id, remote_addr, output_len};
    return read_sync_into(runtime, peer, &window, 1, output, output_len, timeout_ms, dispatch_started,
                          sdk_transfer_ns, output_copy_ns, error, error_len);
}

int main(int argc, char **argv) {
    assert(argc >= 2);
    rlimit no_core{0, 0}; assert(setrlimit(RLIMIT_CORE, &no_core) == 0);
    // Each fake-boundary scenario owns its HCCL inputs independently of the
    // machine's real transport settings. Conflict scenarios set them below.
    assert(unsetenv("HCCL_INTRA_ROCE_ENABLE") == 0);
    assert(unsetenv("HCCL_INTRA_PCIE_ENABLE") == 0);
    assert(unsetenv("HCCL_INTER_HCCS_DISABLE") == 0);
    char error[512] = {};
    void *runtime = nullptr;
    uint64_t actual_domain = 0;
    const std::string scenario(argv[1]);
    const bool roce_pool = scenario.find("roce-pool-") == 0;
    const bool hccs = scenario.find("hccs-") == 0 || scenario == "numa-relay";
    relay_mode = hccs || roce_pool;
    if (roce_pool) {
        if (scenario != "roce-pool-missing-intra") {
            assert(setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1) == 0);
        }
        assert(setenv("HCCL_INTRA_PCIE_ENABLE", scenario == "roce-pool-pcie-conflict" ? "1" : "0", 1) == 0);
        if (scenario != "roce-pool-missing-inter") {
            assert(setenv("HCCL_INTER_HCCS_DISABLE", scenario == "roce-pool-hccs-conflict" ? "FALSE" : "TRUE", 1) == 0);
        }
        // RoCE pool does not require an A3 SoC or an HCCS domain.
        soc_name = "Ascend910B2";
        fail_domain = true;
    }
    if (scenario.find("numa-") == 0) {
        assert(argc >= 5);
        numa_pci_root = argv[2];
        numa_cpu_root = argv[3];
        numa_owner_mask_available = std::strcmp(argv[4], "mask") == 0;
        assert(numa_owner_mask_available || (std::strcmp(argv[4], "failure") == 0 && argc == 5));
        CPU_ZERO(&numa_owner_mask);
        for (int i = 5; i < argc; ++i) {
            const int cpu = std::stoi(argv[i]);
            assert(cpu >= 0 && cpu < CPU_SETSIZE);
            CPU_SET(cpu, &numa_owner_mask);
        }

        const bool pci_known = scenario == "numa-roce" || scenario == "numa-relay" ||
            scenario == "numa-non-default" || scenario == "numa-set-rejected" || scenario == "numa-alloc-failure";
        const bool worker_node_known = scenario == "numa-worker" || scenario == "numa-worker-non-default";
        if (worker_node_known) { expected_numa_node = 4; }
        reject_preferred = scenario == "numa-set-rejected";
        if (scenario == "numa-non-default" || scenario == "numa-worker-non-default") { initial_policy = MPOL_BIND; }
        const bool prefer = (pci_known || worker_node_known) && initial_policy == MPOL_DEFAULT && !reject_preferred;
        current_policy = initial_policy;
        if (scenario == "numa-alloc-failure") { fail_host_alloc_at = 2; }

        const auto status = afs_hixl_start(numa_user_id, "127.0.0.1:7920", 0, 4, 1, 1,
            hccs ? AFS_HIXL_TRANSPORT_HCCS : AFS_HIXL_TRANSPORT_ROCE,
            relay_mode ? 4 : 0, relay_mode ? 8 : 0, &actual_domain, &runtime, error, sizeof(error));
        if (fail_host_alloc_at != 0) {
            assert(status != 0 && runtime == nullptr && allocations == 0);
            assert(std::strstr(error, "aclrtMallocHost") != nullptr);
        } else {
            assert(status == 0 && runtime != nullptr && allocations == 2);
        }
        // Startup allocates one receive and one export slot. Even the failed
        // allocation must observe the desired policy and restore it afterward.
        const int expected = prefer ? MPOL_PREFERRED : initial_policy;
        assert((host_alloc_policies == std::vector<int>{expected, expected}) &&
               "both receive and export allocations must observe the expected policy");
        assert(current_policy == initial_policy);
        assert(user_to_logic_calls == 1 && pci_address_calls == 1);
        assert(owner_cpus_calls == (pci_known ? 0U : 1U) &&
               "only an unknown PCI node may trigger an owner CPU mask query");
        if (prefer) {
            assert(preferred_attempts == 2 && default_restores == 2);
        } else if (reject_preferred) {
            assert(preferred_attempts > 0 && default_restores == 0);
        } else {
            assert(preferred_attempts == 0 && default_restores == 0);
        }
        if (runtime != nullptr) {
            assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
            afs_hixl_destroy(runtime);
            assert(allocations == 0);
        }
        return 0;
    }
    assert(argc == 2);
    if (scenario == "roce-listen-port" || scenario == "hccs-listen-port" || scenario == "roce-pool-listen-port") { expected_listen_port = 27666; }
    if (scenario == "roce-max-listen-port") { expected_listen_port = 65535; }
    if (scenario == "roce-invalid-listen-port" || scenario == "hccs-invalid-listen-port") { expected_listen_port = 65536; }
    if (scenario == "hccs-unsupported-soc") { soc_name = "Ascend910B2"; }
    if (scenario == "hccs-null-soc") { soc_name = nullptr; }
    if (scenario == "hccs-unknown-domain") { fail_domain = true; }
    if (scenario == "hccs-negative-domain") { domain_id = -1; }
    if (scenario == "hccs-roce-conflict") { assert(setenv("HCCL_INTRA_ROCE_ENABLE", "1", 1) == 0); }
    if (scenario == "hccs-inter-roce-conflict") { assert(setenv("HCCL_INTER_HCCS_DISABLE", "TRUE", 1) == 0); }
    if (scenario == "hccs-completed") { assert(setenv("HCCL_INTER_HCCS_DISABLE", "FALSE", 1) == 0); }
    auto start = [&](size_t count = 4, size_t size_mib = 8) {
        return afs_hixl_start(0, "127.0.0.1:7920", expected_listen_port, 4, 1, 1,
            hccs ? AFS_HIXL_TRANSPORT_HCCS : AFS_HIXL_TRANSPORT_ROCE,
            relay_mode ? count : 0, relay_mode ? size_mib : 0, &actual_domain, &runtime, error, sizeof(error));
    };
    auto start_on = [&](int32_t device_id, const char *engine_id, uint32_t listen_port, void **out) {
        expected_listen_port = listen_port;
        return afs_hixl_start(device_id, engine_id, listen_port, 4, 1, 1,
            AFS_HIXL_TRANSPORT_ROCE, 0, 0, &actual_domain, out, error, sizeof(error));
    };
    if (scenario == "hccs-direct" || scenario == "hccs-partial-pool" || scenario == "roce-pool-partial") {
        assert(start(scenario == "hccs-direct" ? 0 : 4, 0) == hixl::PARAM_INVALID);
        assert(runtime == nullptr && allocations == 0 && acl_init_calls == 0);
        return 0;
    }
    if (scenario == "roce-pool-missing-intra" || scenario == "roce-pool-missing-inter" ||
        scenario == "roce-pool-pcie-conflict" || scenario == "roce-pool-hccs-conflict") {
        assert(start() == hixl::PARAM_INVALID);
        assert(runtime == nullptr && allocations == 0 && acl_init_calls == 0);
        return 0;
    }
    if (scenario == "hccs-invalid-pool" || scenario == "roce-invalid-listen-port" ||
        scenario == "hccs-invalid-listen-port") {
        assert(start(64, 8) != 0); assert(runtime == nullptr); assert(allocations == 0); return 0;
    }
    if (scenario == "hccs-unsupported-soc" || scenario == "hccs-null-soc" || scenario == "hccs-roce-conflict" ||
        scenario == "hccs-unknown-domain" || scenario == "hccs-negative-domain" || scenario == "hccs-inter-roce-conflict") {
        assert(start() != 0); assert(runtime == nullptr); assert(allocations == 0); return 0;
    }
    if (scenario == "reserve-export") {
        assert(start() == 0);
        afs_hixl_window_t window{};
        assert(afs_hixl_reserve_export(runtime, 0, 1, 0, &window, error, sizeof(error)) != 0);
        assert(afs_hixl_reserve_export(runtime, 0, 1, 5, &window, error, sizeof(error)) != 0);
        assert(afs_hixl_reserve_export(runtime, 0, 1, 3, nullptr, error, sizeof(error)) != 0);
        assert(afs_hixl_reserve_export(runtime, 0, 1, 3, &window, error, sizeof(error)) == 0);
        assert(window.slot_id == 0 && window.generation == 1 && window.length == 3);
        assert(window.address != 0);
        auto *bytes = reinterpret_cast<char *>(static_cast<uintptr_t>(window.address));
        assert(std::memcmp(bytes, "\0\0\0", 3) == 0);
        // The slot is the caller's until release: it can be written directly
        // and neither a second reservation nor shutdown may take it back.
        std::memcpy(bytes, "abc", 3);
        afs_hixl_window_t busy{};
        assert(afs_hixl_reserve_export(runtime, 0, 2, 3, &busy, error, sizeof(error)) != 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        assert(afs_hixl_release_export(runtime, 0, 1, error, sizeof(error)) == 0);
        assert(afs_hixl_reserve_export(runtime, 0, 2, 4, &busy, error, sizeof(error)) == 0);
        assert(busy.address == window.address && busy.length == 4);
        assert(std::memcmp(bytes, "abc\0", 4) == 0);
        assert(afs_hixl_release_export(runtime, 0, 2, error, sizeof(error)) == 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        return 0;
    }
    retain_failed_start = std::string(argv[1]) == "startup-cleanup-failed";
    if (retain_failed_start) {
        assert(start() == hixl::UNSUPPORTED);
        // A non-null failure handle signals retained resources, so preferred
        // mode must not mistake this for a safe capability refusal.
        assert(runtime != nullptr); assert(allocations == 1);
        assert(device_resets[0] == 0); assert(acl_finalize_calls == 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        return 0;
    }
    if (scenario == "shared-acl-device-leases") {
        void *first = nullptr;
        void *second = nullptr;
        assert(start_on(0, "127.0.0.1:7920", 27666, &first) == 0);
        assert(start_on(1, "127.0.0.1:7921", 27667, &second) == 0);
        assert(first != nullptr && second != nullptr && first != second);
        assert(acl_init_calls == 1);
        assert(afs_hixl_shutdown(first, error, sizeof(error)) == 0);
        afs_hixl_destroy(first);
        assert(device_resets[0] == 1 && device_resets[1] == 0);
        assert(acl_finalize_calls == 0);
        assert(afs_hixl_connect(second, "127.0.0.1:7922", 1000, error, sizeof(error)) == 0);
        assert(afs_hixl_shutdown(second, error, sizeof(error)) == 0);
        afs_hixl_destroy(second);
        assert(device_resets[1] == 1 && acl_finalize_calls == 1);
        return 0;
    }
    if (scenario == "shared-acl-same-device-leases") {
        void *first = nullptr;
        void *second = nullptr;
        assert(start_on(0, "127.0.0.1:7920", 27666, &first) == 0);
        assert(start_on(0, "127.0.0.1:7921", 27667, &second) == 0);
        assert(acl_init_calls == 1);
        assert(afs_hixl_shutdown(first, error, sizeof(error)) == 0);
        afs_hixl_destroy(first);
        assert(device_resets[0] == 0 && acl_finalize_calls == 0);
        assert(afs_hixl_connect(second, "127.0.0.1:7922", 1000, error, sizeof(error)) == 0);
        assert(afs_hixl_shutdown(second, error, sizeof(error)) == 0);
        afs_hixl_destroy(second);
        assert(device_resets[0] == 1 && acl_finalize_calls == 1);
        return 0;
    }
    if (scenario == "external-acl-init") {
        external_acl_init = true;
        assert(start_on(0, "127.0.0.1:7920", 27666, &runtime) == 0);
        assert(acl_init_calls == 1);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(device_resets[0] == 1 && acl_finalize_calls == 0);
        return 0;
    }
    if (scenario == "acl-reset-retry") {
        assert(start_on(0, "127.0.0.1:7920", 27666, &runtime) == 0);
        fail_device_reset = true;
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        assert(device_resets[0] == 1 && acl_finalize_calls == 0);
        fail_device_reset = false;
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(device_resets[0] == 2 && acl_finalize_calls == 1 && finalizations == 1);
        return 0;
    }
    if (scenario == "acl-finalize-retry") {
        assert(start_on(0, "127.0.0.1:7920", 27666, &runtime) == 0);
        fail_acl_finalize = true;
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        assert(device_resets[0] == 1 && acl_finalize_calls == 1);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(device_resets[0] == 1 && acl_finalize_calls == 2 && finalizations == 1);
        return 0;
    }
    if (scenario.find("-write-") != std::string::npos) {
        write_mode = true;
        batch_mode = true;
        constexpr char peer[] = "127.0.0.1:7921";
        assert(afs_hixl_start(0, "127.0.0.1:7920", 0, 4, 2, 2,
            hccs ? AFS_HIXL_TRANSPORT_HCCS : AFS_HIXL_TRANSPORT_ROCE,
            relay_mode ? 4 : 0, relay_mode ? 8 : 0, &actual_domain, &runtime, error, sizeof(error)) == 0);
        assert(afs_hixl_connect(runtime, peer, 1000, error, sizeof(error)) == 0);
        afs_hixl_window_t exports[2]{};
        assert(afs_hixl_reserve_export(runtime, 0, 1, 3, &exports[0], error, sizeof(error)) == 0);
        assert(afs_hixl_reserve_export(runtime, 1, 1, 4, &exports[1], error, sizeof(error)) == 0);
        afs_hixl_batch_window_t windows[]{{0, exports[0].address, 3}, {1, exports[1].address, 4}};
        uint8_t payload[]{'a', 'b', 'c', 'd', 'e', 'f', 'g'};
        uint32_t dispatched = 1;
        auto write = [&](size_t len, uint32_t timeout = 123) {
            return afs_hixl_write_batch(runtime, peer, windows, 2, payload, len, timeout,
                                        &dispatched, error, sizeof(error));
        };
        assert(write(sizeof(payload) - 1) == hixl::PARAM_INVALID && dispatched == 0);
        assert(sync_calls == 0);
        if (scenario.find("-write-success") != std::string::npos) {
            assert(setenv("AFS_HIXL_TEST_FAIL_BEFORE_DISPATCH", "1", 1) == 0);
            assert(write(sizeof(payload)) != 0 && dispatched == 0);
            assert(unsetenv("AFS_HIXL_TEST_FAIL_BEFORE_DISPATCH") == 0);
            for (int attempt = 0; attempt < 2; ++attempt) {
                ++payload[0];
                assert(write(sizeof(payload)) == 0 && dispatched == 1);
                assert(std::memcmp(reinterpret_cast<void *>(exports[0].address), payload, 3) == 0);
                assert(std::memcmp(reinterpret_cast<void *>(exports[1].address), payload + 3, 4) == 0);
            }
            assert(batch_descriptors == 2 && batch_bytes == 7);
            assert(sync_calls == 2);
            assert(afs_hixl_release_export(runtime, 0, 1, error, sizeof(error)) == 0);
            assert(afs_hixl_release_export(runtime, 1, 1, error, sizeof(error)) == 0);
            assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
            afs_hixl_destroy(runtime);
        } else {
            throw_sync = scenario.find("-write-exception") != std::string::npos;
            sync_status = scenario.find("-timeout") != std::string::npos ? hixl::TIMEOUT : hixl::FAILED;
            const auto status = write(sizeof(payload), 123);
            assert(status != 0 && dispatched == 1 && sync_calls == 1);
            if (sync_status == hixl::TIMEOUT) { assert(status == hixl::TIMEOUT); }
            assert(write(sizeof(payload)) != 0 && dispatched == 0);
            assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
            assert(allocations == 4 && finalizations == 0);
        }
        return 0;
    }
    if (scenario == "roce-batch-100mib" || scenario == "hccs-batch-100mib") {
        batch_mode = true;
        constexpr size_t window_bytes = 4u * 1024u * 1024u;
        constexpr size_t window_count = 25;
        constexpr size_t total_bytes = window_count * window_bytes;
        assert(afs_hixl_start(0, "127.0.0.1:7920", expected_listen_port, window_bytes, window_count, 1,
            hccs ? AFS_HIXL_TRANSPORT_HCCS : AFS_HIXL_TRANSPORT_ROCE,
            relay_mode ? 4 : 0, relay_mode ? 8 : 0, &actual_domain, &runtime, error, sizeof(error)) == 0);
        assert(allocations == window_count + 1);
        assert(afs_hixl_connect(runtime, "127.0.0.1:7921", 1000, error, sizeof(error)) == 0);

        std::vector<uint8_t> source_bytes(total_bytes);
        for (size_t index = 0; index < total_bytes; ++index) {
            source_bytes[index] = static_cast<uint8_t>(index % 251u);
        }
        std::vector<afs_hixl_batch_window_t> windows(window_count);
        for (size_t index = 0; index < window_count; ++index) {
            windows[index].slot_id = window_count - 1 - index;
            windows[index].remote_address =
                reinterpret_cast<uintptr_t>(source_bytes.data() + index * window_bytes);
            windows[index].length = window_bytes;
        }
        std::vector<uint8_t> output(total_bytes, 0);
        char rejection[512] = {};
        const size_t sdk_baseline = sync_calls;
        auto rejects = [&](const std::vector<afs_hixl_batch_window_t> &bad) {
            uint32_t dispatch = 999;
            uint64_t sdk_ns = 17;
            uint64_t copy_ns = 17;
            assert(read_sync_into(runtime, "127.0.0.1:7921", bad.data(), bad.size(),
                output.data(), output.size(), 123, &dispatch, &sdk_ns, &copy_ns,
                rejection, sizeof(rejection)) != 0);
            assert(sdk_ns == 0 && copy_ns == 0);
            assert(dispatch == 0 && sync_calls == sdk_baseline);
        };

        rejects({});
        auto duplicate = windows;
        duplicate[1].slot_id = duplicate[0].slot_id;
        rejects(duplicate);
        auto empty = windows;
        empty[0].length = 0;
        rejects(empty);
        auto null_remote = windows;
        null_remote[0].remote_address = 0;
        rejects(null_remote);
        auto overflowing_remote = windows;
        overflowing_remote[0].remote_address = std::numeric_limits<uintptr_t>::max();
        rejects(overflowing_remote);
        auto outside = windows;
        outside[0].slot_id = window_count;
        rejects(outside);
        auto oversized = windows;
        oversized[0].length = window_bytes + 1;
        rejects(oversized);

        uint32_t dispatch = 0;
        uint64_t sdk_ns = 0;
        uint64_t copy_ns = 0;
        assert(read_sync_into(runtime, "127.0.0.1:7921", windows.data(), windows.size(),
            output.data(), output.size(), 123, &dispatch, &sdk_ns, &copy_ns,
            error, sizeof(error)) == 0);
        assert(dispatch == 1 && sync_calls == sdk_baseline + 1);
        assert(std::memcmp(output.data(), source_bytes.data(), total_bytes) == 0);
        assert(sdk_ns >= 4000000 && copy_ns > 0);
        assert(batch_descriptors == window_count);
        assert(batch_bytes == total_bytes);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(allocations == 0);
        assert(finalizations == 1);
        return 0;
    }
    assert(start() == 0);
    assert(allocations == 2);
    if (std::string(argv[1]) == "roce" || std::string(argv[1]) == "roce-listen-port" ||
        std::string(argv[1]) == "roce-max-listen-port") {
        assert(device_roce);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(allocations == 0);
        return 0;
    }
    assert(afs_hixl_connect(runtime, "127.0.0.1:7921", 1000, error, sizeof(error)) == 0);
    char source[4] = {'r', 'e', 'a', 'd'};
    uint32_t dispatch_started = 0;
    if (scenario == "roce-pre-dispatch-bad-alloc" || scenario == "hccs-pre-dispatch-bad-alloc") {
        char output[4] = {};
        uint64_t sdk_ns = 0;
        uint64_t copy_ns = 0;
        auto call = [&]() {
            return read_sync_one(runtime, 0, "127.0.0.1:7921",
                reinterpret_cast<uintptr_t>(source), reinterpret_cast<uint8_t *>(output),
                sizeof(output), 123, &dispatch_started, &sdk_ns, &copy_ns, error, sizeof(error));
        };

        uint32_t status = call();
        assert(status == 0 && dispatch_started == 1);
        assert(std::memcmp(output, source, 4) == 0);

        // Fail deterministically between Host slot reservation and dispatch;
        // the bridge must roll the reservation back without submitting.
        assert(setenv("AFS_HIXL_TEST_FAIL_BEFORE_DISPATCH", "1", 1) == 0);
        dispatch_started = 0;
        status = call();
        assert(unsetenv("AFS_HIXL_TEST_FAIL_BEFORE_DISPATCH") == 0);
        assert(status != 0 && dispatch_started == 0);
        assert(sync_calls == 1);

        dispatch_started = 0;
        status = call();
        assert(status == 0 && dispatch_started == 1);
        assert(std::memcmp(output, source, 4) == 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(allocations == 0);
        return 0;
    }
    if (scenario == "roce-pre-dispatch-deadline" || scenario == "hccs-pre-dispatch-deadline") {
        const char *peer = "127.0.0.1:7921";
        const afs_hixl_batch_window_t window{0, reinterpret_cast<uintptr_t>(source), sizeof(source)};
        uint64_t address = 17;
        uint64_t sdk_ns = 17;
        dispatch_started = 1;
        // The test hook holds the reserved slot past the 1 ms budget.
        assert(setenv("AFS_HIXL_TEST_DELAY_BEFORE_DISPATCH", "1", 1) == 0);
        const auto result = afs_hixl_read_sync_batch(runtime, peer, &window, 1, &address, 1,
            &dispatch_started, &sdk_ns, error, sizeof(error));
        assert(unsetenv("AFS_HIXL_TEST_DELAY_BEFORE_DISPATCH") == 0);
        assert(result == hixl::TIMEOUT && dispatch_started == 0 && sync_calls == 0);
        assert(sdk_ns == 0 && address == 17);

        // Retry the same slot to prove the expired READ returned its reservation.
        uint8_t output[sizeof(source)] = {};
        uint64_t copy_ns = 0;
        assert(read_sync_into(runtime, peer, &window, 1, output, sizeof(output), 123,
            &dispatch_started, &sdk_ns, &copy_ns, error, sizeof(error)) == 0);
        assert(dispatch_started == 1 && sync_calls == 1);
        assert(std::memcmp(output, source, sizeof(output)) == 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        assert(allocations == 0 && finalizations == 1);
        return 0;
    }
    if (scenario == "hccs-pinned-read" || scenario == "roce-pinned-read") {
        afs_hixl_batch_window_t window{0, reinterpret_cast<uintptr_t>(source), 4};
        uint64_t address = 0;
        uint64_t sdk_ns = 0;
        sync_status = hixl::SUCCESS;
        assert(afs_hixl_read_sync_batch(runtime, "127.0.0.1:7921", &window, 1, &address, 123, &dispatch_started,
            &sdk_ns, error, sizeof(error)) == 0);
        assert(dispatch_started == 1 && address != 0);
        assert(std::memcmp(reinterpret_cast<const void *>(static_cast<uintptr_t>(address)), source, 4) == 0);
        // The slot stays the caller's until it is released.
        uint64_t second = 0;
        assert(afs_hixl_read_sync_batch(runtime, "127.0.0.1:7921", &window, 1, &second, 123, &dispatch_started,
            &sdk_ns, error, sizeof(error)) != 0);
        assert(dispatch_started == 0 && sync_calls == 1);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        const uint64_t unknown = 9;
        assert(afs_hixl_release_receives(runtime, &unknown, 1, error, sizeof(error)) != 0);
        const uint64_t slot = 0;
        assert(afs_hixl_release_receives(runtime, nullptr, 1, error, sizeof(error)) != 0);
        assert(afs_hixl_release_receives(runtime, &slot, 1, error, sizeof(error)) == 0);
        assert(afs_hixl_release_receives(runtime, &slot, 1, error, sizeof(error)) != 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime);
        return 0;
    }
    assert(registrations == (relay_mode ? 0 : 2));
    assert(actual_domain == (hccs ? 7 : 0));
    uint8_t output[4] = {};
    uint64_t sdk_transfer_ns = 17;
    uint64_t output_copy_ns = 17;
    const bool invalid = scenario.find("-pre-submit-invalid") != std::string::npos;
    sync_status = scenario.find("-timeout") != std::string::npos ? hixl::TIMEOUT :
        scenario.find("-failed") != std::string::npos ? hixl::FAILED : hixl::SUCCESS;
    throw_sync = scenario.find("-exception") != std::string::npos;
    auto read = [&](uint32_t timeout) {
        return read_sync_one(runtime, 0, "127.0.0.1:7921", reinterpret_cast<uintptr_t>(source),
            output, sizeof(output), timeout, &dispatch_started, &sdk_transfer_ns, &output_copy_ns,
            error, sizeof(error));
    };
    const auto result = read(invalid ? 0 : 123);
    if (invalid) {
        assert(result != 0 && dispatch_started == 0 && sync_calls == 0);
        assert(sdk_transfer_ns == 0 && output_copy_ns == 0);
    } else if (throw_sync || sync_status != hixl::SUCCESS) {
        assert(result != 0 && dispatch_started == 1 && sync_calls == 1);
        if (sync_status == hixl::TIMEOUT) { assert(result == hixl::TIMEOUT); }
        assert(sdk_transfer_ns == 0 && output_copy_ns == 0 && output[0] == 0);
        assert(read(123) != 0 && dispatch_started == 0 && sync_calls == 1);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        assert(allocations == 2 && finalizations == 0);
        return 0;
    } else {
        assert(result == 0 && dispatch_started == 1 && sync_calls == 1);
        assert(std::memcmp(output, source, sizeof(output)) == 0);
        assert(sdk_transfer_ns >= 4000000 && output_copy_ns < sdk_transfer_ns);
    }
    assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
    afs_hixl_destroy(runtime);
    assert(allocations == 0 && finalizations == 1);
}
