/*
 * AFS HIXL rH2H bridge.  This file is deliberately small: the AFS control
 * plane owns authorization and this layer owns only ACL/HIXL resources.
 */
#include "hixl.h"
#include "hixl_fabric.h"
#include "hixl_numa.h"

#include <acl/acl_rt.h>
#include <hixl/hixl.h>
#include <hixl/hixl_types.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <string>
#include <vector>
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
#include <thread>
#endif

namespace {

constexpr uint32_t kSuccess = hixl::SUCCESS;
constexpr uint32_t kParamInvalid = hixl::PARAM_INVALID;
constexpr uint32_t kTimeout = hixl::TIMEOUT;
constexpr uint32_t kNotConnected = hixl::NOT_CONNECTED;
constexpr uint32_t kFailed = hixl::FAILED;
constexpr uint32_t kUnsupported = hixl::UNSUPPORTED;
constexpr uint32_t kResourceExhausted = hixl::RESOURCE_EXHAUSTED;
constexpr size_t kMaxWindowBytes = 4U * 1024U * 1024U;
constexpr size_t kMaxPoolBytes = 512U * 1024U * 1024U;
constexpr size_t kMaxHostSlotsPerPool = 4096;
// Only ordinary direct RoCE registers each individual Host slot.
constexpr size_t kMaxDirectRegistrations = 4000;

constexpr uint32_t kUnset = AFS_HIXL_FABRIC_OPTION_UNSET;

bool valid_fabric_options(const afs_hixl_fabric_options_t &options) {
    const auto in_range = [](uint32_t value, uint32_t min, uint32_t max) {
        return value == kUnset || (value >= min && value <= max);
    };
    return in_range(options.enable_aicpu_unfold, 0, 1) &&
           in_range(options.task_streams, 1, 8) &&
           (options.task_streams == kUnset || options.task_streams == 1 || options.enable_aicpu_unfold == 0) &&
           in_range(options.max_capacity_tb, 1, 1024) &&
           in_range(options.start_address_tb, 0, 1024) &&
           in_range(options.max_transfer_count_per_batch, 1, 1920);
}

std::string fabric_config_json(const afs_hixl_fabric_options_t &options) {
    std::string json = "{";
    const auto append = [&](const char *key, uint32_t value, bool boolean = false) {
        if (value == kUnset) {
            return;
        }
        if (json.size() > 1) {
            json += ',';
        }
        json += '"';
        json += key;
        json += "\":";
        json += boolean ? (value == 0 ? "false" : "true") : std::to_string(value);
    };
    append("fabric_memory.enable_aicpu_unfold", options.enable_aicpu_unfold, true);
    append("fabric_memory.task_stream_num", options.task_streams);
    append("fabric_memory.max_capacity", options.max_capacity_tb);
    append("fabric_memory.start_address", options.start_address_tb);
    append("transfer_config.max_transfer_count_per_batch", options.max_transfer_count_per_batch);
    return json.size() == 1 ? std::string{} : json + '}';
}

struct FabricSlab {
    void *base = nullptr;
    size_t extent = 0;
    hixl::MemHandle handle = nullptr;
    bool registered = false;
};

struct HostSlot {
    void *address = nullptr;
    size_t length = 0;
    hixl::MemHandle handle = nullptr;
    bool registered = false;
    bool in_use = false;
    bool quarantined = false;
};

struct BatchPlan {
    std::vector<size_t> slot_ids;
    std::vector<hixl::TransferOpDesc> descriptors;
};

struct Runtime {
    // Keep the provider loaded until after the engine object is destroyed.
    afs_hixl_fabric::Provider fabric_provider;
    hixl::Hixl engine;
    int32_t device_id = -1;
    aclrtContext context = nullptr;
    bool context_created = false;
    bool acl_process_lease = false;
    bool acl_device_lease = false;
    bool engine_initialized = false;
    bool poisoned = false;
    bool clean = false;
    bool relay = false;
    bool fabric = false;
    bool trace_sdk = false;
    bool shutting_down = false;
    FabricSlab fabric_receive;
    FabricSlab fabric_export;
    uint64_t hccs_domain_id = 0;
    size_t window_bytes = 0;
    std::vector<HostSlot> receive;
    std::vector<HostSlot> exports;
    std::set<std::string> peers;
};

struct SharedAclState {
    std::mutex mutex;
    size_t process_leases = 0;
    bool owns_process_init = false;
    std::map<int32_t, size_t> device_leases;
};

SharedAclState &shared_acl_state() {
    static SharedAclState state;
    return state;
}

int write_error(char *out, size_t capacity, const char *message) {
    if (out != nullptr && capacity != 0) {
        const size_t length = std::strlen(message);
        const size_t copied = std::min(length, capacity - 1);
        std::memcpy(out, message, copied);
        out[copied] = '\0';
    }
    return -1;
}

uint32_t acl_status(aclError status, char *error, size_t error_len, const char *operation) {
    if (status == ACL_SUCCESS) {
        return kSuccess;
    }
    std::string message(operation);
    message.append(" failed with ACL status ");
    message.append(std::to_string(static_cast<uint32_t>(status)));
    write_error(error, error_len, message.c_str());
    return static_cast<uint32_t>(status);
}

uint64_t elapsed_nanoseconds(std::chrono::steady_clock::time_point start,
                             std::chrono::steady_clock::time_point end) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return elapsed > 0 ? static_cast<uint64_t>(elapsed) : 0;
}

// Opt-in per-batch evidence for isolated benchmark runs; do not include addresses.
void trace_sdk(const Runtime &runtime, const char *op, const BatchPlan &plan, uint64_t ns, uint32_t status) {
    uint64_t bytes = 0;
    for (const auto &descriptor : plan.descriptors) { bytes += descriptor.len; }
    std::fprintf(stderr, "AFS_HIXL_SDK device=%d op=%s windows=%zu bytes=%llu sdk_ns=%llu status=%u\n",
                 runtime.device_id, op, plan.descriptors.size(), static_cast<unsigned long long>(bytes),
                 static_cast<unsigned long long>(ns), static_cast<unsigned>(status));
}

uint32_t acquire_acl_leases(Runtime *runtime, char *error, size_t error_len) {
    SharedAclState &state = shared_acl_state();
    std::lock_guard<std::mutex> lock(state.mutex);
    if (!runtime->acl_process_lease) {
        if (state.process_leases == 0) {
            const aclError init_status = aclInit(nullptr);
            if (init_status == ACL_SUCCESS) {
                state.owns_process_init = true;
            } else if (init_status == ACL_ERROR_REPEAT_INITIALIZE) {
                state.owns_process_init = false;
            } else {
                return acl_status(init_status, error, error_len, "aclInit");
            }
        }
        if (state.process_leases == std::numeric_limits<size_t>::max()) {
            write_error(error, error_len, "ACL process lease count exhausted");
            return kResourceExhausted;
        }
        ++state.process_leases;
        runtime->acl_process_lease = true;
    }
    if (!runtime->acl_device_lease) {
        auto [device, inserted] = state.device_leases.try_emplace(runtime->device_id, 0);
        if (device->second == std::numeric_limits<size_t>::max()) {
            if (inserted) {
                state.device_leases.erase(device);
            }
            write_error(error, error_len, "ACL device lease count exhausted");
            return kResourceExhausted;
        }
        aclError status = aclrtSetDevice(runtime->device_id);
        if (status != ACL_SUCCESS) {
            if (inserted) {
                state.device_leases.erase(device);
            }
            return acl_status(status, error, error_len, "aclrtSetDevice");
        }
        ++device->second;
        runtime->acl_device_lease = true;
    }
    return kSuccess;
}

uint32_t release_acl_leases(Runtime *runtime, char *error, size_t error_len) {
    SharedAclState &state = shared_acl_state();
    std::lock_guard<std::mutex> lock(state.mutex);
    if (runtime->acl_device_lease) {
        auto device = state.device_leases.find(runtime->device_id);
        if (device == state.device_leases.end() || device->second == 0) {
            write_error(error, error_len, "ACL device lease is missing during release");
            return kFailed;
        }
        if (device->second == 1) {
            const aclError status = aclrtResetDevice(runtime->device_id);
            if (status != ACL_SUCCESS) {
                return acl_status(status, error, error_len, "aclrtResetDevice");
            }
            state.device_leases.erase(device);
        } else {
            --device->second;
        }
        runtime->acl_device_lease = false;
    }
    if (runtime->acl_process_lease) {
        if (state.process_leases == 0) {
            write_error(error, error_len, "ACL process lease is missing during release");
            return kFailed;
        }
        if (state.process_leases == 1 && state.owns_process_init) {
            const aclError status = aclFinalize();
            if (status != ACL_SUCCESS) {
                return acl_status(status, error, error_len, "aclFinalize");
            }
            state.owns_process_init = false;
        }
        --state.process_leases;
        runtime->acl_process_lease = false;
        if (state.process_leases == 0) {
            state.owns_process_init = false;
        }
    }
    return kSuccess;
}

uint32_t check_runtime(Runtime *runtime, char *error, size_t error_len) {
    if (runtime == nullptr || runtime->clean) {
        write_error(error, error_len, "invalid or finalized HIXL runtime");
        return kParamInvalid;
    }
    if (runtime->poisoned) {
        write_error(error, error_len, "HIXL runtime generation is quarantined");
        return kFailed;
    }
    if (runtime->shutting_down) {
        write_error(error, error_len, "HIXL runtime shutdown has started");
        return kFailed;
    }
    return kSuccess;
}

uint32_t cleanup_fabric_slabs(Runtime *runtime, char *error, size_t error_len) {
    const auto slabs = {&runtime->fabric_receive, &runtime->fabric_export};
    for (auto *slab : slabs) {
        if (slab->registered) {
            const auto status = runtime->engine.DeregisterMem(slab->handle);
            if (status != kSuccess) {
                if (status == kTimeout) {
                    runtime->poisoned = true;
                    write_error(error, error_len, "HIXL FabricMem slab deregistration timed out; outcome unknown");
                    return AFS_HIXL_QUARANTINED;
                }
                write_error(error, error_len, "HIXL FabricMem slab deregistration failed");
                return status;
            }
            slab->registered = false;
            slab->handle = nullptr;
        }
    }
    for (auto *slab : slabs) {
        if (slab->base != nullptr) {
            const auto status = runtime->fabric_provider.free(slab->base, error, error_len);
            if (status != kSuccess) {
                if (status == afs_hixl_fabric::kOwnershipUnknown || status == kTimeout) {
                    runtime->poisoned = true;
                    return AFS_HIXL_QUARANTINED;
                }
                return status;
            }
            slab->base = nullptr;
        }
    }
    return kSuccess;
}

bool startup_cleanup(Runtime *runtime) noexcept try {
    if (runtime == nullptr) {
        return true;
    }
    if (runtime->poisoned) {
        return false;
    }
    if (runtime->fabric) {
        // Fabric slots borrow addresses; only the slab owns the allocation.
        if (cleanup_fabric_slabs(runtime, nullptr, 0) != kSuccess) {
            return false;
        }
    } else {
        bool clean = true;
        for (auto *pool : {&runtime->receive, &runtime->exports}) {
            for (HostSlot &slot : *pool) {
                if (slot.registered && runtime->engine_initialized) {
                    const hixl::Status status = runtime->engine.DeregisterMem(slot.handle);
                    if (status != kSuccess) {
                        clean = false;
                        continue;
                    }
                    slot.registered = false;
                    slot.handle = nullptr;
                }
                if (!slot.registered && slot.address != nullptr) {
                    const aclError status = aclrtFreeHost(slot.address);
                    if (status != ACL_SUCCESS) {
                        clean = false;
                        continue;
                    }
                    slot.address = nullptr;
                }
            }
        }
        if (!clean) {
            return false;
        }
    }
    if (runtime->engine_initialized) {
        runtime->engine.Finalize();
        runtime->engine_initialized = false;
    }
    if (runtime->context_created) {
        if (aclrtDestroyContext(runtime->context) != ACL_SUCCESS) {
            return false;
        }
        runtime->context = nullptr;
        runtime->context_created = false;
    }
    if (release_acl_leases(runtime, nullptr, 0) != kSuccess) {
        return false;
    }
    return true;
} catch (...) {
    return false;
}

void cleanup_failed_start(std::unique_ptr<Runtime> &holder, void **out_runtime) noexcept {
    if (holder != nullptr && !startup_cleanup(holder.get())) {
        holder->poisoned = true;
        *out_runtime = holder.release();
    }
}

uint32_t initialize_acl(Runtime *runtime, char *error, size_t error_len) {
    uint32_t status = acquire_acl_leases(runtime, error, error_len);
    if (status != kSuccess) {
        return status;
    }
    const aclError context_status = aclrtCreateContext(&runtime->context, runtime->device_id);
    if (context_status != ACL_SUCCESS) {
        return acl_status(context_status, error, error_len, "aclrtCreateContext");
    }
    runtime->context_created = true;
    const aclError current_status = aclrtSetCurrentContext(runtime->context);
    return acl_status(current_status, error, error_len, "aclrtSetCurrentContext");
}

uint32_t register_fabric_slab(Runtime *runtime,
                              FabricSlab &slab,
                              std::vector<HostSlot> &slots,
                              char *error,
                              size_t error_len) try {
    // Start has already checked the combined logical size before resizing the pools.
    const size_t logical_bytes = slots.size() * runtime->window_bytes;
    constexpr size_t alignment = afs_hixl_fabric::kAllocationAlignment;
    const size_t remainder = logical_bytes % alignment;
    const size_t padding = remainder == 0 ? 0 : alignment - remainder;
    if (logical_bytes > std::numeric_limits<size_t>::max() - padding) {
        write_error(error, error_len, "HIXL FabricMem slab size overflows after alignment");
        return kParamInvalid;
    }
    slab.extent = logical_bytes + padding;
    auto status = runtime->fabric_provider.allocate(slab.extent, &slab.base, error, error_len);
    if (status != kSuccess) {
        // A failed call leaving an output does not prove it owns a valid allocation.
        runtime->poisoned = slab.base != nullptr || status == afs_hixl_fabric::kOwnershipUnknown ||
                            status == kTimeout;
        return status;
    }
    const auto base = reinterpret_cast<uintptr_t>(slab.base);
    if (base == 0 || slab.extent > std::numeric_limits<uintptr_t>::max() - base) {
        runtime->poisoned = true;
        write_error(error, error_len, "HIXL FabricMem allocation did not return a valid address range");
        return kFailed;
    }
    std::memset(slab.base, 0, slab.extent);
    // ADXL tracks the allocation by its original base. Registering individual
    // slots can require an external-memory API absent from older drivers.
    const hixl::MemDesc descriptor{base, slab.extent};
    status = runtime->engine.RegisterMem(descriptor, hixl::MEM_HOST, slab.handle);
    if (status != kSuccess) {
        runtime->poisoned = slab.handle != nullptr || status == kTimeout;
        write_error(error, error_len, "HIXL RegisterMem(MEM_HOST) for FabricMem slab failed");
        return status;
    }
    if (slab.handle == nullptr) {
        runtime->poisoned = true;
        write_error(error, error_len, "HIXL FabricMem slab registration did not return a handle");
        return kFailed;
    }
    slab.registered = true;
    for (size_t index = 0; index < slots.size(); ++index) {
        slots[index].address = static_cast<unsigned char *>(slab.base) + index * runtime->window_bytes;
        slots[index].length = runtime->window_bytes;
    }
    return kSuccess;
} catch (...) {
    // An exception at either SDK entry point can leave hidden allocator or
    // registration state. Do not touch either slab in startup rollback.
    runtime->poisoned = true;
    throw;
}

uint32_t register_pool(Runtime *runtime,
                       std::vector<HostSlot> &pool,
                       bool for_export,
                       afs_hixl_numa::HostNode numa_node,
                       char *error,
                       size_t error_len) {
    for (HostSlot &slot : pool) {
        afs_hixl_numa::ScopedPreference preference(numa_node.id);
        const aclError status = aclrtMallocHost(&slot.address, runtime->window_bytes);
        if (status == ACL_SUCCESS) {
            slot.length = runtime->window_bytes;
            // First-touch receive pages under the preference too. Exports must
            // always be initialized before Rust borrows them as a byte slice.
            if (for_export || preference.applied()) { std::memset(slot.address, 0, slot.length); }
        }
        if (&slot == &pool.front() && !for_export) {
            std::fprintf(stderr, "AFS_HIXL_NUMA device=%d node=%d policy=%s source=%s\n", runtime->device_id,
                         numa_node.id, preference.applied() ? "preferred" : "inherited", numa_node.source);
        }
        if (!preference.restore()) {
            write_error(error, error_len, "failed to restore HIXL owner thread memory policy");
            return kFailed;
        }
        if (status != ACL_SUCCESS) { return acl_status(status, error, error_len, "aclrtMallocHost"); }
        if (runtime->relay) {
            // Unregistered Host addresses select the SDK's BufferPool relay
            // path. Registering these would accidentally select direct H2H.
            continue;
        }
        // RegisterMem has no access-mode flags. The MEM_HOST registration
        // serves both remote READ exports and remote WRITE destinations.
        hixl::MemDesc descriptor{reinterpret_cast<uintptr_t>(slot.address), slot.length};
        const hixl::Status register_status =
            runtime->engine.RegisterMem(descriptor, hixl::MEM_HOST, slot.handle);
        if (register_status != kSuccess) {
            write_error(error, error_len, "HIXL RegisterMem(MEM_HOST) failed");
            return register_status;
        }
        slot.registered = true;
    }
    return kSuccess;
}

HostSlot *get_slot(std::vector<HostSlot> &pool, uint64_t slot_id, char *error, size_t error_len) {
    if (slot_id >= pool.size()) {
        write_error(error, error_len, "HIXL slot id is outside the fixed pool");
        return nullptr;
    }
    return &pool[static_cast<size_t>(slot_id)];
}

void quarantine_slots(Runtime *runtime, const std::vector<size_t> &slot_ids) {
    for (size_t slot_id : slot_ids) {
        if (slot_id < runtime->receive.size()) {
            HostSlot &slot = runtime->receive[slot_id];
            slot.in_use = true;
            slot.quarantined = true;
        }
    }
    runtime->poisoned = true;
}

uint32_t plan_batch(Runtime *runtime,
                    const afs_hixl_batch_window_t *windows,
                    size_t count,
                    BatchPlan &plan,
                    char *error,
                    size_t error_len) {
    if (windows == nullptr || count == 0 || count > AFS_HIXL_MAX_BATCH_WINDOWS ||
        count > runtime->receive.size()) {
        write_error(error, error_len, "invalid HIXL batch window count");
        return kParamInvalid;
    }
    std::set<size_t> used_slots;
    plan.slot_ids.reserve(count);
    plan.descriptors.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        const afs_hixl_batch_window_t &window = windows[index];
        if (window.slot_id >= runtime->receive.size()) {
            write_error(error, error_len, "HIXL batch slot id is outside the fixed pool");
            return kParamInvalid;
        }
        const size_t slot_id = static_cast<size_t>(window.slot_id);
        HostSlot &slot = runtime->receive[slot_id];
        if (slot.in_use || slot.quarantined) {
            write_error(error, error_len, "HIXL batch receive slot is not free");
            return kResourceExhausted;
        }
        if (!used_slots.insert(slot_id).second) {
            write_error(error, error_len, "HIXL batch repeats a receive slot");
            return kParamInvalid;
        }
        if (window.length == 0 || window.length > runtime->window_bytes ||
            window.length > slot.length) {
            write_error(error, error_len, "HIXL batch window length is invalid");
            return kParamInvalid;
        }
        if (window.remote_address == 0 ||
            window.remote_address > std::numeric_limits<uintptr_t>::max() ||
            window.length > std::numeric_limits<uintptr_t>::max() - static_cast<uintptr_t>(window.remote_address)) {
            write_error(error, error_len, "HIXL batch remote range is invalid");
            return kParamInvalid;
        }
        plan.slot_ids.push_back(slot_id);
        plan.descriptors.push_back(hixl::TransferOpDesc{
            reinterpret_cast<uintptr_t>(slot.address), window.remote_address, window.length});
    }
    return kSuccess;
}

void reserve_batch(Runtime *runtime, const BatchPlan &plan) {
    for (size_t slot_id : plan.slot_ids) {
        runtime->receive[slot_id].in_use = true;
    }
}

void release_reservation(Runtime *runtime, const BatchPlan &plan) noexcept {
    for (size_t slot_id : plan.slot_ids) {
        runtime->receive[slot_id].in_use = false;
    }
}

// Test seam: the fake-boundary bridge test recompiles this file with
// AFS_HIXL_TEST_FAULT_INJECTION so it can fail or delay between Host slot
// reservation and the dispatch boundary. Production builds do not use this hook.
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
void inject_before_dispatch_for_test() {
    if (std::getenv("AFS_HIXL_TEST_FAIL_BEFORE_DISPATCH") != nullptr) {
        throw std::bad_alloc();
    }
    if (std::getenv("AFS_HIXL_TEST_DELAY_BEFORE_DISPATCH") != nullptr) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
#endif

void pinned_addresses(const Runtime *runtime, const std::vector<size_t> &slot_ids, uint64_t *slot_addresses) {
    for (size_t index = 0; index < slot_ids.size(); ++index) {
        const HostSlot &slot = runtime->receive[slot_ids[index]];
        slot_addresses[index] = reinterpret_cast<uint64_t>(slot.address);
    }
}

uint32_t disconnect_peers(Runtime *runtime, char *error, size_t error_len) {
    while (!runtime->peers.empty()) {
        const auto peer = runtime->peers.begin();
        const hixl::Status status = runtime->engine.Disconnect(hixl::AscendString(peer->c_str()), 1000);
        if (status != kSuccess) {
            if (runtime->fabric && status == kTimeout) {
                runtime->poisoned = true;
                write_error(error, error_len, "HIXL FabricMem Disconnect timed out; outcome unknown");
                return AFS_HIXL_QUARANTINED;
            }
            write_error(error, error_len, "HIXL Disconnect did not prove a clean link close");
            return status;
        }
        runtime->peers.erase(peer);
    }
    return kSuccess;
}

}  // namespace

extern "C" uint32_t afs_hixl_abi_version(void) {
    return AFS_HIXL_ABI_VERSION;
}

extern "C" uint32_t afs_hixl_start(int32_t device_id,
                                    const char *engine_id,
                                    uint32_t device_listen_port,
                                    size_t window_bytes,
                                    size_t receive_slots,
                                    size_t export_slots,
                                    uint32_t transport,
                                    size_t buffer_pool_count,
                                    size_t buffer_pool_size_mib,
                                    const afs_hixl_fabric_options_t *fabric_options,
                                    uint64_t *out_hccs_domain_id,
                                    void **out_runtime,
                                    char *error,
                                    size_t error_len) {
    if (out_runtime != nullptr) {
        *out_runtime = nullptr;
    }
    if (out_hccs_domain_id != nullptr) {
        *out_hccs_domain_id = 0;
    }
    const bool fabric = transport == AFS_HIXL_TRANSPORT_HCCS_FABRIC;
    const bool hccs = transport == AFS_HIXL_TRANSPORT_HCCS || fabric;
    const bool relay = buffer_pool_count != 0 && buffer_pool_size_mib != 0;
    if (out_runtime == nullptr || out_hccs_domain_id == nullptr || device_id < 0 || engine_id == nullptr || engine_id[0] == '\0' ||
        device_listen_port > std::numeric_limits<uint16_t>::max() ||
        window_bytes == 0 || window_bytes > kMaxWindowBytes || receive_slots == 0 || export_slots == 0 ||
        receive_slots > kMaxHostSlotsPerPool || export_slots > kMaxHostSlotsPerPool ||
        receive_slots > std::numeric_limits<size_t>::max() - export_slots ||
        (receive_slots + export_slots) > std::numeric_limits<size_t>::max() / window_bytes ||
        (transport != AFS_HIXL_TRANSPORT_ROCE && !hccs) ||
        ((buffer_pool_count == 0) != (buffer_pool_size_mib == 0)) ||
        (transport == AFS_HIXL_TRANSPORT_HCCS && !relay) || (fabric && relay) ||
        (relay && (buffer_pool_count > 64 || buffer_pool_size_mib > 64 ||
                   buffer_pool_count * buffer_pool_size_mib > kMaxPoolBytes / (2U * 1024U * 1024U)))) {
        write_error(error, error_len, "invalid HIXL start arguments");
        return kParamInvalid;
    }
    if (fabric_options != nullptr && (!fabric || !valid_fabric_options(*fabric_options))) {
        write_error(error, error_len, "invalid HIXL FabricMem SDK overrides");
        return kParamInvalid;
    }
    if (fabric && device_listen_port != 0) {
        // CANN 9.1 does not apply this override in the Fabric engine path.
        write_error(error, error_len, "HCCS FabricMem does not support device_listen_port");
        return kParamInvalid;
    }
    if (!fabric && !relay && receive_slots + export_slots > kMaxDirectRegistrations) {
        write_error(error, error_len, "HIXL direct Host pool exceeds 4000 SDK registrations");
        return kResourceExhausted;
    }
    if (hccs) {
        const char *inter_hccs_disable = std::getenv("HCCL_INTER_HCCS_DISABLE");
        if (inter_hccs_disable != nullptr && std::strcmp(inter_hccs_disable, "FALSE") != 0) {
            write_error(error, error_len, "HCCS requires HCCL_INTER_HCCS_DISABLE unset or FALSE");
            return kParamInvalid;
        }
        const char *intra_roce = std::getenv("HCCL_INTRA_ROCE_ENABLE");
        if (intra_roce != nullptr && std::strcmp(intra_roce, "0") != 0) {
            write_error(error, error_len, "HCCS conflicts with HCCL_INTRA_ROCE_ENABLE; unset it or set 0");
            return kParamInvalid;
        }
    } else if (relay) {
        // BufferPool uses HCCL communication domains. Select RoCE for both
        // intra-server and inter-server links before initializing the SDK.
        const char *intra_roce = std::getenv("HCCL_INTRA_ROCE_ENABLE");
        const char *intra_pcie = std::getenv("HCCL_INTRA_PCIE_ENABLE");
        const char *inter_hccs_disable = std::getenv("HCCL_INTER_HCCS_DISABLE");
        if (intra_roce == nullptr || std::strcmp(intra_roce, "1") != 0 ||
            intra_pcie == nullptr || std::strcmp(intra_pcie, "0") != 0 ||
            inter_hccs_disable == nullptr || std::strcmp(inter_hccs_disable, "TRUE") != 0) {
            write_error(error, error_len,
                        "RoCE BufferPool requires HCCL_INTRA_ROCE_ENABLE=1, "
                        "HCCL_INTRA_PCIE_ENABLE=0 and HCCL_INTER_HCCS_DISABLE=TRUE");
            return kParamInvalid;
        }
    }
    std::unique_ptr<Runtime> holder;
    try {
        holder = std::make_unique<Runtime>();
        auto *runtime = holder.get();
        runtime->device_id = device_id;
        runtime->relay = relay;
        const char *trace = std::getenv("AFS_HIXL_TRACE_SDK");
        runtime->trace_sdk = trace != nullptr && std::strcmp(trace, "1") == 0;
        runtime->fabric = fabric;
        runtime->window_bytes = window_bytes;
        runtime->receive.resize(receive_slots);
        runtime->exports.resize(export_slots);
        uint32_t status = initialize_acl(runtime, error, error_len);
        if (status != kSuccess) {
            cleanup_failed_start(holder, out_runtime);
            return status;
        }
        if (hccs) {
            // These are the A3 SoCs recognized by the paired HIXL generation.
            // Unknown devices fail before engine/HBM initialization.
            static const std::set<std::string> a3_socs = {
                "Ascend910_9391", "Ascend910_9381", "Ascend910_9392", "Ascend910_9382",
                "Ascend910_9372", "Ascend910_9362"};
            const char *soc = aclrtGetSocName();
            if (soc == nullptr || a3_socs.find(soc) == a3_socs.end()) {
                write_error(error, error_len, "HCCS requires a supported A3 device");
                cleanup_failed_start(holder, out_runtime);
                return kUnsupported;
            }
            int64_t domain = -1;
            const aclError domain_status = aclrtGetDeviceInfo(static_cast<uint32_t>(device_id), ACL_DEV_ATTR_SUPER_POD_ID, &domain);
            if (domain_status != ACL_SUCCESS || domain < 0) {
                write_error(error, error_len, "HCCS cannot verify the device super-pod identity");
                cleanup_failed_start(holder, out_runtime);
                return kUnsupported;
            }
            runtime->hccs_domain_id = static_cast<uint64_t>(domain);
        }
        std::map<hixl::AscendString, hixl::AscendString> options;
        options.emplace(hixl::AscendString(hixl::OPTION_AUTO_CONNECT), hixl::AscendString("0"));
        if (fabric) {
            status = runtime->fabric_provider.open(error, error_len);
            if (status != kSuccess) {
                cleanup_failed_start(holder, out_runtime);
                return status;
            }
            options.emplace(hixl::AscendString(hixl::OPTION_ENABLE_USE_FABRIC_MEM), hixl::AscendString("1"));
            if (fabric_options != nullptr) {
                const auto json = fabric_config_json(*fabric_options);
                if (!json.empty()) {
                    options.emplace(hixl::AscendString(hixl::OPTION_GLOBAL_RESOURCE_CONFIG),
                                    hixl::AscendString(json.c_str()));
                }
            }
        } else if (relay) {
            const std::string pool = std::to_string(buffer_pool_count) + ":" + std::to_string(buffer_pool_size_mib);
            options.emplace(hixl::AscendString(hixl::OPTION_BUFFER_POOL), hixl::AscendString(pool.c_str()));
            if (device_listen_port != 0) {
                const std::string global_resource =
                    "{\"comm_resource_config.listen_port\":\"" + std::to_string(device_listen_port) + "\"}";
                options.emplace(hixl::AscendString(hixl::OPTION_GLOBAL_RESOURCE_CONFIG),
                                hixl::AscendString(global_resource.c_str()));
            }
            // Nonzero BufferPool and no protocol_desc/LocalCommRes select the
            // communication-domain engine. protocol_desc would select HixlCS,
            // which implements direct transfer and cannot perform this relay.
        } else {
            options.emplace(hixl::AscendString(hixl::OPTION_BUFFER_POOL), hixl::AscendString("0:0"));
            if (device_listen_port == 0) {
                options.emplace(hixl::AscendString(hixl::OPTION_GLOBAL_RESOURCE_CONFIG),
                                hixl::AscendString("{\"comm_resource_config.protocol_desc\":[\"roce:device\"]}"));
            } else {
                const std::string global_resource =
                    "{\"comm_resource_config.protocol_desc\":[\"roce:device\"],\"comm_resource_config.listen_port\":\"" +
                    std::to_string(device_listen_port) + "\"}";
                options.emplace(hixl::AscendString(hixl::OPTION_GLOBAL_RESOURCE_CONFIG),
                                hixl::AscendString(global_resource.c_str()));
            }
        }
        const hixl::AscendString local_engine(engine_id);
        // For Fabric, a throw from Initialize does not prove the engine is idle.
        runtime->poisoned = fabric;
        status = runtime->engine.Initialize(local_engine, options);
        // A timed-out Fabric initialization may still own device resources.
        runtime->poisoned = fabric && status == kTimeout;
        if (status != kSuccess) {
            write_error(error, error_len, "HIXL Initialize failed");
            cleanup_failed_start(holder, out_runtime);
            return status;
        }
        runtime->engine_initialized = true;
        if (fabric) {
            status = register_fabric_slab(runtime, runtime->fabric_receive, runtime->receive, error, error_len);
            if (status == kSuccess) {
                status = register_fabric_slab(runtime, runtime->fabric_export, runtime->exports, error, error_len);
            }
            if (status == kSuccess) {
                std::fprintf(stderr, "AFS_HIXL_NUMA device=%d source=%s receive_bytes=%zu export_bytes=%zu\n",
                             device_id, runtime->fabric_provider.placement_source(),
                             runtime->fabric_receive.extent, runtime->fabric_export.extent);
            }
        } else {
            const auto numa_node = afs_hixl_numa::preferred_node(device_id);
            status = register_pool(runtime, runtime->receive, false, numa_node, error, error_len);
            if (status == kSuccess) {
                status = register_pool(runtime, runtime->exports, true, numa_node, error, error_len);
            }
        }
        if (status != kSuccess) {
            cleanup_failed_start(holder, out_runtime);
            return status;
        }
        *out_runtime = runtime;
        *out_hccs_domain_id = runtime->hccs_domain_id;
        holder.release();
        return kSuccess;
    } catch (const std::exception &exception) {
        cleanup_failed_start(holder, out_runtime);
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        cleanup_failed_start(holder, out_runtime);
        write_error(error, error_len, "unknown exception while starting HIXL");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_connect(void *opaque,
                                      const char *peer,
                                      uint32_t timeout_ms,
                                      char *error,
                                      size_t error_len) {
    auto *runtime = static_cast<Runtime *>(opaque);
    try {
        uint32_t status = check_runtime(runtime, error, error_len);
        if (runtime != nullptr && runtime->fabric && runtime->poisoned) { return AFS_HIXL_QUARANTINED; }
        if (status != kSuccess || peer == nullptr || peer[0] == '\0' || timeout_ms == 0 ||
            timeout_ms > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            if (status == kSuccess) {
                write_error(error, error_len, "invalid HIXL connect arguments");
                return kParamInvalid;
            }
            return status;
        }
        if (runtime->peers.find(peer) != runtime->peers.end()) {
            return kSuccess;
        }
        const hixl::AscendString remote(peer);
        // Once the SDK has been entered, only a recorded successful Connect
        // gives shutdown a peer it can safely disconnect. A failed call does
        // not prove that no link was established or slab mappings exchanged.
        runtime->poisoned = runtime->fabric;
        status = runtime->engine.Connect(remote, static_cast<int32_t>(timeout_ms));
        if (status != kSuccess) {
            if (runtime->fabric) {
                if (error != nullptr && error_len != 0) {
                    std::snprintf(error, error_len, "HIXL FabricMem Connect outcome is uncertain (SDK status %llu)",
                                  static_cast<unsigned long long>(status));
                }
                return AFS_HIXL_QUARANTINED;
            }
            write_error(error, error_len, "HIXL Connect failed");
            return status;
        }
        runtime->peers.emplace(peer);
        runtime->poisoned = false;
        return kSuccess;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
    } catch (...) {
        write_error(error, error_len, "unknown exception while connecting HIXL");
    }
    return runtime != nullptr && runtime->fabric && runtime->poisoned ? AFS_HIXL_QUARANTINED : kFailed;
}

static uint32_t pin_export_slot(Runtime *runtime,
                                uint64_t slot_id,
                                uint64_t generation,
                                size_t len,
                                afs_hixl_window_t *out,
                                HostSlot **pinned,
                                char *error,
                                size_t error_len) {
    if (out == nullptr || generation == 0 || len == 0 || len > runtime->window_bytes) {
        write_error(error, error_len, "invalid HIXL export arguments");
        return kParamInvalid;
    }
    HostSlot *slot = get_slot(runtime->exports, slot_id, error, error_len);
    if (slot == nullptr) {
        return kParamInvalid;
    }
    if (slot->in_use || slot->quarantined) {
        write_error(error, error_len, "HIXL export slot is not free");
        return kResourceExhausted;
    }
    slot->in_use = true;
    out->slot_id = slot_id;
    out->generation = generation;
    out->address = reinterpret_cast<uint64_t>(slot->address);
    out->length = len;
    *pinned = slot;
    return kSuccess;
}

extern "C" uint32_t afs_hixl_reserve_export(void *opaque,
                                             uint64_t slot_id,
                                             uint64_t generation,
                                             size_t len,
                                             afs_hixl_window_t *out,
                                             char *error,
                                             size_t error_len) {
    try {
        auto *runtime = static_cast<Runtime *>(opaque);
        uint32_t status = check_runtime(runtime, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        HostSlot *slot = nullptr;
        return pin_export_slot(runtime, slot_id, generation, len, out, &slot, error, error_len);
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception reserving HIXL export");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_release_export(void *opaque,
                                             uint64_t slot_id,
                                             uint64_t generation,
                                             char *error,
                                             size_t error_len) {
    try {
        auto *runtime = static_cast<Runtime *>(opaque);
        uint32_t status = check_runtime(runtime, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        if (generation == 0) {
            write_error(error, error_len, "invalid HIXL export generation");
            return kParamInvalid;
        }
        HostSlot *slot = get_slot(runtime->exports, slot_id, error, error_len);
        if (slot == nullptr) {
            return kParamInvalid;
        }
        if (!slot->in_use) {
            return kSuccess;
        }
        if (slot->quarantined) {
            write_error(error, error_len, "quarantined HIXL export cannot be reused");
            return kFailed;
        }
        slot->in_use = false;
        return kSuccess;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception releasing HIXL export");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_read_sync_batch(void *opaque,
                                             const char *peer,
                                             const afs_hixl_batch_window_t *windows,
                                             size_t count,
                                             uint64_t *slot_addresses,
                                             uint32_t timeout_ms,
                                             uint32_t *dispatch_started,
                                             uint64_t *sdk_transfer_ns,
                                             char *error,
                                             size_t error_len) {
    auto *runtime = static_cast<Runtime *>(opaque);
    bool invoked = false;
    bool reserved = false;
    BatchPlan plan;
    if (dispatch_started == nullptr || sdk_transfer_ns == nullptr) {
        write_error(error, error_len, "missing HIXL batch read timing output");
        return kParamInvalid;
    }
    *dispatch_started = 0;
    *sdk_transfer_ns = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    try {
        const uint32_t checked = check_runtime(runtime, error, error_len);
        if (checked != kSuccess) { return checked; }
        if (peer == nullptr || peer[0] == '\0' ||
            slot_addresses == nullptr ||
            timeout_ms == 0 || timeout_ms > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            write_error(error, error_len, "invalid HIXL batch READ arguments");
            return kParamInvalid;
        }
        if (runtime->peers.find(peer) == runtime->peers.end()) {
            write_error(error, error_len, "HIXL READ peer is not connected");
            return kNotConnected;
        }
        const uint32_t planned = plan_batch(runtime, windows, count, plan, error, error_len);
        if (planned != kSuccess) { return planned; }
        const hixl::AscendString remote(peer);
        reserve_batch(runtime, plan);
        reserved = true;
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
        inject_before_dispatch_for_test();
#endif
        const auto available = deadline - std::chrono::steady_clock::now();
        if (available <= std::chrono::steady_clock::duration::zero()) {
            release_reservation(runtime, plan);
            write_error(error, error_len, "HIXL batch READ expired before dispatch");
            return kTimeout;
        }
        const auto millis = std::chrono::ceil<std::chrono::milliseconds>(available).count();
        // A non-successful synchronous call is still an uncertain submission.
        // Do not finalize the engine/HBM pools or free any Host window.
        *dispatch_started = 1;
        invoked = true;
        const auto sdk_start = std::chrono::steady_clock::now();
        const auto status = runtime->engine.TransferSync(remote, hixl::READ,
                                                          plan.descriptors,
                                                          static_cast<int32_t>(millis));
        const auto sdk_end = std::chrono::steady_clock::now();
        if (runtime->trace_sdk) { trace_sdk(*runtime, "READ", plan, elapsed_nanoseconds(sdk_start, sdk_end), status); }
        if (status != kSuccess) {
            quarantine_slots(runtime, plan.slot_ids);
            write_error(error, error_len, "HIXL batch READ outcome is uncertain; Host windows and engine retained");
            return status;
        }
        *sdk_transfer_ns = elapsed_nanoseconds(sdk_start, sdk_end);
        pinned_addresses(runtime, plan.slot_ids, slot_addresses);
        return kSuccess;
    } catch (const std::exception &exception) {
        if (invoked) {
            quarantine_slots(runtime, plan.slot_ids);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        if (invoked) {
            quarantine_slots(runtime, plan.slot_ids);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
        write_error(error, error_len, "unknown exception during HIXL batch READ");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_write_batch(void *opaque,
                                          const char *peer,
                                          const afs_hixl_batch_window_t *windows,
                                          size_t count,
                                          const uint8_t *bytes,
                                          size_t len,
                                          uint32_t timeout_ms,
                                          uint32_t *dispatch_started,
                                          char *error,
                                          size_t error_len) {
    if (dispatch_started == nullptr) {
        write_error(error, error_len, "missing HIXL batch WRITE dispatch output");
        return kParamInvalid;
    }
    *dispatch_started = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    auto *runtime = static_cast<Runtime *>(opaque);
    BatchPlan plan;
    bool reserved = false;
    auto retain_or_release = [&]() {
        if (*dispatch_started != 0) {
            quarantine_slots(runtime, plan.slot_ids);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
    };
    try {
        uint32_t status = check_runtime(runtime, error, error_len);
        if (status != kSuccess) { return status; }
        if (peer == nullptr || peer[0] == '\0' || bytes == nullptr || len == 0 ||
            timeout_ms == 0 || timeout_ms > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            write_error(error, error_len, "invalid HIXL batch WRITE arguments");
            return kParamInvalid;
        }
        if (runtime->peers.find(peer) == runtime->peers.end()) {
            write_error(error, error_len, "HIXL WRITE peer is not connected");
            return kNotConnected;
        }
        status = plan_batch(runtime, windows, count, plan, error, error_len);
        if (status != kSuccess) { return status; }
        size_t remaining = len;
        for (const auto &descriptor : plan.descriptors) {
            if (descriptor.len > remaining) {
                write_error(error, error_len, "HIXL batch WRITE payload is too short");
                return kParamInvalid;
            }
            remaining -= descriptor.len;
        }
        if (remaining != 0) {
            write_error(error, error_len, "HIXL batch WRITE payload is too long");
            return kParamInvalid;
        }
        const hixl::AscendString remote(peer);
        reserve_batch(runtime, plan);
        reserved = true;
        size_t offset = 0;
        for (size_t index = 0; index < count; ++index) {
            const size_t length = static_cast<size_t>(windows[index].length);
            std::memcpy(runtime->receive[plan.slot_ids[index]].address, bytes + offset, length);
            offset += length;
        }
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
        inject_before_dispatch_for_test();
#endif
        const auto available = deadline - std::chrono::steady_clock::now();
        if (available <= std::chrono::steady_clock::duration::zero()) {
            release_reservation(runtime, plan);
            write_error(error, error_len, "HIXL batch WRITE expired before dispatch");
            return kTimeout;
        }
        // After SDK entry, a failure does not prove that the engine or peer
        // stopped accessing the native-owned staging and export windows.
        *dispatch_started = 1;
        const auto millis = std::chrono::ceil<std::chrono::milliseconds>(available).count();
        const auto sdk_start = runtime->trace_sdk ? std::chrono::steady_clock::now()
                                                  : std::chrono::steady_clock::time_point{};
        status = runtime->engine.TransferSync(remote, hixl::WRITE, plan.descriptors,
                                              static_cast<int32_t>(millis));
        if (runtime->trace_sdk) {
            trace_sdk(*runtime, "WRITE", plan, elapsed_nanoseconds(sdk_start, std::chrono::steady_clock::now()), status);
        }
        if (status != kSuccess) {
            quarantine_slots(runtime, plan.slot_ids);
            write_error(error, error_len, "HIXL batch WRITE outcome is uncertain; Host slots and engine retained");
            return status;
        }
        release_reservation(runtime, plan);
        return kSuccess;
    } catch (const std::exception &exception) {
        retain_or_release();
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        retain_or_release();
        write_error(error, error_len, "unknown exception during HIXL batch WRITE");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_release_receives(void *opaque,
                                               const uint64_t *slot_ids,
                                               size_t count,
                                               char *error,
                                               size_t error_len) {
    try {
        auto *runtime = static_cast<Runtime *>(opaque);
        uint32_t status = check_runtime(runtime, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        if (slot_ids == nullptr || count == 0) {
            write_error(error, error_len, "invalid HIXL receive release arguments");
            return kParamInvalid;
        }
        for (size_t index = 0; index < count; ++index) {
            HostSlot *slot = get_slot(runtime->receive, slot_ids[index], error, error_len);
            if (slot == nullptr) {
                return kParamInvalid;
            }
            if (!slot->in_use || slot->quarantined) {
                write_error(error, error_len, "HIXL receive slot is not pinned");
                return kParamInvalid;
            }
        }
        for (size_t index = 0; index < count; ++index) {
            runtime->receive[static_cast<size_t>(slot_ids[index])].in_use = false;
        }
        return kSuccess;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception releasing HIXL receive slots");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_shutdown(void *opaque, char *error, size_t error_len) {
    auto *runtime = static_cast<Runtime *>(opaque);
    try {
        if (runtime == nullptr || runtime->clean) {
            write_error(error, error_len, "invalid HIXL runtime for shutdown");
            return kParamInvalid;
        }
        if (runtime->fabric && runtime->poisoned) {
            write_error(error, error_len, "HIXL FabricMem generation is quarantined; resources retained");
            return AFS_HIXL_QUARANTINED;
        }
        if (runtime->poisoned ||
            std::any_of(runtime->receive.begin(), runtime->receive.end(), [](const HostSlot &slot) { return slot.in_use; }) ||
            std::any_of(runtime->exports.begin(), runtime->exports.end(), [](const HostSlot &slot) { return slot.in_use; })) {
            write_error(error, error_len, "HIXL shutdown has not proved drain");
            return kFailed;
        }
        runtime->shutting_down = true;
        uint32_t status = disconnect_peers(runtime, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        const auto cleanup_pool = [runtime, error, error_len](std::vector<HostSlot> &pool,
            const char *deregister_error, const char *free_operation) -> uint32_t {
            for (HostSlot &slot : pool) {
                if (slot.registered) {
                    const hixl::Status status = runtime->engine.DeregisterMem(slot.handle);
                    if (status != kSuccess) {
                        write_error(error, error_len, deregister_error);
                        return status;
                    }
                    slot.registered = false;
                }
                if (slot.address != nullptr) {
                    const aclError status = aclrtFreeHost(slot.address);
                    if (status != ACL_SUCCESS) {
                        return acl_status(status, error, error_len, free_operation);
                    }
                    slot.address = nullptr;
                }
            }
            return kSuccess;
        };
        if (runtime->fabric) {
            status = cleanup_fabric_slabs(runtime, error, error_len);
        } else {
            status = cleanup_pool(runtime->receive, "HIXL receive deregistration failed", "aclrtFreeHost receive");
            if (status == kSuccess) {
                status = cleanup_pool(runtime->exports, "HIXL export deregistration failed", "aclrtFreeHost export");
            }
        }
        if (status != kSuccess) {
            return status;
        }
        if (runtime->engine_initialized) {
            runtime->engine.Finalize();
            runtime->engine_initialized = false;
        }
        if (runtime->context_created) {
            const aclError acl_result = aclrtDestroyContext(runtime->context);
            if (acl_result != ACL_SUCCESS) {
                return acl_status(acl_result, error, error_len, "aclrtDestroyContext");
            }
            runtime->context = nullptr;
            runtime->context_created = false;
        }
        const uint32_t lease_status = release_acl_leases(runtime, error, error_len);
        if (lease_status != kSuccess) {
            return lease_status;
        }
        runtime->clean = true;
        return kSuccess;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
    } catch (...) {
        write_error(error, error_len, "unknown exception shutting down HIXL");
    }
    if (runtime != nullptr && runtime->fabric) {
        runtime->poisoned = true;
        return AFS_HIXL_QUARANTINED;
    }
    return kFailed;
}

extern "C" void afs_hixl_destroy(void *opaque) {
    auto *runtime = static_cast<Runtime *>(opaque);
    if (runtime != nullptr && runtime->clean) {
        delete runtime;
    }
}
