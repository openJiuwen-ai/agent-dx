/*
 * AFS HIXL rH2H bridge.  This file is deliberately small: the AFS control
 * plane owns authorization and this layer owns only ACL/HIXL resources.
 */
#include "hixl.h"

#include <acl/acl_rt.h>
#include <hixl/hixl.h>
#include <hixl/hixl_types.h>

#include <algorithm>
#include <chrono>
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

struct HostSlot {
    void *address = nullptr;
    size_t length = 0;
    hixl::MemHandle handle = nullptr;
    bool registered = false;
    bool in_use = false;
    bool quarantined = false;
};

struct RequestEntry {
    size_t slot_id = 0;
    size_t length = 0;
    size_t local_offset = 0;
};

struct Request {
    hixl::TransferReq handle = nullptr;
    std::vector<RequestEntry> entries;
    size_t length = 0;
    std::chrono::steady_clock::time_point dispatched_at{};
    bool active = false;
};

struct BatchPlan {
    std::vector<size_t> slot_ids;
    std::vector<hixl::TransferOpDesc> descriptors;
    std::vector<RequestEntry> entries;
    size_t output_length = 0;
};

struct Runtime {
    hixl::Hixl engine;
    int32_t device_id = -1;
    aclrtContext context = nullptr;
    bool context_created = false;
    bool acl_process_lease = false;
    bool acl_device_lease = false;
    bool engine_initialized = false;
    bool poisoned = false;
    bool clean = false;
    bool hccs_relay = false;
    uint64_t hccs_domain_id = 0;
    size_t window_bytes = 0;
    std::vector<HostSlot> receive;
    std::vector<HostSlot> exports;
    std::vector<Request> requests;
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
    return kSuccess;
}

bool startup_cleanup(Runtime *runtime) noexcept try {
    if (runtime == nullptr) {
        return true;
    }
    bool clean = true;
    for (HostSlot &slot : runtime->receive) {
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
    for (HostSlot &slot : runtime->exports) {
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
    if (!clean) {
        return false;
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

uint32_t register_pool(Runtime *runtime,
                       std::vector<HostSlot> &pool,
                       char *error,
                       size_t error_len) {
    for (HostSlot &slot : pool) {
        aclError status = aclrtMallocHost(&slot.address, runtime->window_bytes);
        if (status != ACL_SUCCESS) {
            return acl_status(status, error, error_len, "aclrtMallocHost");
        }
        slot.length = runtime->window_bytes;
        if (runtime->hccs_relay) {
            // Unregistered Host addresses select the SDK's BufferPool relay
            // path. Registering these would accidentally select direct H2H.
            continue;
        }
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

Request *find_request(Runtime *runtime, hixl::TransferReq handle) {
    for (Request &request : runtime->requests) {
        if (request.active && request.handle == handle) {
            return &request;
        }
    }
    return nullptr;
}

Request *find_free_request(Runtime *runtime) {
    for (Request &request : runtime->requests) {
        if (!request.active) {
            return &request;
        }
    }
    return nullptr;
}

void quarantine_request(Runtime *runtime, Request &request) {
    for (const RequestEntry &entry : request.entries) {
        if (entry.slot_id < runtime->receive.size()) {
            HostSlot &slot = runtime->receive[entry.slot_id];
            slot.in_use = true;
            slot.quarantined = true;
        }
    }
    request.active = false;
    runtime->poisoned = true;
}

uint32_t plan_batch(Runtime *runtime,
                    const afs_hixl_batch_window_t *windows,
                    size_t count,
                    size_t output_len,
                    BatchPlan &plan,
                    char *error,
                    size_t error_len) {
    if (windows == nullptr || count == 0 || count > AFS_HIXL_MAX_BATCH_WINDOWS ||
        count > runtime->receive.size()) {
        write_error(error, error_len, "invalid HIXL batch window count");
        return kParamInvalid;
    }
    std::set<size_t> used_slots;
    std::vector<std::pair<size_t, size_t>> local_ranges;
    local_ranges.reserve(count);
    plan.slot_ids.reserve(count);
    plan.descriptors.reserve(count);
    plan.entries.reserve(count);
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
        if (window.local_offset > std::numeric_limits<size_t>::max() - window.length ||
            (output_len != 0 && window.local_offset + window.length > output_len)) {
            write_error(error, error_len, "HIXL batch local range exceeds the output buffer");
            return kParamInvalid;
        }
        local_ranges.emplace_back(static_cast<size_t>(window.local_offset),
                                  static_cast<size_t>(window.local_offset + window.length));
    }
    std::sort(local_ranges.begin(), local_ranges.end());
    for (size_t index = 1; index < local_ranges.size(); ++index) {
        if (local_ranges[index].first < local_ranges[index - 1].second) {
            write_error(error, error_len, "HIXL batch local ranges overlap");
            return kParamInvalid;
        }
    }
    for (size_t index = 0; index < count; ++index) {
        const afs_hixl_batch_window_t &window = windows[index];
        const size_t slot_id = static_cast<size_t>(window.slot_id);
        HostSlot &slot = runtime->receive[slot_id];
        plan.slot_ids.push_back(slot_id);
        plan.descriptors.push_back(hixl::TransferOpDesc{
            reinterpret_cast<uintptr_t>(slot.address), window.remote_address, window.length});
        plan.entries.push_back(RequestEntry{
            slot_id, static_cast<size_t>(window.length), static_cast<size_t>(window.local_offset)});
    }
    plan.output_length = local_ranges.back().second;
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
// AFS_HIXL_TEST_FAULT_INJECTION so it can fail deterministically between Host
// slot reservation and the dispatch boundary. Production builds neither
// define nor call this helper.
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
void fail_before_dispatch_for_test() {
    if (std::getenv("AFS_HIXL_TEST_FAIL_BEFORE_DISPATCH") != nullptr) {
        throw std::bad_alloc();
    }
}
#endif

void release_batch(Runtime *runtime, const Request &request) {
    for (const RequestEntry &entry : request.entries) {
        HostSlot &slot = runtime->receive[entry.slot_id];
        slot.in_use = false;
        slot.quarantined = false;
    }
}

void copy_batch(Runtime *runtime, const Request &request, uint8_t *output, size_t output_len) {
    for (const RequestEntry &entry : request.entries) {
        const HostSlot &slot = runtime->receive[entry.slot_id];
        if (entry.local_offset > output_len || entry.length > output_len - entry.local_offset) {
            continue;
        }
        std::memcpy(output + entry.local_offset, slot.address, entry.length);
    }
}

uint32_t disconnect_peers(Runtime *runtime, char *error, size_t error_len) {
    for (const std::string &peer : runtime->peers) {
        const hixl::Status status = runtime->engine.Disconnect(hixl::AscendString(peer.c_str()), 1000);
        if (status != kSuccess) {
            write_error(error, error_len, "HIXL Disconnect did not prove a clean link close");
            return status;
        }
    }
    runtime->peers.clear();
    return kSuccess;
}

}  // namespace

extern "C" uint32_t afs_hixl_start(int32_t device_id,
                                    const char *engine_id,
                                    uint32_t device_listen_port,
                                    size_t window_bytes,
                                    size_t receive_slots,
                                    size_t export_slots,
                                    uint32_t transport,
                                    size_t buffer_pool_count,
                                    size_t buffer_pool_size_mib,
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
    const bool relay = transport == AFS_HIXL_TRANSPORT_HCCS_BUFFER_POOL;
    if (out_runtime == nullptr || out_hccs_domain_id == nullptr || device_id < 0 || engine_id == nullptr || engine_id[0] == '\0' ||
        device_listen_port > std::numeric_limits<uint16_t>::max() ||
        window_bytes == 0 || window_bytes > kMaxWindowBytes || receive_slots == 0 || export_slots == 0 ||
        receive_slots > 64 || export_slots > 64 ||
        receive_slots > std::numeric_limits<size_t>::max() - export_slots ||
        (receive_slots + export_slots) > kMaxPoolBytes / window_bytes ||
        (transport != AFS_HIXL_TRANSPORT_ROCE && !relay) ||
        (!relay && (buffer_pool_count != 0 || buffer_pool_size_mib != 0)) ||
        (relay && (buffer_pool_count == 0 || buffer_pool_count > 64 ||
                   buffer_pool_size_mib == 0 || buffer_pool_size_mib > 64 ||
                   buffer_pool_count * buffer_pool_size_mib > kMaxPoolBytes / (2U * 1024U * 1024U)))) {
        write_error(error, error_len, "invalid HIXL start arguments");
        return kParamInvalid;
    }
    *out_runtime = nullptr;
    if (relay) {
        const char *inter_hccs_disable = std::getenv("HCCL_INTER_HCCS_DISABLE");
        if (inter_hccs_disable != nullptr && std::strcmp(inter_hccs_disable, "FALSE") != 0) {
            write_error(error, error_len, "hccs-buffer-pool requires HCCL_INTER_HCCS_DISABLE unset or FALSE");
            return kParamInvalid;
        }
        const char *intra_roce = std::getenv("HCCL_INTRA_ROCE_ENABLE");
        if (intra_roce != nullptr && std::strcmp(intra_roce, "0") != 0) {
            write_error(error, error_len, "hccs-buffer-pool conflicts with HCCL_INTRA_ROCE_ENABLE; unset it or set 0");
            return kParamInvalid;
        }
    }
    std::unique_ptr<Runtime> holder;
    try {
        holder = std::make_unique<Runtime>();
        auto *runtime = holder.get();
        runtime->device_id = device_id;
        runtime->hccs_relay = relay;
        runtime->window_bytes = window_bytes;
        runtime->receive.resize(receive_slots);
        runtime->exports.resize(export_slots);
        runtime->requests.resize(receive_slots);
        uint32_t status = initialize_acl(runtime, error, error_len);
        if (status != kSuccess) {
            cleanup_failed_start(holder, out_runtime);
            return status;
        }
        if (relay) {
            // These are the A3 SoCs recognized by the paired HIXL generation.
            // Unknown devices fail before engine/HBM initialization.
            static const std::set<std::string> a3_socs = {
                "Ascend910_9391", "Ascend910_9381", "Ascend910_9392", "Ascend910_9382",
                "Ascend910_9372", "Ascend910_9362"};
            const char *soc = aclrtGetSocName();
            if (soc == nullptr || a3_socs.find(soc) == a3_socs.end()) {
                write_error(error, error_len, "hccs-buffer-pool requires a supported A3 device");
                cleanup_failed_start(holder, out_runtime);
                return kUnsupported;
            }
            int64_t domain = -1;
            const aclError domain_status = aclrtGetDeviceInfo(static_cast<uint32_t>(device_id), ACL_DEV_ATTR_SUPER_POD_ID, &domain);
            if (domain_status != ACL_SUCCESS || domain < 0) {
                write_error(error, error_len, "HCCS relay cannot verify the device super-pod identity");
                cleanup_failed_start(holder, out_runtime);
                return kUnsupported;
            }
            runtime->hccs_domain_id = static_cast<uint64_t>(domain);
        }
        std::map<hixl::AscendString, hixl::AscendString> options;
        options.emplace(hixl::AscendString(hixl::OPTION_AUTO_CONNECT), hixl::AscendString("0"));
        if (relay) {
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
        status = runtime->engine.Initialize(hixl::AscendString(engine_id), options);
        if (status != kSuccess) {
            write_error(error, error_len, "HIXL Initialize failed");
            cleanup_failed_start(holder, out_runtime);
            return status;
        }
        runtime->engine_initialized = true;
        status = register_pool(runtime, runtime->receive, error, error_len);
        if (status == kSuccess) {
            status = register_pool(runtime, runtime->exports, error, error_len);
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
    try {
        auto *runtime = static_cast<Runtime *>(opaque);
        uint32_t status = check_runtime(runtime, error, error_len);
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
        status = runtime->engine.Connect(hixl::AscendString(peer), static_cast<int32_t>(timeout_ms));
        if (status != kSuccess) {
            write_error(error, error_len, "HIXL Connect failed");
            return status;
        }
        runtime->peers.emplace(peer);
        return kSuccess;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception while connecting HIXL");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_prepare_export(void *opaque,
                                             uint64_t slot_id,
                                             uint64_t generation,
                                             const uint8_t *data,
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
        if (data == nullptr || out == nullptr || generation == 0 || len == 0 || len > runtime->window_bytes) {
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
        std::memcpy(slot->address, data, len);
        slot->in_use = true;
        out->slot_id = slot_id;
        out->generation = generation;
        out->address = reinterpret_cast<uint64_t>(slot->address);
        out->length = len;
        return kSuccess;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception preparing HIXL export");
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

extern "C" uint32_t afs_hixl_submit_read_batch(void *opaque,
                                               const char *peer,
                                               const afs_hixl_batch_window_t *windows,
                                               size_t count,
                                               void **out_request,
                                               uint32_t *dispatch_started,
                                               char *error,
                                               size_t error_len) {
    auto *runtime = static_cast<Runtime *>(opaque);
    bool invoked = false;
    bool reserved = false;
    Request *request = nullptr;
    BatchPlan plan;
    if (out_request == nullptr || dispatch_started == nullptr) {
        write_error(error, error_len, "invalid HIXL submit output arguments");
        return kParamInvalid;
    }
    *out_request = nullptr;
    *dispatch_started = 0;
    try {
        uint32_t status = check_runtime(runtime, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        if (runtime->hccs_relay || peer == nullptr || peer[0] == '\0') {
            write_error(error, error_len, "invalid HIXL batch READ arguments");
            return kParamInvalid;
        }
        if (runtime->peers.find(peer) == runtime->peers.end()) {
            write_error(error, error_len, "HIXL READ peer is not connected");
            return kNotConnected;
        }
        request = find_free_request(runtime);
        if (request == nullptr) {
            return kResourceExhausted;
        }
        status = plan_batch(runtime, windows, count, 0, plan, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        hixl::TransferArgs args{};
        hixl::TransferReq native_request = nullptr;
        // Entering TransferAsync is the uncertainty boundary. A failure or
        // exception after this point does not prove that no DMA was submitted.
        reserve_batch(runtime, plan);
        reserved = true;
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
        fail_before_dispatch_for_test();
#endif
        request->entries = plan.entries;
        request->length = plan.output_length;
        request->active = true;
        *dispatch_started = 1;
        invoked = true;
        request->dispatched_at = std::chrono::steady_clock::now();
        status = runtime->engine.TransferAsync(hixl::AscendString(peer), hixl::READ,
                                               plan.descriptors, args, native_request);
        request->handle = native_request;
        *out_request = native_request;
        if (status != kSuccess || native_request == nullptr) {
            quarantine_request(runtime, *request);
            write_error(error, error_len, "HIXL batch READ submission outcome is unknown; Host slots and engine are retained");
            return status == kSuccess ? kFailed : status;
        }
        return kSuccess;
    } catch (const std::exception &exception) {
        if (invoked && request != nullptr) {
            quarantine_request(runtime, *request);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        if (invoked && request != nullptr) {
            quarantine_request(runtime, *request);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
        write_error(error, error_len, "unknown exception submitting HIXL batch READ");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_read_sync_batch(void *opaque,
                                             const char *peer,
                                             const afs_hixl_batch_window_t *windows,
                                             size_t count,
                                             uint8_t *output,
                                             size_t output_len,
                                             uint32_t timeout_ms,
                                             uint32_t *dispatch_started,
                                             uint64_t *sdk_transfer_ns,
                                             uint64_t *output_copy_ns,
                                             char *error,
                                             size_t error_len) {
    auto *runtime = static_cast<Runtime *>(opaque);
    bool invoked = false;
    bool reserved = false;
    Request request;
    BatchPlan plan;
    if (dispatch_started == nullptr || sdk_transfer_ns == nullptr || output_copy_ns == nullptr) {
        write_error(error, error_len, "missing HIXL batch read timing output");
        return kParamInvalid;
    }
    *dispatch_started = 0;
    *sdk_transfer_ns = 0;
    *output_copy_ns = 0;
    try {
        const uint32_t checked = check_runtime(runtime, error, error_len);
        if (checked != kSuccess) { return checked; }
        if (!runtime->hccs_relay || peer == nullptr || peer[0] == '\0' ||
            output == nullptr || output_len == 0 ||
            timeout_ms == 0 || timeout_ms > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            write_error(error, error_len, "invalid HCCS relay batch READ arguments");
            return kParamInvalid;
        }
        if (runtime->peers.find(peer) == runtime->peers.end()) {
            write_error(error, error_len, "HIXL READ peer is not connected");
            return kNotConnected;
        }
        const uint32_t planned = plan_batch(runtime, windows, count, output_len, plan, error, error_len);
        if (planned != kSuccess) { return planned; }
        reserve_batch(runtime, plan);
        reserved = true;
#ifdef AFS_HIXL_TEST_FAULT_INJECTION
        fail_before_dispatch_for_test();
#endif
        request.entries = plan.entries;
        request.length = plan.output_length;
        request.active = true;
        // A non-successful synchronous call is still an uncertain submission.
        // Do not finalize the engine/HBM pools or free any Host window.
        *dispatch_started = 1;
        invoked = true;
        const auto sdk_start = std::chrono::steady_clock::now();
        const auto status = runtime->engine.TransferSync(hixl::AscendString(peer), hixl::READ,
                                                          plan.descriptors,
                                                          static_cast<int32_t>(timeout_ms));
        const auto sdk_end = std::chrono::steady_clock::now();
        if (status != kSuccess) {
            quarantine_request(runtime, request);
            write_error(error, error_len, "HCCS relay batch READ outcome is uncertain; Host windows, HBM and engine retained");
            return status;
        }
        *sdk_transfer_ns = elapsed_nanoseconds(sdk_start, sdk_end);
        const auto copy_start = std::chrono::steady_clock::now();
        copy_batch(runtime, request, output, output_len);
        const auto copy_end = std::chrono::steady_clock::now();
        *output_copy_ns = elapsed_nanoseconds(copy_start, copy_end);
        release_batch(runtime, request);
        return kSuccess;
    } catch (const std::exception &exception) {
        if (invoked) {
            quarantine_request(runtime, request);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        if (invoked) {
            quarantine_request(runtime, request);
        } else if (reserved) {
            release_reservation(runtime, plan);
        }
        write_error(error, error_len, "unknown exception during HCCS relay batch READ");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_poll_read(void *opaque,
                                        void *opaque_request,
                                        uint8_t *output,
                                        size_t output_len,
                                        uint32_t *out_state,
                                        uint64_t *sdk_transfer_ns,
                                        uint64_t *output_copy_ns,
                                        char *error,
                                        size_t error_len) {
    try {
        auto *runtime = static_cast<Runtime *>(opaque);
        uint32_t status = check_runtime(runtime, error, error_len);
        if (status != kSuccess || opaque_request == nullptr || out_state == nullptr ||
            sdk_transfer_ns == nullptr || output_copy_ns == nullptr) {
            if (status == kSuccess) {
                write_error(error, error_len, "invalid HIXL poll timing arguments");
                return kParamInvalid;
            }
            return status;
        }
        *sdk_transfer_ns = 0;
        *output_copy_ns = 0;
        Request *request = find_request(runtime, opaque_request);
        if (request == nullptr) {
            runtime->poisoned = true;
            write_error(error, error_len, "HIXL completion handle was already consumed or unknown");
            return kFailed;
        }
        hixl::TransferStatus transfer_status = hixl::TransferStatus::WAITING;
        status = runtime->engine.GetTransferStatus(request->handle, transfer_status);
        if (status != kSuccess) {
            quarantine_request(runtime, *request);
            write_error(error, error_len, "HIXL GetTransferStatus failed; request is retained");
            return status;
        }
        if (transfer_status == hixl::TransferStatus::WAITING) {
            *out_state = AFS_HIXL_POLL_WAITING;
            return kSuccess;
        }
        if (transfer_status == hixl::TransferStatus::COMPLETED) {
            if (output == nullptr || output_len < request->length) {
                quarantine_request(runtime, *request);
                write_error(error, error_len, "HIXL READ output buffer is too small");
                return kFailed;
            }
            *sdk_transfer_ns = elapsed_nanoseconds(request->dispatched_at,
                                                   std::chrono::steady_clock::now());
            const auto copy_start = std::chrono::steady_clock::now();
            copy_batch(runtime, *request, output, output_len);
            const auto copy_end = std::chrono::steady_clock::now();
            *output_copy_ns = elapsed_nanoseconds(copy_start, copy_end);
            release_batch(runtime, *request);
            request->active = false;
            request->entries.clear();
            *out_state = AFS_HIXL_POLL_COMPLETED;
            return kSuccess;
        }
        quarantine_request(runtime, *request);
        if (transfer_status == hixl::TransferStatus::TIMEOUT) {
            write_error(error, error_len, "HIXL READ completion timed out; request is retained");
            return kTimeout;
        }
        write_error(error, error_len, "HIXL READ completion failed; request is retained");
        return kFailed;
    } catch (const std::exception &exception) {
        write_error(error, error_len, exception.what());
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception polling HIXL READ");
        return kFailed;
    }
}

extern "C" uint32_t afs_hixl_shutdown(void *opaque, char *error, size_t error_len) {
    try {
        auto *runtime = static_cast<Runtime *>(opaque);
        if (runtime == nullptr || runtime->clean) {
            write_error(error, error_len, "invalid HIXL runtime for shutdown");
            return kParamInvalid;
        }
        if (runtime->poisoned ||
            std::any_of(runtime->receive.begin(), runtime->receive.end(), [](const HostSlot &slot) { return slot.in_use; }) ||
            std::any_of(runtime->exports.begin(), runtime->exports.end(), [](const HostSlot &slot) { return slot.in_use; }) ||
            std::any_of(runtime->requests.begin(), runtime->requests.end(), [](const Request &request) { return request.active; })) {
            write_error(error, error_len, "HIXL shutdown has not proved drain");
            return kFailed;
        }
        uint32_t status = disconnect_peers(runtime, error, error_len);
        if (status != kSuccess) {
            return status;
        }
        for (HostSlot &slot : runtime->receive) {
            if (slot.registered) {
                status = runtime->engine.DeregisterMem(slot.handle);
                if (status != kSuccess) {
                    write_error(error, error_len, "HIXL receive deregistration failed");
                    return status;
                }
                slot.registered = false;
            }
            if (slot.address != nullptr) {
                const aclError free_status = aclrtFreeHost(slot.address);
                if (free_status != ACL_SUCCESS) {
                    return acl_status(free_status, error, error_len, "aclrtFreeHost receive");
                }
                slot.address = nullptr;
            }
        }
        for (HostSlot &slot : runtime->exports) {
            if (slot.registered) {
                status = runtime->engine.DeregisterMem(slot.handle);
                if (status != kSuccess) {
                    write_error(error, error_len, "HIXL export deregistration failed");
                    return status;
                }
                slot.registered = false;
            }
            if (slot.address != nullptr) {
                const aclError free_status = aclrtFreeHost(slot.address);
                if (free_status != ACL_SUCCESS) {
                    return acl_status(free_status, error, error_len, "aclrtFreeHost export");
                }
                slot.address = nullptr;
            }
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
        return kFailed;
    } catch (...) {
        write_error(error, error_len, "unknown exception shutting down HIXL");
        return kFailed;
    }
}

extern "C" void afs_hixl_destroy(void *opaque) {
    auto *runtime = static_cast<Runtime *>(opaque);
    if (runtime != nullptr && runtime->clean) {
        delete runtime;
    }
}
