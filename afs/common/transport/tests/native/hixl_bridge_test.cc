// Exercises the actual C ABI bridge against a fake external CANN boundary.
// Official SDK headers remain the ABI contract; no device is initialized.
#include "../../native/hixl.h"
#include <acl/acl_rt.h>
#include <hixl/hixl.h>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <stdexcept>
#include <sys/resource.h>
#include <thread>
#include <vector>

namespace {
size_t allocations = 0;
size_t submissions = 0;
bool refuse_submit = false;
bool device_roce = false;
void *receive_address = nullptr;
uintptr_t remote_address = 0;
bool fail_poll = false;
size_t queries = 0;
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
    *address = std::malloc(length); assert(*address != nullptr); ++allocations; return ACL_SUCCESS;
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
    ++registrations;
    assert(!relay_mode);
    if (retain_failed_start && registrations == 2) { return UNSUPPORTED; }
    assert(type == MEM_HOST); assert(desc.addr != 0); handle = reinterpret_cast<void *>(desc.addr); return SUCCESS;
}
Status Hixl::DeregisterMem(MemHandle) { return retain_failed_start ? FAILED : SUCCESS; }
Status Hixl::Connect(const AscendString &, int32_t) { return SUCCESS; }
Status Hixl::Disconnect(const AscendString &, int32_t) { return SUCCESS; }
Status Hixl::TransferAsync(const AscendString &, TransferOp operation, const std::vector<TransferOpDesc> &ops, const TransferArgs &, TransferReq &request) {
    assert(!relay_mode);
    assert(operation == READ); assert(!ops.empty()); ++submissions;
    if (batch_mode) {
        batch_descriptors = ops.size();
        batch_bytes = 0;
        for (const auto &op : ops) { batch_bytes += op.len; }
    } else {
        assert(ops.size() == 1);
    }
    receive_address = reinterpret_cast<void *>(ops[0].local_addr); remote_address = ops[0].remote_addr;
    if (refuse_submit) { request = nullptr; return FAILED; }
    for (const auto &op : ops) {
        std::memcpy(reinterpret_cast<void *>(op.local_addr), reinterpret_cast<void *>(op.remote_addr), op.len);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    request = reinterpret_cast<void *>(1); return SUCCESS;
}
Status Hixl::TransferSync(const AscendString &, TransferOp operation, const std::vector<TransferOpDesc> &ops, int32_t timeout) {
    assert(relay_mode); assert(operation == READ); assert(!ops.empty());
    assert(registrations == 0); assert(timeout == 123);
    if (batch_mode) {
        batch_descriptors = ops.size();
        batch_bytes = 0;
        for (const auto &op : ops) { batch_bytes += op.len; }
    } else {
        assert(ops.size() == 1);
    }
    ++sync_calls;
    if (throw_sync) { throw std::runtime_error("uncertain relay submission"); }
    if (sync_status != SUCCESS) { return sync_status; }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    receive_address = reinterpret_cast<void *>(ops[0].local_addr);
    remote_address = ops[0].remote_addr;
    for (const auto &op : ops) {
        std::memcpy(reinterpret_cast<void *>(op.local_addr), reinterpret_cast<void *>(op.remote_addr), op.len);
    }
    return SUCCESS;
}
Status Hixl::GetTransferStatus(const TransferReq &, TransferStatus &status) {
    ++queries; status = fail_poll ? TransferStatus::FAILED : TransferStatus::COMPLETED; return SUCCESS;
}
}

int main(int argc, char **argv) {
    assert(argc == 2);
    rlimit no_core{0, 0}; assert(setrlimit(RLIMIT_CORE, &no_core) == 0);
    // Each fake-boundary scenario owns its HCCL inputs independently of the
    // machine's real transport settings. Conflict scenarios set them below.
    assert(unsetenv("HCCL_INTRA_ROCE_ENABLE") == 0);
    assert(unsetenv("HCCL_INTER_HCCS_DISABLE") == 0);
    char error[512] = {};
    void *runtime = nullptr;
    uint64_t actual_domain = 0;
    const std::string scenario(argv[1]);
    relay_mode = scenario.find("hccs-") == 0;
    if (scenario == "roce-listen-port" || scenario == "hccs-listen-port") { expected_listen_port = 27666; }
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
            relay_mode ? AFS_HIXL_TRANSPORT_HCCS_BUFFER_POOL : AFS_HIXL_TRANSPORT_ROCE,
            relay_mode ? count : 0, relay_mode ? size_mib : 0, &actual_domain, &runtime, error, sizeof(error));
    };
    auto start_on = [&](int32_t device_id, const char *engine_id, uint32_t listen_port, void **out) {
        expected_listen_port = listen_port;
        return afs_hixl_start(device_id, engine_id, listen_port, 4, 1, 1,
            AFS_HIXL_TRANSPORT_ROCE, 0, 0, &actual_domain, out, error, sizeof(error));
    };
    if (scenario == "hccs-invalid-pool" || scenario == "roce-invalid-listen-port" ||
        scenario == "hccs-invalid-listen-port") {
        assert(start(64, 8) != 0); assert(runtime == nullptr); assert(allocations == 0); return 0;
    }
    if (scenario == "hccs-unsupported-soc" || scenario == "hccs-null-soc" || scenario == "hccs-roce-conflict" ||
        scenario == "hccs-unknown-domain" || scenario == "hccs-negative-domain" || scenario == "hccs-inter-roce-conflict") {
        assert(start() != 0); assert(runtime == nullptr); assert(allocations == 0); return 0;
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
    if (scenario == "roce-batch-100mib" || scenario == "hccs-batch-100mib") {
        batch_mode = true;
        constexpr size_t window_bytes = 4u * 1024u * 1024u;
        constexpr size_t window_count = 25;
        constexpr size_t total_bytes = window_count * window_bytes;
        assert(afs_hixl_start(0, "127.0.0.1:7920", expected_listen_port, window_bytes, window_count, 1,
            relay_mode ? AFS_HIXL_TRANSPORT_HCCS_BUFFER_POOL : AFS_HIXL_TRANSPORT_ROCE,
            relay_mode ? 4 : 0, relay_mode ? 8 : 0, &actual_domain, &runtime, error, sizeof(error)) == 0);
        assert(allocations == window_count + 1);
        assert(afs_hixl_connect(runtime, "127.0.0.1:7921", 1000, error, sizeof(error)) == 0);

        std::vector<uint8_t> source_bytes(total_bytes);
        for (size_t index = 0; index < total_bytes; ++index) {
            source_bytes[index] = static_cast<uint8_t>(index % 251u);
        }
        std::vector<afs_hixl_batch_window_t> windows(window_count);
        for (size_t index = 0; index < window_count; ++index) {
            windows[index].slot_id = index;
            windows[index].remote_address =
                reinterpret_cast<uintptr_t>(source_bytes.data() + index * window_bytes);
            windows[index].length = window_bytes;
            windows[index].local_offset = index * window_bytes;
        }
        std::vector<uint8_t> output(total_bytes, 0);
        char rejection[512] = {};
        const size_t sdk_baseline = relay_mode ? sync_calls : submissions;
        auto sdk_calls = [&]() { return relay_mode ? sync_calls : submissions; };
        auto rejects = [&](const std::vector<afs_hixl_batch_window_t> &bad) {
            uint32_t dispatch = 999;
            if (relay_mode) {
                uint64_t sdk_ns = 17;
                uint64_t copy_ns = 17;
                assert(afs_hixl_read_sync_batch(runtime, "127.0.0.1:7921", bad.data(), bad.size(),
                    output.data(), output.size(), 123, &dispatch, &sdk_ns, &copy_ns,
                    rejection, sizeof(rejection)) != 0);
                assert(sdk_ns == 0 && copy_ns == 0);
            } else {
                void *rejected = nullptr;
                assert(afs_hixl_submit_read_batch(runtime, "127.0.0.1:7921", bad.data(), bad.size(),
                    &rejected, &dispatch, rejection, sizeof(rejection)) != 0);
                assert(rejected == nullptr);
            }
            assert(dispatch == 0);
            assert(sdk_calls() == sdk_baseline);
        };

        rejects({});
        auto duplicate = windows;
        duplicate[1].slot_id = duplicate[0].slot_id;
        rejects(duplicate);
        auto overlap = windows;
        overlap[1].local_offset = 0;
        rejects(overlap);
        auto outside = windows;
        outside[0].slot_id = window_count;
        rejects(outside);
        auto oversized = windows;
        oversized[0].length = window_bytes + 1;
        rejects(oversized);

        uint32_t dispatch = 0;
        if (relay_mode) {
            uint64_t sdk_ns = 0;
            uint64_t copy_ns = 0;
            assert(afs_hixl_read_sync_batch(runtime, "127.0.0.1:7921", windows.data(), windows.size(),
                output.data(), output.size(), 123, &dispatch, &sdk_ns, &copy_ns,
                error, sizeof(error)) == 0);
            assert(dispatch == 1);
            assert(sync_calls == sdk_baseline + 1);
            assert(std::memcmp(output.data(), source_bytes.data(), total_bytes) == 0);
            assert(sdk_ns >= 4000000);
            assert(copy_ns > 0);
        } else {
            void *request = nullptr;
            assert(afs_hixl_submit_read_batch(runtime, "127.0.0.1:7921", windows.data(), windows.size(),
                &request, &dispatch, error, sizeof(error)) == 0);
            assert(dispatch == 1 && request != nullptr);
            assert(submissions == sdk_baseline + 1);
            uint32_t state = 0;
            uint64_t sdk_ns = 0;
            uint64_t copy_ns = 0;
            assert(afs_hixl_poll_read(runtime, request, output.data(), output.size(), &state,
                &sdk_ns, &copy_ns, error, sizeof(error)) == 0);
            assert(state == AFS_HIXL_POLL_COMPLETED);
            assert(std::memcmp(output.data(), source_bytes.data(), total_bytes) == 0);
            assert(sdk_ns >= 4000000 && copy_ns > 0);
        }
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
    void *request = nullptr;
    uint32_t dispatch_started = 0;
    if (relay_mode) {
        assert(registrations == 0);
        assert(actual_domain == 7);
        char output[4] = {};
        uint64_t sdk_transfer_ns = 17;
        uint64_t output_copy_ns = 17;
        const uint32_t timeout = scenario == "hccs-pre-submit-invalid" ? 0 : 123;
        sync_status = scenario == "hccs-timeout" ? hixl::TIMEOUT :
            scenario == "hccs-failed" ? hixl::FAILED : hixl::SUCCESS;
        throw_sync = scenario == "hccs-exception";
        const auto result = afs_hixl_read_sync(runtime, 0, "127.0.0.1:7921", reinterpret_cast<uintptr_t>(source),
            reinterpret_cast<uint8_t *>(output), sizeof(output), timeout, &dispatch_started,
            &sdk_transfer_ns, &output_copy_ns, error, sizeof(error));
        if (scenario == "hccs-pre-submit-invalid") {
            assert(result != 0); assert(dispatch_started == 0); assert(sync_calls == 0);
            assert(sdk_transfer_ns == 0 && output_copy_ns == 0);
        } else if (scenario == "hccs-completed" || scenario == "hccs-listen-port") {
            assert(result == 0); assert(dispatch_started == 1); assert(sync_calls == 1);
            assert(std::memcmp(output, source, 4) == 0); assert(queries == 0);
            assert(sdk_transfer_ns >= 4000000);
            assert(output_copy_ns < sdk_transfer_ns);
        } else {
            assert(result != 0); assert(dispatch_started == 1); assert(sync_calls == 1);
            assert(sdk_transfer_ns == 0 && output_copy_ns == 0);
            assert(output[0] == 0); assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
            assert(allocations == 2); assert(finalizations == 0); return 0;
        }
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime); assert(allocations == 0); assert(finalizations == 1); return 0;
    }
    if (std::string(argv[1]) == "pre-submit-invalid") {
        dispatch_started = 999;
        assert(afs_hixl_submit_read(runtime, 0, "127.0.0.1:7921", 0, 4, &request, &dispatch_started, error, sizeof(error)) != 0);
        assert(dispatch_started == 0); assert(submissions == 0);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
        afs_hixl_destroy(runtime); assert(allocations == 0); return 0;
    }
    refuse_submit = std::string(argv[1]) == "unknown-submit";
    uint32_t status = afs_hixl_submit_read(runtime, 0, "127.0.0.1:7921", reinterpret_cast<uintptr_t>(source), 4, &request, &dispatch_started, error, sizeof(error));
    assert(dispatch_started == 1);
    if (refuse_submit) {
        assert(status != 0);
        refuse_submit = false;
        assert(afs_hixl_submit_read(runtime, 0, "127.0.0.1:7921", reinterpret_cast<uintptr_t>(source), 4, &request, &dispatch_started, error, sizeof(error)) != 0);
        assert(dispatch_started == 0);
        assert(submissions == 1);
        assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        assert(allocations == 2);
        // Quarantined resources intentionally survive until this test process exits.
        return 0;
    }
    assert(status == 0); assert(remote_address == reinterpret_cast<uintptr_t>(source));
    char output[4] = {}; uint32_t state = 0;
    uint64_t sdk_transfer_ns = 17;
    uint64_t output_copy_ns = 17;
    fail_poll = std::string(argv[1]) == "failed-poll";
    if (fail_poll) {
        assert(afs_hixl_poll_read(runtime, request, reinterpret_cast<uint8_t *>(output), sizeof(output), &state,
            &sdk_transfer_ns, &output_copy_ns, error, sizeof(error)) != 0);
        assert(sdk_transfer_ns == 0 && output_copy_ns == 0);
        assert(afs_hixl_poll_read(runtime, request, reinterpret_cast<uint8_t *>(output), sizeof(output), &state,
            &sdk_transfer_ns, &output_copy_ns, error, sizeof(error)) != 0);
        assert(queries == 1); assert(afs_hixl_shutdown(runtime, error, sizeof(error)) != 0);
        assert(allocations == 2); return 0;
    }
    assert(afs_hixl_poll_read(runtime, request, reinterpret_cast<uint8_t *>(output), sizeof(output), &state,
        &sdk_transfer_ns, &output_copy_ns, error, sizeof(error)) == 0);
    assert(state == AFS_HIXL_POLL_COMPLETED); assert(std::memcmp(output, source, 4) == 0);
    assert(sdk_transfer_ns >= 4000000);
    assert(output_copy_ns < sdk_transfer_ns);
    assert(afs_hixl_shutdown(runtime, error, sizeof(error)) == 0);
    afs_hixl_destroy(runtime); assert(allocations == 0);
}
