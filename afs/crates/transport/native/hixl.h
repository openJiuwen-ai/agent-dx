/*
 * AFS HIXL rH2H thin C ABI.
 *
 * Rust owns the lifecycle state machine and invokes this ABI only from its
 * dedicated ACL owner thread. The native side owns Host buffers and their
 * optional HIXL registrations. No C++ object or STL type crosses the boundary.
 */
#ifndef AFS_HIXL_H
#define AFS_HIXL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct afs_hixl_window {
    uint64_t slot_id;
    uint64_t generation;
    uint64_t address;
    uint64_t length;
} afs_hixl_window_t;

/* One remote range and its local receive or staging slot. Each batch must use
 * distinct free slots. Returned addresses follow descriptor order. */
typedef struct afs_hixl_batch_window {
    uint64_t slot_id;
    uint64_t remote_address;
    uint64_t length;
} afs_hixl_batch_window_t;

#define AFS_HIXL_MAX_BATCH_WINDOWS 64U

/* Bridge status for a permanently retained Fabric generation. No operation,
 * including shutdown, may retry SDK work on this generation. */
#define AFS_HIXL_QUARANTINED UINT32_MAX

#define AFS_HIXL_TRANSPORT_ROCE 1U
#define AFS_HIXL_TRANSPORT_HCCS 2U
#define AFS_HIXL_TRANSPORT_HCCS_FABRIC 3U

/* Increment when changing an exported signature, shared layout or mode contract. */
#define AFS_HIXL_ABI_VERSION 3U
uint32_t afs_hixl_abi_version(void);

/* All fields use UNSET for omission. Unfold is 0 for Host, 1 for AICPU. */
#define AFS_HIXL_FABRIC_OPTION_UNSET UINT32_MAX
typedef struct afs_hixl_fabric_options {
    uint32_t enable_aicpu_unfold;
    uint32_t task_streams;
    uint32_t max_capacity_tb;
    uint32_t start_address_tb;
    uint32_t max_transfer_count_per_batch;
} afs_hixl_fabric_options_t;

/* Pool dimensions are both zero for direct RoCE and HCCS FabricMem, or both
 * positive for relay. HCCS FabricMem requires device_listen_port = 0.
 * fabric_options must be NULL outside FabricMem. NULL keeps all SDK defaults.
 * RoCE relay requires HCCL_INTRA_ROCE_ENABLE=1,
 * HCCL_INTRA_PCIE_ENABLE=0 and HCCL_INTER_HCCS_DISABLE=TRUE before SDK startup.
 * On failure, a non-null out_runtime marks retained resources after incomplete
 * cleanup. It is quarantined and must never permit an unsupported fallback. */
uint32_t afs_hixl_start(int32_t device_id,
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
                        size_t error_len);

/* AFS_HIXL_QUARANTINED means the Fabric connection outcome was uncertain or
 * the generation was already quarantined. Other failures do not carry this
 * meaning. */
uint32_t afs_hixl_connect(void *runtime,
                          const char *peer,
                          uint32_t timeout_ms,
                          char *error,
                          size_t error_len);

/* Pins a free export slot without copying into it and returns its address.
 * The caller owns the first `len` bytes until afs_hixl_release_export; the slot
 * stays allocated for that whole time because shutdown refuses a pinned slot. */
uint32_t afs_hixl_reserve_export(void *runtime,
                                 uint64_t slot_id,
                                 uint64_t generation,
                                 size_t len,
                                 afs_hixl_window_t *out,
                                 char *error,
                                 size_t error_len);

uint32_t afs_hixl_release_export(void *runtime,
                                 uint64_t slot_id,
                                 uint64_t generation,
                                 char *error,
                                 size_t error_len);

/* All windows are submitted through one TransferSync call.
 * On success every slot stays pinned and `slot_addresses` receives one address
 * per window, in window order, for the caller to copy from before it calls
 * afs_hixl_release_receives.  dispatch_started distinguishes pre-entry
 * rejection from an uncertain native outcome; failure after entry retains Host
 * windows, any relay HBM pools, and the engine. */
uint32_t afs_hixl_read_sync_batch(void *runtime,
                                  const char *peer,
                                  const afs_hixl_batch_window_t *windows,
                                  size_t count,
                                  uint64_t *slot_addresses,
                                  uint32_t timeout_ms,
                                  uint32_t *dispatch_started,
                                  uint64_t *sdk_transfer_ns,
                                  char *error,
                                  size_t error_len);

/* Copies `bytes` into native Host slots, submits one WRITE and waits for
 * completion. Rust payload storage is never used by DMA. On failure after
 * dispatch_started is set, the slots and engine are retained. */
uint32_t afs_hixl_write_batch(void *runtime,
                              const char *peer,
                              const afs_hixl_batch_window_t *windows,
                              size_t count,
                              const uint8_t *bytes,
                              size_t len,
                              uint32_t timeout_ms,
                              uint32_t *dispatch_started,
                              char *error,
                              size_t error_len);

/* Frees receive slots a completed READ left pinned.  Every slot must be pinned;
 * the call validates the whole array before changing any slot. */
uint32_t afs_hixl_release_receives(void *runtime,
                                   const uint64_t *slot_ids,
                                   size_t count,
                                   char *error,
                                   size_t error_len);

/* AFS_HIXL_QUARANTINED permanently blocks Fabric cleanup. Other failures may
 * be retried after pending work drains or a retryable cleanup error clears. */
uint32_t afs_hixl_shutdown(void *runtime, char *error, size_t error_len);

/* Only call after afs_hixl_shutdown succeeds.  An uncertain runtime must be
 * retained and never passed to this function. */
void afs_hixl_destroy(void *runtime);

#ifdef __cplusplus
}
#endif

#endif
