/*
 * AFS HIXL rH2H thin C ABI.
 *
 * Rust owns the lifecycle state machine and invokes this ABI only from its
 * dedicated ACL owner thread.  The native side owns aclrtMallocHost buffers,
 * optional HIXL registrations and request handles.  No C++ object or STL
 * type crosses the boundary.
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

/* One remote read range and its destination receive slot. Each batch must use
 * distinct free slots. Returned addresses follow descriptor order. */
typedef struct afs_hixl_batch_window {
    uint64_t slot_id;
    uint64_t remote_address;
    uint64_t length;
} afs_hixl_batch_window_t;

#define AFS_HIXL_MAX_BATCH_WINDOWS 64U

/* poll states: waiting, completed, failed, timeout, unknown */
#define AFS_HIXL_POLL_WAITING 0U
#define AFS_HIXL_POLL_COMPLETED 1U
#define AFS_HIXL_POLL_FAILED 2U
#define AFS_HIXL_POLL_TIMEOUT 3U
#define AFS_HIXL_POLL_UNKNOWN 4U

#define AFS_HIXL_TRANSPORT_ROCE 1U
#define AFS_HIXL_TRANSPORT_HCCS_BUFFER_POOL 2U

/* On failure, a non-null out_runtime marks retained resources after incomplete
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
                        uint64_t *out_hccs_domain_id,
                        void **out_runtime,
                        char *error,
                        size_t error_len);

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

/* Submits every window in one SDK batch.  Validation happens for the whole
 * array before any slot is reserved; a failure after native entry retains
 * every listed slot as quarantined. */
uint32_t afs_hixl_submit_read_batch(void *runtime,
                                    const char *peer,
                                    const afs_hixl_batch_window_t *windows,
                                    size_t count,
                                    void **out_request,
                                    uint32_t *dispatch_started,
                                    char *error,
                                    size_t error_len);

/* On AFS_HIXL_POLL_COMPLETED every slot stays pinned, exactly as for
 * afs_hixl_read_sync_batch; `slot_count` must equal the submitted windows. */
uint32_t afs_hixl_poll_read(void *runtime,
                            void *request,
                            uint64_t *slot_addresses,
                            size_t slot_count,
                            uint32_t *out_state,
                            uint64_t *sdk_transfer_ns,
                            char *error,
                            size_t error_len);

/* HCCS relay only.  All windows are submitted through one TransferSync call.
 * On success every slot stays pinned and `slot_addresses` receives one address
 * per window, in window order, for the caller to copy from before it calls
 * afs_hixl_release_receives.  dispatch_started distinguishes pre-entry
 * rejection from an uncertain native outcome; failure after entry retains Host
 * windows, HBM pools and the engine. */
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

/* Frees receive slots a completed READ left pinned.  Every slot must be pinned;
 * the call validates the whole array before changing any slot. */
uint32_t afs_hixl_release_receives(void *runtime,
                                   const uint64_t *slot_ids,
                                   size_t count,
                                   char *error,
                                   size_t error_len);

uint32_t afs_hixl_shutdown(void *runtime, char *error, size_t error_len);

/* Only call after afs_hixl_shutdown succeeds.  An uncertain runtime must be
 * retained and never passed to this function. */
void afs_hixl_destroy(void *runtime);

#ifdef __cplusplus
}
#endif

#endif
