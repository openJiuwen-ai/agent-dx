/*
 * AFS RDMA native shim public C ABI.
 *
 * This header is consumed only by Rust FFI in crates/transport/src/rdma.rs.
 * It is not a filesystem API and not a daemon API: callers still send file
 * commands through gRPC/proto; this C layer only manages libibverbs endpoint
 * resources and one-sided data movement.
 */
#ifndef AFS_RDMA_H
#define AFS_RDMA_H
#include <stddef.h>
#include <stdint.h>
#define AFS_RDMA_INFO_BYTES 38U
#define AFS_RDMA_READ 1
#define AFS_RDMA_WRITE 2
int afs_rdma_open(const char *, uint32_t, void **, size_t *, char *, size_t);
int afs_rdma_info(void *, uint8_t *, uint32_t, char *, size_t);
int afs_rdma_connect(void *, const uint8_t *, uint32_t, char *, size_t);
int afs_rdma_prepare_probe(void *, char *, size_t);
int afs_rdma_send_probe(void *, uint32_t, char *, size_t);
int afs_rdma_wait_probe(void *, uint32_t, char *, size_t);
int afs_rdma_put_local(void *, const uint8_t *, uint32_t, char *, size_t);
int afs_rdma_get_local(void *, uint8_t *, uint32_t, char *, size_t);
int afs_rdma_transfer(void *, int, uint32_t, uint32_t, char *, size_t);
/* A nonzero result transfers the still-owned endpoint back to the caller.
 * The failed step is preserved; successful prior steps are cleared. */
enum afs_rdma_close_result {
    AFS_RDMA_RELEASED = 0,
    AFS_RDMA_RETAINED_QP = 1,
    AFS_RDMA_RETAINED_MR = 2,
    AFS_RDMA_RETAINED_CQ = 3,
    AFS_RDMA_RETAINED_PD = 4,
    AFS_RDMA_RETAINED_CONTEXT = 5,
    AFS_RDMA_RETAINED_DRAIN = 6,
};
int afs_rdma_close(void *, size_t *retained_registered_bytes);
#endif
