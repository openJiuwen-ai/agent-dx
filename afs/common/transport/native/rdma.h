/*
 * AFS RDMA native shim public C ABI.
 *
 * This header is consumed only by Rust FFI in common/transport/src/rdma.rs.
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
void *afs_rdma_open(const char *, uint32_t, char *, size_t);
int afs_rdma_info(void *, uint8_t *, uint32_t, char *, size_t);
int afs_rdma_connect(void *, const uint8_t *, uint32_t, char *, size_t);
int afs_rdma_prepare_probe(void *, char *, size_t);
int afs_rdma_send_probe(void *, uint32_t, char *, size_t);
int afs_rdma_wait_probe(void *, uint32_t, char *, size_t);
int afs_rdma_put_local(void *, const uint8_t *, uint32_t, char *, size_t);
int afs_rdma_get_local(void *, uint8_t *, uint32_t, char *, size_t);
int afs_rdma_transfer(void *, int, uint32_t, uint32_t, char *, size_t);
void afs_rdma_close(void *);
#endif
