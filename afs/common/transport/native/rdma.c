/*
 * Linux 单边 RDMA 的 libibverbs C 适配层，由 Rust 会话持有其资源。
 *
 * native 指本机 C ABI，不是 Native FS、NFS 或另一个 daemon。
 * Rust 侧封装资源所有权，Node 的 gRPC Handler 决定文件操作；本层只创建
 * QP（队列对）、MR（注册内存）、CQ（完成队列），并等待单边传输完成。
 * CQ 完成不等于文件已经写入，更不等于已经 fsync；文件语义由上层负责。
 * 阻塞的 CQ 轮询必须放在 Tokio blocking 线程中；同一 endpoint 不可并发使用。
 */
#include "rdma.h"
#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define AFS_RDMA_WR_DATA 1ULL
#define AFS_RDMA_WR_PROBE_SEND 0xAF500001ULL
#define AFS_RDMA_WR_PROBE_RECV 0xAF500002ULL
#define AFS_RDMA_PROBE_MAGIC 0xAF5A1001U
#define AFS_RDMA_MAX_CAPACITY (4U*1024U*1024U)

/* One endpoint owns the verbs lifecycle for a single AFS RDMA session:
 * context -> PD -> CQ -> QP -> MR(buffer). The remote_* fields are filled after
 * both sides exchange descriptors via gRPC control and QP enters RTS.
 */
struct endpoint {
    struct ibv_context *context;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *mr;
    uint8_t *buffer;
    uint32_t capacity;
    union ibv_gid gid;
    uint8_t gid_index;
    uint16_t lid;
    uint64_t remote_address;
    uint32_t remote_key, remote_capacity;
    bool connected, poisoned;
    bool probe_recv_posted, probe_sent, probe_received;
};

static int error(char *out, size_t size, const char *message) {
    if (out && size) snprintf(out, size, "%s (errno=%d)", message, errno);
    return -1;
}
static void put32(uint8_t *out, uint32_t value) { value=htonl(value); memcpy(out,&value,4); }
static uint32_t get32(const uint8_t *in) { uint32_t v; memcpy(&v,in,4); return ntohl(v); }
static uint64_t monotonic_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000U + (uint64_t)ts.tv_nsec/1000000U;
}
static void poison(struct endpoint *e);

/* GID indices are local device state, not a universal constant. RoCE on an
 * IPv4 cluster must advertise the IPv4-mapped GID and use that same index in
 * the QP's source route. Prefer a global IPv6 GID when no IPv4 GID exists;
 * link-local is a last choice for a genuinely shared IPv6 link. */
static bool ipv4_gid(const union ibv_gid *gid) {
    static const uint8_t prefix[12]={0,0,0,0,0,0,0,0,0,0,0xff,0xff};
    return !memcmp(gid->raw,prefix,sizeof(prefix));
}
static int select_gid(struct endpoint *e, int table_length) {
    int best_rank=0;
    /* ibv_global_route.sgid_index is uint8_t even when a provider advertises
     * a larger table; never truncate a selected index into another GID. */
    for (int i=0;i<table_length && i<=UINT8_MAX;i++) {
        union ibv_gid gid;
        if (ibv_query_gid(e->context,1,i,&gid)) continue;
        static const uint8_t zero[16]={0};
        if (!memcmp(gid.raw,zero,sizeof(zero))) continue;
        bool link_local=gid.raw[0]==0xfe && (gid.raw[1]&0xc0)==0x80;
        int rank=ipv4_gid(&gid)?3:(link_local?1:2);
        if (rank>best_rank) { e->gid=gid; e->gid_index=(uint8_t)i; best_rank=rank; }
    }
    if (!best_rank) { errno=EADDRNOTAVAIL; return -1; }
    char address[INET6_ADDRSTRLEN];
    if (!inet_ntop(AF_INET6,e->gid.raw,address,sizeof(address))) return -1;
    fprintf(stderr,"AFS_RDMA_GID index=%u address=%s family=%s\n",
        e->gid_index,address,ipv4_gid(&e->gid)?"ipv4":"ipv6");
    return 0;
}

void afs_rdma_close(void *opaque) {
    struct endpoint *e=opaque;
    if (!e) return;
    /* QP must stop accessing MR before the MR/buffer can be released. */
    if (e->qp && ibv_destroy_qp(e->qp)) {
        fprintf(stderr,"AFS_RDMA_TEARDOWN failed destroy_qp; retaining memory\n");
        return; /* A leak on exceptional teardown is safer than DMA into freed memory. */
    }
    if (e->mr && ibv_dereg_mr(e->mr)) {
        fprintf(stderr,"AFS_RDMA_TEARDOWN failed dereg_mr; retaining memory\n");
        return;
    }
    if (e->cq) ibv_destroy_cq(e->cq);
    if (e->pd) ibv_dealloc_pd(e->pd);
    if (e->context) ibv_close_device(e->context);
    free(e->buffer); free(e);
}

void *afs_rdma_open(const char *device, uint32_t capacity, char *err, size_t errlen) {
    if (!capacity || capacity>AFS_RDMA_MAX_CAPACITY) { error(err,errlen,"capacity must be 1..4MiB"); return NULL; }
    struct endpoint *e=calloc(1,sizeof(*e));
    if (!e) { error(err,errlen,"endpoint allocation"); return NULL; }
    int count=0;
    struct ibv_device **devices=ibv_get_device_list(&count);
    if (!devices) goto failed;
    for (int i=0;i<count;i++) {
        if (!strcmp(device,ibv_get_device_name(devices[i]))) {
            e->context=ibv_open_device(devices[i]); break;
        }
    }
    ibv_free_device_list(devices);
    if (!e->context) goto failed;
    struct ibv_port_attr port;
    if (ibv_query_port(e->context,1,&port) || port.state != IBV_PORT_ACTIVE ||
        select_gid(e,port.gid_tbl_len)) goto failed;
    e->lid=port.lid;
    e->capacity=capacity;
    e->pd=ibv_alloc_pd(e->context);
    e->cq=ibv_create_cq(e->context,16,NULL,NULL,0);
    size_t allocation=((size_t)capacity+4095U)&~(size_t)4095U;
    e->buffer=aligned_alloc(4096,allocation);
    if (!e->pd || !e->cq || !e->buffer) goto failed;
    memset(e->buffer,0,allocation);
    e->mr=ibv_reg_mr(e->pd,e->buffer,capacity,
        IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE);
    if (!e->mr) goto failed;
    struct ibv_qp_init_attr init={
        .send_cq=e->cq,.recv_cq=e->cq,.qp_type=IBV_QPT_RC,
        .cap={.max_send_wr=4,.max_recv_wr=1,.max_send_sge=1,.max_recv_sge=1},
    };
    e->qp=ibv_create_qp(e->pd,&init);
    if (!e->qp) goto failed;
    struct ibv_qp_attr attr={
        .qp_state=IBV_QPS_INIT,.pkey_index=0,.port_num=1,
        .qp_access_flags=IBV_ACCESS_REMOTE_READ|IBV_ACCESS_REMOTE_WRITE,
    };
    if (ibv_modify_qp(e->qp,&attr,IBV_QP_STATE|IBV_QP_PKEY_INDEX|
        IBV_QP_PORT|IBV_QP_ACCESS_FLAGS)) goto failed;
    return e;
failed:
    error(err,errlen,"open RDMA endpoint failed (device/port/resources)");
    afs_rdma_close(e); return NULL;
}

int afs_rdma_info(void *opaque,uint8_t *out,uint32_t len,char *err,size_t errlen) {
    struct endpoint *e=opaque;
    if (!e || !out || len!=AFS_RDMA_INFO_BYTES) return error(err,errlen,"invalid descriptor length");
    put32(out,e->qp->qp_num);
    uint16_t lid=htons(e->lid); memcpy(out+4,&lid,2);
    memcpy(out+6,&e->gid,16);
    uint64_t address=(uintptr_t)e->buffer;
    put32(out+22,(uint32_t)(address>>32)); put32(out+26,(uint32_t)address);
    put32(out+30,e->mr->rkey); put32(out+34,e->capacity);
    return 0;
}

int afs_rdma_connect(void *opaque,const uint8_t *peer,uint32_t len,char *err,size_t errlen) {
    struct endpoint *e=opaque;
    if (!e || !peer || len!=AFS_RDMA_INFO_BYTES || e->connected || e->poisoned)
        return error(err,errlen,"invalid connection state/descriptor");
    uint32_t capacity=get32(peer+34);
    if (!capacity || capacity>AFS_RDMA_MAX_CAPACITY || !get32(peer))
        return error(err,errlen,"invalid peer capacity/QP");
    e->remote_address=((uint64_t)get32(peer+22)<<32)|get32(peer+26);
    e->remote_key=get32(peer+30); e->remote_capacity=capacity;
    struct ibv_qp_attr attr={0};
    attr.qp_state=IBV_QPS_RTR; attr.path_mtu=IBV_MTU_1024;
    attr.dest_qp_num=get32(peer); attr.rq_psn=0;
    attr.max_dest_rd_atomic=1; attr.min_rnr_timer=12;
    attr.ah_attr.is_global=1; attr.ah_attr.port_num=1;
    uint16_t lid; memcpy(&lid,peer+4,2); attr.ah_attr.dlid=ntohs(lid);
    memcpy(&attr.ah_attr.grh.dgid,peer+6,16);
    if (ipv4_gid(&e->gid)!=ipv4_gid(&attr.ah_attr.grh.dgid)) {
        errno=EAFNOSUPPORT;
        e->poisoned=true;
        return error(err,errlen,"RDMA peer GID address family differs from local selected GID");
    }
    attr.ah_attr.grh.hop_limit=1; attr.ah_attr.grh.sgid_index=e->gid_index;
    if (ibv_modify_qp(e->qp,&attr,IBV_QP_STATE|IBV_QP_AV|IBV_QP_PATH_MTU|
        IBV_QP_DEST_QPN|IBV_QP_RQ_PSN|IBV_QP_MAX_DEST_RD_ATOMIC|IBV_QP_MIN_RNR_TIMER)) goto failed;
    memset(&attr,0,sizeof(attr));
    attr.qp_state=IBV_QPS_RTS; attr.timeout=10; attr.retry_cnt=2;
    attr.rnr_retry=2; attr.max_rd_atomic=1;
    if (ibv_modify_qp(e->qp,&attr,IBV_QP_STATE|IBV_QP_TIMEOUT|IBV_QP_RETRY_CNT|
        IBV_QP_RNR_RETRY|IBV_QP_SQ_PSN|IBV_QP_MAX_QP_RD_ATOMIC)) goto failed;
    e->connected=true; return 0;
failed:
    e->poisoned=true;
    return error(err,errlen,"QP connect transition failed");
}

int afs_rdma_prepare_probe(void *opaque,char *err,size_t errlen) {
    /*
     * 参考 3FS：server 在公开 server_info 前先投递一个 RECV。client 随后发
     * 0-byte SEND_WITH_IMM，server 用接收 CQ completion 证明真实 RDMA 通道已通。
     * 这里不占用 MR buffer，也不改变单边 READ/WRITE 的数据内容。
     */
    struct endpoint *e=opaque;
    if (!e || e->poisoned) return error(err,errlen,"endpoint poisoned");
    if (e->probe_recv_posted) return error(err,errlen,"probe receive already posted");
    struct ibv_recv_wr wr={
        .wr_id=AFS_RDMA_WR_PROBE_RECV,
        .next=NULL,
        .sg_list=NULL,
        .num_sge=0,
    },*bad;
    if (ibv_post_recv(e->qp,&wr,&bad)) { poison(e); return error(err,errlen,"post RDMA probe receive"); }
    e->probe_recv_posted=true;
    return 0;
}

int afs_rdma_send_probe(void *opaque,uint32_t timeout_ms,char *err,size_t errlen) {
    /*
     * client 侧发送探测并等待本地 send completion。它只能说明本端 SEND 已经完成，
     * server 是否收到由 server 的 wait_probe() 在首次数据请求前消费确认。
     */
    struct endpoint *e=opaque;
    if (!e || !e->connected || e->poisoned)
        return error(err,errlen,"endpoint disconnected/poisoned");
    if (!timeout_ms) return error(err,errlen,"invalid probe timeout");
    if (e->probe_sent) return error(err,errlen,"probe already sent");
    struct ibv_send_wr wr={
        .wr_id=AFS_RDMA_WR_PROBE_SEND,
        .next=NULL,
        .sg_list=NULL,
        .num_sge=0,
        .opcode=IBV_WR_SEND_WITH_IMM,
        .send_flags=IBV_SEND_SIGNALED,
        .imm_data=htonl(AFS_RDMA_PROBE_MAGIC),
    },*bad;
    if (ibv_post_send(e->qp,&wr,&bad)) { poison(e); return error(err,errlen,"post RDMA probe send"); }
    uint64_t deadline=monotonic_ms()+timeout_ms;
    for (;;) {
        struct ibv_wc wc={0};
        int n=ibv_poll_cq(e->cq,1,&wc);
        if (n<0) { poison(e); return error(err,errlen,"poll probe send CQ"); }
        if (n) {
            if (wc.status!=IBV_WC_SUCCESS || wc.wr_id!=AFS_RDMA_WR_PROBE_SEND ||
                wc.opcode!=IBV_WC_SEND) {
                poison(e);
                if (err && errlen) snprintf(err,errlen,"RDMA probe send completion failed: status=%s wr_id=%llu opcode=%d",
                    ibv_wc_status_str(wc.status),(unsigned long long)wc.wr_id,wc.opcode);
                return -1;
            }
            e->probe_sent=true;
            fprintf(stderr,"AFS_RDMA_PROBE send_complete\n");
            return 0;
        }
        if (monotonic_ms()>=deadline) { poison(e); return error(err,errlen,"RDMA probe send timeout; endpoint poisoned"); }
        usleep(50);
    }
}

int afs_rdma_wait_probe(void *opaque,uint32_t timeout_ms,char *err,size_t errlen) {
    /*
     * server 侧消费 client 的探测接收完成。完成项必须是我们投递的 RECV，
     * 必须带 immediate data，且 magic 要匹配；否则说明 CQ 上出现了非预期事件。
     */
    struct endpoint *e=opaque;
    if (!e || !e->connected || e->poisoned)
        return error(err,errlen,"endpoint disconnected/poisoned");
    if (!timeout_ms) return error(err,errlen,"invalid probe timeout");
    if (!e->probe_recv_posted) return error(err,errlen,"probe receive not posted");
    if (e->probe_received) return error(err,errlen,"probe already received");
    uint64_t deadline=monotonic_ms()+timeout_ms;
    for (;;) {
        struct ibv_wc wc={0};
        int n=ibv_poll_cq(e->cq,1,&wc);
        if (n<0) { poison(e); return error(err,errlen,"poll probe receive CQ"); }
        if (n) {
            uint32_t imm=ntohl(wc.imm_data);
            if (wc.status!=IBV_WC_SUCCESS || wc.wr_id!=AFS_RDMA_WR_PROBE_RECV ||
                wc.opcode!=IBV_WC_RECV || !(wc.wc_flags&IBV_WC_WITH_IMM) ||
                wc.byte_len!=0 || imm!=AFS_RDMA_PROBE_MAGIC) {
                poison(e);
                if (err && errlen) snprintf(err,errlen,"RDMA probe receive completion failed: status=%s wr_id=%llu opcode=%d flags=%u imm=0x%x",
                    ibv_wc_status_str(wc.status),(unsigned long long)wc.wr_id,wc.opcode,wc.wc_flags,imm);
                return -1;
            }
            e->probe_received=true;
            fprintf(stderr,"AFS_RDMA_PROBE receive_complete\n");
            return 0;
        }
        if (monotonic_ms()>=deadline) { poison(e); return error(err,errlen,"RDMA probe receive timeout; endpoint poisoned"); }
        usleep(50);
    }
}

int afs_rdma_put_local(void *opaque,const uint8_t *data,uint32_t len,char *err,size_t errlen) {
    struct endpoint *e=opaque;
    if (!e || len>e->capacity || (len && !data) || e->poisoned)
        return error(err,errlen,"local buffer put bounds/state");
    if (len) memcpy(e->buffer,data,len);
    return 0;
}
int afs_rdma_get_local(void *opaque,uint8_t *data,uint32_t len,char *err,size_t errlen) {
    struct endpoint *e=opaque;
    if (!e || len>e->capacity || (len && !data) || e->poisoned)
        return error(err,errlen,"local buffer get bounds/state");
    if (len) memcpy(data,e->buffer,len);
    return 0;
}
static void poison(struct endpoint *e) {
    e->poisoned=true;
    struct ibv_qp_attr attr={.qp_state=IBV_QPS_ERR};
    if (ibv_modify_qp(e->qp,&attr,IBV_QP_STATE))
        fprintf(stderr,"AFS_RDMA_ERROR QP ERR transition failed\n");
}

int afs_rdma_transfer(void *opaque,int operation,uint32_t len,uint32_t timeout_ms,char *err,size_t errlen) {
    /* Direction reminder for the Rust caller:
     * - AFS write path: server uses AFS_RDMA_READ to pull client write bytes.
     * - AFS read path:  server uses AFS_RDMA_WRITE to push file bytes to client.
     * CQ success means DMA finished, not that file storage is durable.
     */
    struct endpoint *e=opaque;
    if (!e || !e->connected || e->poisoned)
        return error(err,errlen,"endpoint disconnected/poisoned");
    if ((operation!=AFS_RDMA_READ && operation!=AFS_RDMA_WRITE) ||
        len>e->capacity || len>e->remote_capacity || !timeout_ms)
        return error(err,errlen,"invalid operation/length/timeout");
    if (!len) return 0;
    struct ibv_sge sge={.addr=(uintptr_t)e->buffer,.length=len,.lkey=e->mr->lkey};
    struct ibv_send_wr wr={
        .wr_id=AFS_RDMA_WR_DATA,.sg_list=&sge,.num_sge=1,
        .opcode=operation==AFS_RDMA_READ ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE,
        .send_flags=IBV_SEND_SIGNALED,
        .wr.rdma={.remote_addr=e->remote_address,.rkey=e->remote_key},
    },*bad;
    if (ibv_post_send(e->qp,&wr,&bad)) { poison(e); return error(err,errlen,"post one-sided operation"); }
    uint64_t deadline=monotonic_ms()+timeout_ms;
    for (;;) {
        struct ibv_wc wc={0};
        int n=ibv_poll_cq(e->cq,1,&wc);
        if (n<0) { poison(e); return error(err,errlen,"poll CQ"); }
        if (n) {
            if (wc.status!=IBV_WC_SUCCESS || wc.wr_id!=AFS_RDMA_WR_DATA) {
                poison(e);
                if (err && errlen) snprintf(err,errlen,"RDMA completion failed: %s",ibv_wc_status_str(wc.status));
                return -1;
            }
            fprintf(stderr,"AFS_RDMA_COMPLETE op=%s bytes=%u\n",operation==AFS_RDMA_READ?"READ":"WRITE",len);
            return 0;
        }
        if (monotonic_ms()>=deadline) { poison(e); return error(err,errlen,"RDMA completion timeout; endpoint poisoned"); }
        usleep(50);
    }
}
