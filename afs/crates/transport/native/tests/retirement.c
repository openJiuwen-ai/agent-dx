/* Linux: cc -Wall -Wextra -Werror retirement.c -libverbs -o retirement */
#include <assert.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>

static int stop_error, poll_error, destroy_error;
static unsigned stop_calls, poll_calls, destroy_calls, dereg_calls;
static unsigned initial_empty_polls, completion_count, next_completion;
static uint64_t completions[2];

static int mock_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr, int mask) {
    assert(qp && attr->qp_state == IBV_QPS_ERR && mask == IBV_QP_STATE);
    ++stop_calls;
    return stop_error;
}

static int mock_poll_cq(struct ibv_cq *cq, int capacity, struct ibv_wc *wc) {
    assert(cq && capacity == 1 && !destroy_calls && !dereg_calls);
    if (++poll_calls <= initial_empty_polls) return 0;
    if (poll_error) return -1;
    if (next_completion == completion_count) return 0;
    wc->wr_id = completions[next_completion++];
    wc->qp_num = 42;
    wc->status = IBV_WC_WR_FLUSH_ERR;
    return 1;
}

static int mock_destroy_qp(struct ibv_qp *qp) {
    assert(stop_calls == 1 && !dereg_calls);
    ++destroy_calls;
    if (destroy_error) return destroy_error;
    free(qp);
    return 0;
}

static int mock_dereg_mr(struct ibv_mr *mr) {
    assert(destroy_calls == 1 && !destroy_error);
    ++dereg_calls;
    free(mr);
    return 0;
}

static int mock_destroy_cq(struct ibv_cq *cq) {
    assert(dereg_calls == 1);
    free(cq);
    return 0;
}

#define ibv_modify_qp mock_modify_qp
#define ibv_poll_cq mock_poll_cq
#define ibv_destroy_qp mock_destroy_qp
#define ibv_dereg_mr mock_dereg_mr
#define ibv_destroy_cq mock_destroy_cq
#include "../rdma.c"

static struct endpoint *fixture(unsigned pending_wr) {
    stop_error = poll_error = destroy_error = 0;
    stop_calls = poll_calls = destroy_calls = dereg_calls = 0;
    initial_empty_polls = completion_count = next_completion = 0;
    struct endpoint *e = calloc(1, sizeof(*e));
    assert(e);
    e->qp = calloc(1, sizeof(*e->qp));
    e->cq = calloc(1, sizeof(*e->cq));
    e->mr = calloc(1, sizeof(*e->mr));
    e->capacity = 4096;
    e->buffer = calloc(1, e->capacity);
    assert(e->qp && e->cq && e->mr && e->buffer);
    e->qp->qp_num = 42;
    e->pending_wr = pending_wr;
    e->accepting_remote = true;
    e->buffer[0] = 0xab;
    return e;
}

static void retained(struct endpoint *e, int expected, const char *label) {
    size_t bytes = 0;
    assert(afs_rdma_close(e, &bytes) == expected);
    assert(bytes == e->capacity && !dereg_calls && e->buffer[0] == 0xab);
    assert(e->qp && e->cq && e->mr);
    printf("PASS %s retained=%zu destroy_calls=%u\n", label, bytes, destroy_calls);
    /* Mock resources have no provider or in-flight work. */
    free(e->qp);
    free(e->cq);
    free(e->mr);
    free(e->buffer);
    free(e);
}

int main(void) {
    const unsigned pending = AFS_RDMA_PENDING_DATA | AFS_RDMA_PENDING_PROBE_RECV;
    struct endpoint *e = fixture(pending);
    size_t bytes = 4096;
    initial_empty_polls = 2;
    completions[0] = AFS_RDMA_WR_PROBE_RECV;
    completions[1] = AFS_RDMA_WR_DATA;
    completion_count = 2;
    assert(afs_rdma_close(e, &bytes) == AFS_RDMA_RELEASED);
    assert(bytes == 0 && poll_calls == 5 && dereg_calls == 1);
    puts("PASS delayed flushes collected before QP destroy and MR deregistration");

    e = fixture(AFS_RDMA_PENDING_DATA);
    assert(afs_rdma_close(e, &bytes) == AFS_RDMA_RETAINED_DRAIN);
    assert(!destroy_calls && poll_calls > 1);
    assert(bytes == e->capacity && !dereg_calls && e->buffer[0] == 0xab);
    assert(e->qp && e->cq && e->mr);
    puts("PASS empty CQ with outstanding data until deadline retains endpoint and buffer");
    completions[0] = AFS_RDMA_WR_DATA;
    completion_count = 1;
    assert(afs_rdma_close(e, &bytes) == AFS_RDMA_RELEASED);
    assert(bytes == 0 && stop_calls == 1 && destroy_calls == 1 && dereg_calls == 1);
    puts("PASS second close after late flush releases QP and registered buffer");

    e = fixture(AFS_RDMA_PENDING_DATA);
    poll_error = 1;
    retained(e, AFS_RDMA_RETAINED_CQ, "untrusted CQ");
    assert(!destroy_calls && poll_calls == 1);

    e = fixture(AFS_RDMA_PENDING_DATA);
    completion_count = 1;
    completions[0] = AFS_RDMA_WR_PROBE_SEND;
    retained(e, AFS_RDMA_RETAINED_CQ, "unmatched completion");
    assert(!destroy_calls && poll_calls == 1);

    e = fixture(0);
    stop_error = 1;
    retained(e, AFS_RDMA_RETAINED_DRAIN, "QP ERR transition failed on the passive end");
    assert(!destroy_calls && !poll_calls);

    e = fixture(0);
    destroy_error = 1;
    retained(e, AFS_RDMA_RETAINED_QP, "drained passive end with failed QP destroy");
    assert(destroy_calls == 1 && poll_calls == 1);
    return 0;
}
