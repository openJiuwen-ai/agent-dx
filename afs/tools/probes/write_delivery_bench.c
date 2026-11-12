#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define MIB (1024UL * 1024)
#define SMALL (64UL * 1024)
#define OPS 32

static uint64_t clock_ns(clockid_t id) {
    struct timespec t;
    if (clock_gettime(id, &t)) { perror("clock_gettime"); exit(2); }
    return (uint64_t)t.tv_sec * 1000000000ULL + (uint64_t)t.tv_nsec;
}
static uint64_t mono(void) { return clock_ns(CLOCK_MONOTONIC); }
static uint64_t epoch(void) { return clock_ns(CLOCK_REALTIME); }
static double process_cpu(void) {
    struct rusage r;
    if (getrusage(RUSAGE_SELF, &r)) { perror("getrusage"); exit(2); }
    return r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6
        + r.ru_stime.tv_sec + r.ru_stime.tv_usec / 1e6;
}
static uint64_t word(uint64_t i) {
    i += 0x9e3779b97f4a7c15ULL;
    i = (i ^ (i >> 30)) * 0xbf58476d1ce4e5b9ULL;
    i = (i ^ (i >> 27)) * 0x94d049bb133111ebULL;
    return i ^ (i >> 31);
}
struct operation {
    uint64_t begin_ns, write_end_ns, fsync_begin_ns, end_ns;
    uint64_t begin_epoch_ns, write_end_epoch_ns, fsync_begin_epoch_ns, end_epoch_ns;
};
struct job {
    const char *path;
    uint8_t *data;
    uint64_t size;
    int small, progress, error;
    long tid;
    pthread_barrier_t *ready, *go;
    uint64_t start_ns, start_epoch_ns, open_end_ns, write_end_ns;
    uint64_t fsync_end_ns, close_end_ns, end_epoch_ns, bytes, calls;
    struct operation ops[OPS];
    unsigned completed_ops;
};
static void barrier(pthread_barrier_t *b) {
    int rc = pthread_barrier_wait(b);
    if (rc && rc != PTHREAD_BARRIER_SERIAL_THREAD) { errno = rc; perror("barrier"); exit(2); }
}
static void fail(struct job *j, int err) { if (!j->error) j->error = err ? err : EIO; }
static int progress(struct job *j) {
    if (pwrite(j->progress, &j->bytes, sizeof j->bytes, 0) != (ssize_t)sizeof j->bytes) {
        fail(j, errno); return -1;
    }
    return 0;
}
static int write_all(struct job *j, int fd, const uint8_t *data, size_t len, uint64_t offset) {
    size_t pos = 0;
    while (pos < len) {
        ssize_t n = pwrite(fd, data + pos, len - pos, (off_t)(offset + pos));
        j->calls++;
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { fail(j, n < 0 ? errno : EIO); return -1; }
        pos += (size_t)n; j->bytes += (uint64_t)n;
    }
    return 0;
}
static void *worker(void *arg) {
    struct job *j = arg;
    j->tid = syscall(SYS_gettid);
    barrier(j->ready);
    barrier(j->go);
    j->start_ns = mono(); j->start_epoch_ns = epoch();
    int fd = open(j->path, O_RDWR | (j->small ? 0 : O_CREAT | O_TRUNC), 0600);
    j->open_end_ns = mono();
    if (fd < 0) {
        fail(j, errno); j->close_end_ns = mono(); j->end_epoch_ns = epoch(); return NULL;
    }
    if (j->small) {
        for (unsigned i = 0; i < OPS; i++) {
            struct operation *o = &j->ops[i];
            o->begin_ns = mono(); o->begin_epoch_ns = epoch();
            int rc = write_all(j, fd, j->data, SMALL, 0);
            o->write_end_ns = mono(); o->write_end_epoch_ns = epoch();
            if (!rc) rc = progress(j);
            o->fsync_begin_ns = mono(); o->fsync_begin_epoch_ns = epoch();
            if (fsync(fd)) { fail(j, errno); rc = -1; }
            o->end_ns = mono(); o->end_epoch_ns = epoch();
            j->write_end_ns = o->write_end_ns; j->fsync_end_ns = o->end_ns;
            j->completed_ops++;
            if (rc) break;
        }
    } else {
        for (uint64_t pos = 0; pos < j->size; pos += MIB) {
            if (write_all(j, fd, j->data + pos, MIB, pos)) break;
            if ((j->bytes % (16 * MIB) == 0 || j->bytes == j->size) && progress(j)) break;
        }
        j->write_end_ns = mono();
        if (fsync(fd)) fail(j, errno);
        j->fsync_end_ns = mono();
    }
    if (close(fd)) fail(j, errno);
    j->close_end_ns = mono(); j->end_epoch_ns = epoch();
    return NULL;
}
static int progress_open(const char *root, const char *name) {
    char path[4096];
    if (snprintf(path, sizeof path, "%s/%s", root, name) >= (int)sizeof path) return -1;
    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
    uint64_t zero = 0;
    if (fd < 0) return -1;
    if (pwrite(fd, &zero, sizeof zero, 0) != (ssize_t)sizeof zero) { close(fd); return -1; }
    return fd;
}
static void print_job(const char *name, const struct job *j) {
    printf("\"%s\":{\"tid\":%ld,\"start_ns\":%" PRIu64 ",\"start_epoch_ns\":%" PRIu64
           ",\"open_end_ns\":%" PRIu64 ",\"write_end_ns\":%" PRIu64
           ",\"fsync_end_ns\":%" PRIu64 ",\"close_end_ns\":%" PRIu64
           ",\"end_epoch_ns\":%" PRIu64 ",\"bytes\":%" PRIu64
           ",\"write_calls\":%" PRIu64 ",\"error\":%d,\"completed_ops\":%u,\"ops\":[",
           name,j->tid,j->start_ns,j->start_epoch_ns,j->open_end_ns,j->write_end_ns,
           j->fsync_end_ns,j->close_end_ns,j->end_epoch_ns,j->bytes,j->calls,j->error,j->completed_ops);
    for (unsigned i = 0; i < j->completed_ops; i++) {
        const struct operation *o = &j->ops[i];
        printf("%s{\"index\":%u,\"begin_ns\":%" PRIu64 ",\"write_end_ns\":%" PRIu64
               ",\"fsync_begin_ns\":%" PRIu64 ",\"end_ns\":%" PRIu64
               ",\"begin_epoch_ns\":%" PRIu64 ",\"write_end_epoch_ns\":%" PRIu64
               ",\"fsync_begin_epoch_ns\":%" PRIu64 ",\"end_epoch_ns\":%" PRIu64 "}",
               i ? "," : "",i,o->begin_ns,o->write_end_ns,o->fsync_begin_ns,o->end_ns,
               o->begin_epoch_ns,o->write_end_epoch_ns,o->fsync_begin_epoch_ns,o->end_epoch_ns);
    }
    printf("]}");
}
int main(int argc, char **argv) {
    if (argc != 4 && argc != 5) {
        fprintf(stderr,"two-thread BIG SMALL PROGRESS_DIR [BIG_BYTES]\n"); return 2;
    }
    uint64_t size = argc == 5 ? strtoull(argv[4], NULL, 10) : 1024 * MIB;
    if (!size || size % MIB) return 2;
    uint8_t *big = NULL, *small = NULL;
    if (posix_memalign((void **)&big,4096,(size_t)size) || posix_memalign((void **)&small,4096,SMALL)) return 2;
    for (uint64_t i = 0; i < size / 8; i++) ((uint64_t *)big)[i] = word(i);
    memset(small,'l',SMALL);
    int pb = progress_open(argv[3],"big-progress.bin"), ps = progress_open(argv[3],"small-progress.bin");
    if (pb < 0 || ps < 0) { perror("progress_open"); return 2; }
    pthread_barrier_t ready, go;
    if (pthread_barrier_init(&ready,NULL,3) || pthread_barrier_init(&go,NULL,3)) return 2;
    struct job a = {.path=argv[1],.data=big,.size=size,.progress=pb,.ready=&ready,.go=&go};
    struct job b = {.path=argv[2],.data=small,.size=SMALL,.small=1,.progress=ps,.ready=&ready,.go=&go};
    pthread_t ta,tb;
    if (pthread_create(&ta,NULL,worker,&a) || pthread_create(&tb,NULL,worker,&b)) return 2;
    barrier(&ready);
    double cpu_begin = process_cpu();
    uint64_t t0_epoch = epoch(), t0 = mono();
    barrier(&go);
    int ja = pthread_join(ta,NULL), jb = pthread_join(tb,NULL);
    uint64_t joined = mono(), joined_epoch = epoch();
    double cpu_used = process_cpu() - cpu_begin;
    int error = a.error ? a.error : b.error;
    if (!error && (ja || jb)) error = ja ? ja : jb;
    uint64_t last = a.close_end_ns > b.close_end_ns ? a.close_end_ns : b.close_end_ns;
    printf("{\"worker_threads\":2,\"single_process\":true,\"pid\":%jd,\"t0_ns\":%" PRIu64
           ",\"t0_epoch_ns\":%" PRIu64 ",\"joined_ns\":%" PRIu64 ",\"joined_epoch_ns\":%" PRIu64
           ",\"complete_ms\":%.6f,\"both_files_done_ms\":%.6f,\"process_cpu_s\":%.9f,\"error\":%d,",
           (intmax_t)getpid(),t0,t0_epoch,joined,joined_epoch,(joined-t0)/1e6,(last-t0)/1e6,cpu_used,error);
    print_job("large",&a); printf(","); print_job("small",&b); printf("}\n");
    pthread_barrier_destroy(&ready); pthread_barrier_destroy(&go);
    close(pb);close(ps);free(big);free(small);
    return error ? 1 : 0;
}
