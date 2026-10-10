#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Experimental buffered IO, fixed total bytes regardless of thread count.
 * Per-operation intervals end after the syscall/count check, before content validation.
 * Content validation remains required and is included in the whole-task timer.
 * One final file barrier is inside the task timer, after all threads join. */
static int fd, threads, writing, random_io, byte_value;
static uint64_t file_bytes, io_bytes, block_bytes, operations, *latencies;
static uint64_t now(clockid_t clock) {
    struct timespec value;
    if (clock_gettime(clock, &value)) { perror("clock_gettime"); exit(2); }
    return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec;
}
static void fail(const char *label) { perror(label); exit(2); }
static void require(int ok, const char *label) { if (!ok) { errno = EIO; fail(label); } }
static uint64_t mix(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
static uint64_t offset(uint64_t index) {
    return (random_io ? mix(index ^ 257ULL) % (file_bytes / block_bytes) : index) * block_bytes;
}
static void *worker(void *argument) {
    unsigned char *buffer = malloc(block_bytes), *expected = malloc(block_bytes);
    require(buffer && expected, "buffers");
    memset(buffer, byte_value, block_bytes); memset(expected, byte_value, block_bytes);
    for (uint64_t index = (uintptr_t)argument; index < operations; index += (uint64_t)threads) {
        uint64_t begin = now(CLOCK_MONOTONIC);
        ssize_t size = writing ? pwrite(fd, buffer, block_bytes, (off_t)offset(index))
                               : pread(fd, buffer, block_bytes, (off_t)offset(index));
        require(size == (ssize_t)block_bytes, "IO count");
        latencies[index] = now(CLOCK_MONOTONIC) - begin;
        if (!writing) require(!memcmp(buffer, expected, block_bytes), "read content");
    }
    free(buffer); free(expected); return NULL;
}
static int compare(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}
static uint64_t resident(int descriptor) {
    size_t pages = (size_t)((file_bytes + 4095) / 4096);
    unsigned char *vector = malloc(pages);
    require(vector != NULL, "residency vector");
    void *mapping = mmap(NULL, file_bytes, PROT_READ, MAP_SHARED, descriptor, 0);
    if (mapping == MAP_FAILED) fail("residency mapping");
    if (mincore(mapping, file_bytes, vector)) fail("mincore");
    uint64_t count = 0;
    for (size_t page = 0; page < pages; ++page) count += !!(vector[page] & 1);
    require(munmap(mapping, file_bytes) == 0, "munmap"); free(vector);
    return count * 4096ULL;
}
int main(int argc, char **argv) {
    if (argc < 10 || argc > 12) {
        fprintf(stderr, "usage: io FILE seq-write|seq-read|random-write|random-read BYTES BLOCK THREADS close|fdatasync|fsync IO_BYTES BYTE existing|create [guest-cold|hot|repeat|unchecked [samples]]\n");
        return 2;
    }
    const char *operation = argv[2], *barrier = argv[6];
    const char *cache = argc >= 11 ? argv[10] : "unchecked";
    int samples = argc == 12;
    require(!samples || !strcmp(argv[11], "samples"), "sample output mode");
    require(!strcmp(cache, "guest-cold") || !strcmp(cache, "hot") || !strcmp(cache, "repeat") || !strcmp(cache, "unchecked") || !strcmp(cache, "unobserved"), "cache mode");
    writing = !strcmp(operation, "seq-write") || !strcmp(operation, "random-write");
    random_io = !strcmp(operation, "random-write") || !strcmp(operation, "random-read");
    require(writing || !strcmp(operation, "seq-read") || !strcmp(operation, "random-read"), "operation");
    file_bytes = strtoull(argv[3], NULL, 10); block_bytes = strtoull(argv[4], NULL, 10);
    threads = atoi(argv[5]); io_bytes = strtoull(argv[7], NULL, 10); byte_value = atoi(argv[8]);
    require(argv[1][0] == '/' && (threads == 1 || threads == 8) && byte_value >= 0 && byte_value <= 255,
            "arguments");
    require(block_bytes && file_bytes && io_bytes && file_bytes % block_bytes == 0 && io_bytes % block_bytes == 0,
            "size alignment");
    require(random_io || io_bytes == file_bytes, "sequential bytes");
    require(!strcmp(barrier, "close") || (writing && (!strcmp(barrier, "fdatasync") || !strcmp(barrier, "fsync"))), "barrier");
    int flags = writing ? O_RDWR : O_RDONLY;
    if (!strcmp(argv[9], "create")) { require(writing, "create read"); flags |= O_CREAT | O_EXCL; }
    else require(!strcmp(argv[9], "existing"), "existing mode");
    operations = io_bytes / block_bytes;
    latencies = malloc(operations * sizeof(*latencies)); require(latencies != NULL, "latencies");
    /* Residency inspection and allocation are excluded from measurement. */
    uint64_t before_resident = 0;
    unsigned prepare_attempts = 0;
    if (!(flags & O_CREAT)) {
        fd = open(argv[1], flags); if (fd < 0) fail("inspect open");
        struct stat stat; require(fstat(fd, &stat) == 0 && (uint64_t)stat.st_size == file_bytes, "source size");
        before_resident = !strcmp(cache, "unobserved") ? 0 : resident(fd);
        if (!strcmp(cache, "guest-cold")) {
            for (prepare_attempts = 1; prepare_attempts <= 3; ++prepare_attempts) {
                require(fsync(fd) == 0, "preflight fsync");
                int error = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
                require(error == 0, "preflight discard");
                before_resident = resident(fd);
                if (before_resident == 0) break;
            }
            if (before_resident != 0) {
                fprintf(stderr, "cold precondition failed before timer: %llu resident bytes\n", (unsigned long long)before_resident);
                close(fd); free(latencies); return 3;
            }
        }
        if (!strcmp(cache, "hot") && before_resident != file_bytes) {
            fprintf(stderr, "hot precondition failed before timer: %llu resident bytes\n", (unsigned long long)before_resident);
            close(fd); free(latencies); return 3;
        }
        require(close(fd) == 0, "inspect close");
    }
    uint64_t begin = now(CLOCK_MONOTONIC), begin_cpu = now(CLOCK_PROCESS_CPUTIME_ID);
    fd = open(argv[1], flags, 0600); if (fd < 0) fail("open");
    pthread_t pool[8];
    for (int index = 0; index < threads; ++index)
        require(pthread_create(&pool[index], NULL, worker, (void *)(uintptr_t)index) == 0, "thread create");
    for (int index = 0; index < threads; ++index) require(pthread_join(pool[index], NULL) == 0, "thread join");
    uint64_t barrier_begin = now(CLOCK_MONOTONIC);
    if (!strcmp(barrier, "fdatasync")) require(fdatasync(fd) == 0, "fdatasync");
    if (!strcmp(barrier, "fsync")) require(fsync(fd) == 0, "fsync");
    uint64_t barrier_ns = now(CLOCK_MONOTONIC) - barrier_begin;
    require(close(fd) == 0, "close");
    uint64_t cpu = now(CLOCK_PROCESS_CPUTIME_ID) - begin_cpu, elapsed = now(CLOCK_MONOTONIC) - begin;
    fd = open(argv[1], O_RDONLY); if (fd < 0) fail("verify open");
    struct stat stat; require(fstat(fd, &stat) == 0 && (uint64_t)stat.st_size == file_bytes, "final size");
    if (writing) {
        unsigned char *buffer = malloc(block_bytes), *expected = malloc(block_bytes);
        require(buffer && expected, "verify buffers"); memset(expected, byte_value, block_bytes);
        for (uint64_t index = 0; index < operations; ++index) {
            require(pread(fd, buffer, block_bytes, (off_t)offset(index)) == (ssize_t)block_bytes, "verify read");
            require(!memcmp(buffer, expected, block_bytes), "write content");
        }
        free(buffer); free(expected);
    }
    uint64_t after_resident = !strcmp(cache, "unobserved") ? 0 : resident(fd); require(close(fd) == 0, "verify close");
    qsort(latencies, operations, sizeof(*latencies), compare);
    printf("{\"operation\":\"%s\",\"file_bytes\":%llu,\"io_bytes\":%llu,\"block_bytes\":%llu,\"concurrency\":%d,\"barrier\":\"%s\",\"pattern_byte\":%d,\"seed\":257,\"operations\":%llu,\"wall_ns\":%llu,\"client_cpu_ns\":%llu,\"barrier_ns\":%llu,\"p50_ns\":%llu,\"p95_ns\":%llu,\"p99_ns\":%llu,\"residency_observed\":%s,\"resident_before_bytes\":%llu,\"resident_after_bytes\":%llu,\"content_ok\":true,\"cache_requested\":\"%s\",\"cache_prepare_attempts\":%u,\"file_object\":{\"device\":%llu,\"inode\":%llu}",
           operation, (unsigned long long)file_bytes, (unsigned long long)io_bytes, (unsigned long long)block_bytes,
           threads, barrier, byte_value, (unsigned long long)operations, (unsigned long long)elapsed,
           (unsigned long long)cpu, (unsigned long long)barrier_ns,
           (unsigned long long)latencies[operations/2], (unsigned long long)latencies[operations*95/100],
           (unsigned long long)latencies[operations*99/100], !strcmp(cache, "unobserved") ? "false" : "true", (unsigned long long)before_resident,
           (unsigned long long)after_resident, cache, prepare_attempts,
           (unsigned long long)stat.st_dev, (unsigned long long)stat.st_ino);
    if (samples) {
        /* Keep every observed interval for independent pooled percentiles.
         * Sorting and serialization are outside the measured IO task. */
        printf(",\"latency_interval\":\"%s\",\"latency_order\":\"sorted\",\"latency_samples_ns\":[",
               writing ? "pwrite+count-check" : "pread+count-check");
        for (uint64_t index = 0; index < operations; ++index)
            printf("%s%llu", index ? "," : "", (unsigned long long)latencies[index]);
        printf("]");
    }
    printf("}\n");
    /* Unobserved residency is not a zero/cold-cache claim. */
    free(latencies); return 0;
}
