#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Finite syscall workload; no SSH/control latency inside the timed phases.
 * A fresh, experiment-owned directory is required. Errors invalidate a sample.
 * File close is a visibility endpoint, not a claimed persistence barrier. */
enum { FILES = 1000, BLOCK = 4096, PHASES = 6 };
static const char *names[] = {"create_write_close", "stat", "read_close", "readdir", "rename", "unlink"};
static char root[4096];
static int threads;
static uint64_t latency[PHASES][FILES];
static uint64_t wall[PHASES];
static uint64_t cpu[PHASES];
static uint64_t now(clockid_t clock) {
    struct timespec t;
    if (clock_gettime(clock, &t)) { perror("clock_gettime"); exit(2); }
    return (uint64_t)t.tv_sec * 1000000000ULL + (uint64_t)t.tv_nsec;
}
static void fail(const char *operation) { perror(operation); exit(2); }
static void path(char *out, size_t size, int id, int renamed) {
    int n = snprintf(out, size, "%s/%c%05d", root, renamed ? 'r' : 'f', id);
    if (n < 0 || (size_t)n >= size) { errno = ENAMETOOLONG; fail("path"); }
}
struct work { int phase; int worker; };
static void *worker(void *opaque) {
    struct work *w = opaque;
    unsigned char data[BLOCK], read_data[BLOCK];
    memset(data, 0x5a, sizeof(data));
    for (int id = w->worker; id < FILES; id += threads) {
        char name[8192], replacement[8192];
        path(name, sizeof(name), id, 0);
        path(replacement, sizeof(replacement), id, 1);
        uint64_t start = now(CLOCK_MONOTONIC);
        int fd;
        struct stat st;
        switch (w->phase) {
        case 0:
            fd = open(name, O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd < 0) fail("create");
            if (write(fd, data, sizeof(data)) != BLOCK) fail("write");
            if (close(fd)) fail("close");
            break;
        case 1:
            if (stat(name, &st)) fail("stat");
            if (st.st_size != BLOCK) { errno = EIO; fail("size"); }
            break;
        case 2:
            fd = open(name, O_RDONLY);
            if (fd < 0) fail("open read");
            if (read(fd, read_data, sizeof(read_data)) != BLOCK) fail("read");
            if (memcmp(read_data, data, BLOCK)) { errno = EIO; fail("content"); }
            if (close(fd)) fail("close read");
            break;
        case 4:
            if (rename(name, replacement)) fail("rename");
            break;
        case 5:
            if (unlink(replacement)) fail("unlink");
            break;
        default: errno = EINVAL; fail("phase");
        }
        latency[w->phase][id] = now(CLOCK_MONOTONIC) - start;
    }
    return NULL;
}
static int compare(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}
int main(int argc, char **argv) {
    if (argc != 4 || (strcmp(argv[2], "absolute") && strcmp(argv[2], "relative"))) {
        fprintf(stderr, "usage: benchmark FRESH_DIRECTORY absolute|relative 1|8\n"); return 2;
    }
    threads = atoi(argv[3]);
    if (threads != 1 && threads != 8) return 2;
    if (argv[1][0] != '/') return 2;
    if (mkdir(argv[1], 0700)) fail("fresh directory");
    if (!strcmp(argv[2], "relative")) {
        if (chdir(argv[1])) fail("chdir");
        strcpy(root, ".");
    } else {
        if (strlen(argv[1]) >= sizeof(root)) return 2;
        strcpy(root, argv[1]);
    }
    for (int phase = 0; phase < PHASES; ++phase) {
        uint64_t begin = now(CLOCK_MONOTONIC), begin_cpu = now(CLOCK_PROCESS_CPUTIME_ID);
        if (phase == 3) {
            DIR *directory = opendir(root);
            if (!directory) fail("opendir");
            size_t count = 0;
            errno = 0;
            struct dirent *entry;
            while ((entry = readdir(directory))) if (entry->d_name[0] == 'f') ++count;
            if (errno) fail("readdir");
            if (count != FILES) { errno = EIO; fail("readdir count"); }
            if (closedir(directory)) fail("closedir");
        } else {
            pthread_t pool[8]; struct work work[8];
            for (int i = 0; i < threads; ++i) {
                work[i] = (struct work){phase, i};
                if (pthread_create(&pool[i], NULL, worker, &work[i])) { errno = EIO; fail("pthread_create"); }
            }
            for (int i = 0; i < threads; ++i)
                if (pthread_join(pool[i], NULL)) { errno = EIO; fail("pthread_join"); }
        }
        cpu[phase] = now(CLOCK_PROCESS_CPUTIME_ID) - begin_cpu;
        wall[phase] = now(CLOCK_MONOTONIC) - begin;
    }
    /* Leave the empty, uniquely named directory as an inspectable artifact. */
    printf("{\"files\":%d,\"file_bytes\":%d,\"concurrency\":%d,\"path_form\":\"%s\",\"barrier\":\"close visibility; durability unqualified\",\"phases\":[", FILES, BLOCK, threads, argv[2]);
    for (int p = 0; p < PHASES; ++p) {
        qsort(latency[p], FILES, sizeof(uint64_t), compare);
        printf("%s{\"name\":\"%s\",\"wall_ns\":%llu,\"client_cpu_ns\":%llu", p ? "," : "", names[p], (unsigned long long)wall[p], (unsigned long long)cpu[p]);
        if (p != 3) printf(",\"operations\":%d,\"p50_ns\":%llu,\"p95_ns\":%llu,\"p99_ns\":%llu", FILES, (unsigned long long)latency[p][FILES/2], (unsigned long long)latency[p][FILES*95/100], (unsigned long long)latency[p][FILES*99/100]);
        else printf(",\"operations\":1");
        printf("}");
    }
    printf("]}\n");
    return 0;
}
