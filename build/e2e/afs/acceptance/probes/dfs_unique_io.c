#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/*
 * Fixed DFS one-writer/many-reader qualification payload.
 *
 * The file is always 64 MiB, split into 64 x 1 MiB blocks.  Every block is
 * filled with byte 0x61, then its first eight bytes are replaced by the
 * little-endian block counter.  This keeps the workload close to the existing
 * constant-byte probes while giving each 4 MiB DFS chunk distinct content.
 */

enum {
    PATTERN_BYTE = 0x61,
    BLOCK_BYTES = 1048576,
    OPERATIONS = 64,
};

static const uint64_t FILE_BYTES = (uint64_t)BLOCK_BYTES * (uint64_t)OPERATIONS;
static const char *DATASET = "counter-1m-v1";
static uint64_t generation = 0; /* Legacy CLI uses the original dataset. */

/* Guest-local intervals of complete logical reads, including short-read retries.
 * These are not individual syscall timings or a cross-VM clock mapping. */
static struct {
    uint64_t task_begin, task_end;
    uint64_t open[2], fstat[2], reads[OPERATIONS][2], eof[2], close[2];
} read_timing;

static void emit_span(const uint64_t span[2]) {
    printf("[%llu,%llu]", (unsigned long long)span[0],
           (unsigned long long)span[1]);
}

static void emit_read_timing(void) {
    printf(",\"read_timing\":{\"schema\":\"complete-read-v1\","
           "\"clock\":\"CLOCK_MONOTONIC\","
           "\"boundary\":\"full_read_1MiB_excluding_content_oracle\","
           "\"task_begin_ns\":%llu,\"task_end_ns\":%llu,\"open\":",
           (unsigned long long)read_timing.task_begin,
           (unsigned long long)read_timing.task_end);
    emit_span(read_timing.open);
    printf(",\"fstat\":");
    emit_span(read_timing.fstat);
    printf(",\"reads\":[");
    for (unsigned index = 0; index < OPERATIONS; ++index) {
        if (index != 0) printf(",");
        emit_span(read_timing.reads[index]);
    }
    printf("],\"eof\":");
    emit_span(read_timing.eof);
    printf(",\"close\":");
    emit_span(read_timing.close);
    printf("}");
}

static uint64_t now_ns(clockid_t clock) {
    struct timespec value;
    if (clock_gettime(clock, &value) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec;
}

static void fail(const char *label) {
    perror(label);
    exit(2);
}

static void fail_text(const char *message) {
    fprintf(stderr, "%s\n", message);
    exit(2);
}

static void write_counter_le(unsigned char *buffer, uint64_t value) {
    for (unsigned shift = 0; shift < 8; ++shift) {
        buffer[shift] = (unsigned char)((value >> (shift * 8)) & 0xffU);
    }
}

static void fill_block(unsigned char *buffer, uint64_t index) {
    memset(buffer, PATTERN_BYTE, BLOCK_BYTES);
    write_counter_le(buffer, (generation << 32) | index);
}

static void full_write(int fd, const unsigned char *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t written = write(fd, buffer + offset, length - offset);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            fail("write");
        }
        if (written == 0) {
            fail_text("short write");
        }
        offset += (size_t)written;
    }
}

static void full_read(int fd, unsigned char *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t got = read(fd, buffer + offset, length - offset);
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            fail("read");
        }
        if (got == 0) {
            fail_text("short read");
        }
        offset += (size_t)got;
    }
}

static void emit_success(const char *operation, const char *barrier, uint64_t wall_ns,
                         uint64_t cpu_ns, uint64_t barrier_ns) {
    char generation_json[40] = "";
    if (generation != 0) {
        snprintf(generation_json, sizeof(generation_json), "\"generation\":%llu,",
                 (unsigned long long)generation);
    }
    printf("{\"dataset\":\"%s\",%s\"operation\":\"%s\",\"file_bytes\":%llu,"
           "\"io_bytes\":%llu,\"block_bytes\":%d,\"concurrency\":1,"
           "\"pattern_byte\":%d,\"operations\":%d,\"barrier\":\"%s\","
           "\"cache_requested\":\"unobserved\",\"residency_observed\":false,"
           "\"content_ok\":true,\"wall_ns\":%llu,\"client_cpu_ns\":%llu,"
           "\"barrier_ns\":%llu",
           DATASET, generation_json, operation, (unsigned long long)FILE_BYTES,
           (unsigned long long)FILE_BYTES, BLOCK_BYTES, PATTERN_BYTE, OPERATIONS,
           barrier, (unsigned long long)wall_ns, (unsigned long long)cpu_ns,
           (unsigned long long)barrier_ns);
    if (strcmp(operation, "seq-read") == 0) emit_read_timing();
    printf("}\n");
}

static int write_file(const char *path) {
    unsigned char *buffer = malloc(BLOCK_BYTES);
    if (buffer == NULL) {
        fail("malloc");
    }

    uint64_t begin = now_ns(CLOCK_MONOTONIC);
    uint64_t cpu_begin = now_ns(CLOCK_PROCESS_CPUTIME_ID);
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) {
        fail("open");
    }
    for (uint64_t index = 0; index < OPERATIONS; ++index) {
        fill_block(buffer, index);
        full_write(fd, buffer, BLOCK_BYTES);
    }
    uint64_t barrier_begin = now_ns(CLOCK_MONOTONIC);
    if (fdatasync(fd) != 0) {
        fail("fdatasync");
    }
    uint64_t barrier_ns = now_ns(CLOCK_MONOTONIC) - barrier_begin;
    if (close(fd) != 0) {
        fail("close");
    }
    uint64_t cpu_ns = now_ns(CLOCK_PROCESS_CPUTIME_ID) - cpu_begin;
    uint64_t wall_ns = now_ns(CLOCK_MONOTONIC) - begin;
    free(buffer);
    emit_success("seq-write", "fdatasync", wall_ns, cpu_ns, barrier_ns);
    return 0;
}

static int read_file(const char *path) {
    unsigned char *buffer = malloc(BLOCK_BYTES);
    unsigned char *expected = malloc(BLOCK_BYTES);
    if (buffer == NULL || expected == NULL) {
        fail("malloc");
    }

    uint64_t begin = now_ns(CLOCK_MONOTONIC);
    uint64_t cpu_begin = now_ns(CLOCK_PROCESS_CPUTIME_ID);
    read_timing.open[0] = now_ns(CLOCK_MONOTONIC);
    int fd = open(path, O_RDONLY);
    read_timing.open[1] = now_ns(CLOCK_MONOTONIC);
    if (fd < 0) {
        fail("open");
    }

    struct stat st;
    read_timing.fstat[0] = now_ns(CLOCK_MONOTONIC);
    if (fstat(fd, &st) != 0) {
        fail("fstat");
    }
    read_timing.fstat[1] = now_ns(CLOCK_MONOTONIC);
    if ((uint64_t)st.st_size != FILE_BYTES) {
        fail_text("unexpected file size");
    }

    for (uint64_t index = 0; index < OPERATIONS; ++index) {
        read_timing.reads[index][0] = now_ns(CLOCK_MONOTONIC);
        full_read(fd, buffer, BLOCK_BYTES);
        read_timing.reads[index][1] = now_ns(CLOCK_MONOTONIC);
        fill_block(expected, index);
        if (memcmp(buffer, expected, BLOCK_BYTES) != 0) {
            fail_text("content mismatch");
        }
    }

    unsigned char eof_byte;
    read_timing.eof[0] = now_ns(CLOCK_MONOTONIC);
    ssize_t eof = read(fd, &eof_byte, 1);
    read_timing.eof[1] = now_ns(CLOCK_MONOTONIC);
    if (eof < 0) {
        fail("eof read");
    }
    if (eof != 0) {
        fail_text("trailing data");
    }

    uint64_t barrier_begin = now_ns(CLOCK_MONOTONIC);
    if (close(fd) != 0) {
        fail("close");
    }
    read_timing.close[0] = barrier_begin;
    read_timing.close[1] = now_ns(CLOCK_MONOTONIC);
    uint64_t barrier_ns = read_timing.close[1] - barrier_begin;
    uint64_t cpu_ns = now_ns(CLOCK_PROCESS_CPUTIME_ID) - cpu_begin;
    uint64_t wall_ns = now_ns(CLOCK_MONOTONIC) - begin;
    read_timing.task_begin = begin;
    read_timing.task_end = begin + wall_ns;
    free(buffer);
    free(expected);
    emit_success("seq-read", "close", wall_ns, cpu_ns, barrier_ns);
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3 && argc != 4) {
        fprintf(stderr, "usage: %s write|read ABS_PATH [GENERATION_1_TO_6]\n", argv[0]);
        return 2;
    }
    if (argc == 4) {
        if (argv[3][0] < '1' || argv[3][0] > '6' || argv[3][1] != '\0') {
            fail_text("generation must be an integer from1 to6");
        }
        generation = (uint64_t)(argv[3][0] - '0');
        DATASET = "counter-generation-1m-v1";
    }
    if (argv[2][0] != '/') {
        fprintf(stderr, "path must be absolute\n");
        return 2;
    }
    if (strcmp(argv[1], "write") == 0) {
        return write_file(argv[2]);
    }
    if (strcmp(argv[1], "read") == 0) {
        return read_file(argv[2]);
    }
    fprintf(stderr, "usage: %s write|read ABS_PATH\n", argv[0]);
    return 2;
}
