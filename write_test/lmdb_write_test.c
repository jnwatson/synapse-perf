#define _GNU_SOURCE

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include "lmdb.h"

// This will only work on Linux; cribbed from how psutil does it
size_t get_my_uss(void)
{
    FILE *fp;
    ssize_t read;
    char * line = NULL;
    size_t len = 0;
    size_t kb_total = 0;

    fp = fopen("/proc/self/smaps", "r");
    if (!fp)
    {
        printf("failure in get_my_uss\n");
        return -1;
    }

    while ((read = getline(&line, &len, fp)) != -1) {
        size_t kb;
        if (0 != strncmp(line, "Private_", 8))
        {
            continue;
        }
        char * colon_p = strchr(line, ':');
        if (NULL == colon_p)
        {
            continue;
        }
        kb = 0;
        sscanf(colon_p + 1, " %zu kB", &kb);
        kb_total += kb;
    }

    free(line);
    fclose(fp);
    return kb_total * 1024;
}

#define DATA_SIZE 1024
#define BATCH_SIZE 128

#define MAP_SIZE (30ULL * 1024 * 1024 * 1024)

double get_time()
{
    struct timespec ts_o;
    int rc;
    rc = clock_gettime(CLOCK_MONOTONIC_RAW, &ts_o);
    if (rc)
    {
        return 0.0;
    }
    return (ts_o.tv_sec * 1000000000 + ts_o.tv_nsec) / 1000000000.0;
}

void fill_db(size_t size_in_mb, char const * filename)
{
    MDB_env *env;
    MDB_txn *txn;
    size_t key;
    size_t alt_key;
    MDB_val key_struct = {sizeof(alt_key), &alt_key};
    uint8_t val_buf[DATA_SIZE];
    MDB_val val_struct = {sizeof(val_buf), val_buf};
    size_t total_size = size_in_mb * 1024 * 1024;
    MDB_dbi dbi;
    int rc;
    int line_fail = 0;
    int flags = MDB_NOSUBDIR | MDB_NOMETASYNC | MDB_NOSYNC | MDB_NORDAHEAD | MDB_WRITEMAP | MDB_NOMEMINIT | MDB_NOLOCK;
    rc = mdb_env_create(&env);
    if (rc) { line_fail = __LINE__; goto fail; }

    rc = mdb_env_set_mapsize(env, MAP_SIZE);
    if (rc) { line_fail = __LINE__; goto fail; }

    rc = mdb_env_open(env, filename, flags, 0777);
    if (rc) { line_fail = __LINE__; goto fail; }

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc) { line_fail = __LINE__; goto fail; }

    rc = mdb_dbi_open(txn, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi);
    if (rc) { line_fail = __LINE__; goto fail; }

    rc = mdb_txn_commit(txn);
    if (rc) { line_fail = __LINE__; goto fail; }

    srand(time(NULL));

    size_t first_key = ((uint64_t)rand() << 32ull) | rand();
    size_t last_key = first_key;
    key = first_key;
    printf("First key is %zu\n", first_key);
    double start_time = get_time();
    double last_now = start_time;
    memset(val_buf, 0xa5, sizeof(val_buf));
    int urandom_f = open("/dev/urandom", O_RDONLY);
        if (!urandom_f) { line_fail = __LINE__; goto fail; }

    for (int i=0; i < (total_size / DATA_SIZE / BATCH_SIZE); i++)
    {
        if (i && (i % 512 == 0)) {
            // size_t uss = (get_my_uss() / 1024 / 1024);
            // printf("uss=%zuMiB\n", uss);

            double now = get_time();
            uint64_t mib = DATA_SIZE * (key - first_key) / 1024 / 1024;
            float mib_s = (DATA_SIZE * (key - last_key) / 1024.0 / 1024.0)/(now - last_now);
            printf("MiB=%" PRIu64 ", MiB/s=%.3f\n", mib, mib_s);
            printf("> {\"c_lmdb\": {\"mib\": %" PRIu64 ", \"mib_s\": %.3f}}\n", mib, mib_s);
            last_key = key;
            last_now = now;
        }
        rc = mdb_txn_begin(env, NULL, 0, &txn);
        if (rc) { line_fail = __LINE__; goto fail; }

        for (int j=0; j<BATCH_SIZE; j++)
        {
            key++;
            rc = read(urandom_f, &alt_key, sizeof(alt_key));
            if (rc != sizeof(alt_key)) { line_fail = __LINE__; goto fail; }
            rc = mdb_put(txn, dbi, &key_struct, &val_struct, 0);
        }

        rc = mdb_txn_commit(txn);
        if (rc) { line_fail = __LINE__; goto fail; }

    }
    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    uint64_t mib = DATA_SIZE * (key - first_key) / 1024 / 1024;
    double now = get_time();
    printf("Cum MiB=%" PRIu64 ", MiB/s=%.2f\n", mib, mib/(now - start_time));
    printf("> {\"c_lmdb cum\": {\"mib\": %" PRIu64 ", \"mib_s\": %.3f}}\n", mib, mib/(now - start_time));

    return;

fail:
    printf("Failed with error %d on line %d\n", rc, line_fail);
}

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        printf("usage: size_in_mb, filename\n");
        return 1;
    }
    fill_db(atoi(argv[1]), argv[2]);
    return 0;
}
