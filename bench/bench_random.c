#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <rocksdb/c.h>
#include <lmdb.h>
#include <libk4.h>

#define DB_PATH "testdb"
#define NUM_OPS 10000

void benchmark_rocksdb();
void benchmark_lmdb();
void benchmark_k4();
void generate_random_key(char *key, int length);

int main() {
    srand(time(NULL));
    benchmark_rocksdb();
    benchmark_lmdb();
    benchmark_k4();
    return 0;
}

void generate_random_key(char *key, int length) {
    for (int i = 0; i < length - 1; i++) {
        key[i] = 'a' + rand() % 26;
    }
    key[length - 1] = '\0';
}

void benchmark_k4() {
    void* db = db_open(DB_PATH, (1024*1024)*256, 3600, 0, 0);
    if (db == NULL) {
        fprintf(stderr, "Error opening K4 database\n");
        return;
    }

    char key[20], value[20];
    clock_t start, end;
    double cpu_time_used;

    // Benchmark Put
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key, sizeof(key));
        sprintf(value, "value%d", i);
        db_put(db, key, strlen(key), value, strlen(value), 0);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("K4 Put: %f seconds\n", cpu_time_used);

    // Benchmark Get
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key, sizeof(key));
        char* read_value = db_get(db, key, strlen(key));
        free(read_value);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("K4 Get: %f seconds\n", cpu_time_used);

    // Benchmark Delete
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key, sizeof(key));
        db_delete(db, key, strlen(key));
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("K4 Delete: %f seconds\n", cpu_time_used);

    db_close(db);

    remove(DB_PATH);
}

void benchmark_rocksdb() {
    rocksdb_t *db;
    rocksdb_options_t *options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    char *err = NULL;

    db = rocksdb_open(options, DB_PATH, &err);
    if (err != NULL) {
        fprintf(stderr, "Error opening RocksDB: %s\n", err);
        return;
    }

    char key[20], value[20];
    clock_t start, end;
    double cpu_time_used;

    // Benchmark Put
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key, sizeof(key));
        sprintf(value, "value%d", i);
        rocksdb_put(db, rocksdb_writeoptions_create(), key, strlen(key), value, strlen(value), &err);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("RocksDB Put: %f seconds\n", cpu_time_used);

    // Benchmark Get
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key, sizeof(key));
        size_t read_len;
        char *read_value = rocksdb_get(db, rocksdb_readoptions_create(), key, strlen(key), &read_len, &err);
        free(read_value);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("RocksDB Get: %f seconds\n", cpu_time_used);

    // Benchmark Delete
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key, sizeof(key));
        rocksdb_delete(db, rocksdb_writeoptions_create(), key, strlen(key), &err);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("RocksDB Delete: %f seconds\n", cpu_time_used);

    rocksdb_close(db);
    rocksdb_options_destroy(options);

    remove(DB_PATH);
}

void benchmark_lmdb() {
    MDB_env *env;
    MDB_dbi dbi;
    MDB_val key, value;
    MDB_txn *txn;
    int rc;

    rc = mdb_env_create(&env);
    rc = mdb_env_set_maxdbs(env, 1);
    rc = mdb_env_open(env, DB_PATH, 0, 0664);
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    rc = mdb_txn_commit(txn);

    char key_str[20], value_str[20];
    clock_t start, end;
    double cpu_time_used;

    // Benchmark Put
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key_str, sizeof(key_str));
        sprintf(value_str, "value%d", i);
        key.mv_size = strlen(key_str);
        key.mv_data = key_str;
        value.mv_size = strlen(value_str);
        value.mv_data = value_str;
        rc = mdb_txn_begin(env, NULL, 0, &txn);
        rc = mdb_put(txn, dbi, &key, &value, 0);
        rc = mdb_txn_commit(txn);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("LMDB Put: %f seconds\n", cpu_time_used);

    // Benchmark Get
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key_str, sizeof(key_str));
        key.mv_size = strlen(key_str);
        key.mv_data = key_str;
        rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
        rc = mdb_get(txn, dbi, &key, &value);
        rc = mdb_txn_commit(txn);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("LMDB Get: %f seconds\n", cpu_time_used);

    // Benchmark Delete
    start = clock();
    for (int i = 0; i < NUM_OPS; i++) {
        generate_random_key(key_str, sizeof(key_str));
        key.mv_size = strlen(key_str);
        key.mv_data = key_str;
        rc = mdb_txn_begin(env, NULL, 0, &txn);
        rc = mdb_del(txn, dbi, &key, NULL);
        rc = mdb_txn_commit(txn);
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("LMDB Delete: %f seconds\n", cpu_time_used);

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);

    remove(DB_PATH);
}