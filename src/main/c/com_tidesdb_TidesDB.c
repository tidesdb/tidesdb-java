/**
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Alex Gaetano Padula
 *
 * Licensed under the Mozilla Public License, v. 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.mozilla.org/en-US/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <tidesdb/db.h>

static void throwTidesDBException(JNIEnv *env, int errorCode, const char *message)
{
    jclass exClass = (*env)->FindClass(env, "com/tidesdb/TidesDBException");
    if (exClass == NULL)
    {
        return;
    }

    jmethodID constructor = (*env)->GetMethodID(env, exClass, "<init>", "(Ljava/lang/String;I)V");
    if (constructor == NULL)
    {
        (*env)->ThrowNew(env, exClass, message);
        return;
    }

    jstring jMessage = (*env)->NewStringUTF(env, message);
    jthrowable exception =
        (jthrowable)(*env)->NewObject(env, exClass, constructor, jMessage, errorCode);
    (*env)->Throw(env, exception);
}

static const char *getErrorMessage(int code)
{
    switch (code)
    {
        case TDB_ERR_MEMORY:
            return "memory allocation failed";
        case TDB_ERR_INVALID_ARGS:
            return "invalid arguments";
        case TDB_ERR_NOT_FOUND:
            return "not found";
        case TDB_ERR_IO:
            return "I/O error";
        case TDB_ERR_CORRUPTION:
            return "data corruption";
        case TDB_ERR_EXISTS:
            return "already exists";
        case TDB_ERR_CONFLICT:
            return "transaction conflict";
        case TDB_ERR_TOO_LARGE:
            return "key or value too large";
        case TDB_ERR_MEMORY_LIMIT:
            return "memory limit exceeded";
        case TDB_ERR_INVALID_DB:
            return "invalid database handle";
        case TDB_ERR_LOCKED:
            return "database is locked";
        default:
            return "unknown error";
    }
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeOpen(JNIEnv *env, jclass cls, jstring dbPath,
                                                            jint numFlushThreads,
                                                            jint numCompactionThreads,
                                                            jint logLevel, jlong blockCacheSize,
                                                            jlong maxOpenSSTables)
{
    const char *path = (*env)->GetStringUTFChars(env, dbPath, NULL);
    if (path == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get database path");
        return 0;
    }

    tidesdb_config_t config = {.db_path = (char *)path,
                               .num_flush_threads = numFlushThreads,
                               .num_compaction_threads = numCompactionThreads,
                               .log_level = (tidesdb_log_level_t)logLevel,
                               .block_cache_size = (size_t)blockCacheSize,
                               .max_open_sstables = (size_t)maxOpenSSTables};

    tidesdb_t *db = NULL;
    int result = tidesdb_open(&config, &db);

    (*env)->ReleaseStringUTFChars(env, dbPath, path);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return 0;
    }

    return (jlong)(uintptr_t)db;
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeClose(JNIEnv *env, jclass cls, jlong handle)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    if (db != NULL)
    {
        tidesdb_close(db);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeCreateColumnFamily(
    JNIEnv *env, jclass cls, jlong handle, jstring name, jlong writeBufferSize,
    jlong levelSizeRatio, jint minLevels, jint dividingLevelOffset, jlong klogValueThreshold,
    jint compressionAlgorithm, jboolean enableBloomFilter, jdouble bloomFPR,
    jboolean enableBlockIndexes, jint indexSampleRatio, jint blockIndexPrefixLen, jint syncMode,
    jlong syncIntervalUs, jstring comparatorName, jint skipListMaxLevel, jfloat skipListProbability,
    jint defaultIsolationLevel, jlong minDiskSpace, jint l1FileCountTrigger,
    jint l0QueueStallThreshold)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    const char *cfName = (*env)->GetStringUTFChars(env, name, NULL);
    if (cfName == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get column family name");
        return;
    }

    const char *compName = NULL;
    if (comparatorName != NULL)
    {
        compName = (*env)->GetStringUTFChars(env, comparatorName, NULL);
    }

    tidesdb_column_family_config_t config = {
        .write_buffer_size = (size_t)writeBufferSize,
        .level_size_ratio = (size_t)levelSizeRatio,
        .min_levels = minLevels,
        .dividing_level_offset = dividingLevelOffset,
        .klog_value_threshold = (size_t)klogValueThreshold,
        .compression_algorithm = (compression_algorithm)compressionAlgorithm,
        .enable_bloom_filter = enableBloomFilter ? 1 : 0,
        .bloom_fpr = bloomFPR,
        .enable_block_indexes = enableBlockIndexes ? 1 : 0,
        .index_sample_ratio = indexSampleRatio,
        .block_index_prefix_len = blockIndexPrefixLen,
        .sync_mode = syncMode,
        .sync_interval_us = (uint64_t)syncIntervalUs,
        .skip_list_max_level = skipListMaxLevel,
        .skip_list_probability = skipListProbability,
        .default_isolation_level = (tidesdb_isolation_level_t)defaultIsolationLevel,
        .min_disk_space = (uint64_t)minDiskSpace,
        .l1_file_count_trigger = l1FileCountTrigger,
        .l0_queue_stall_threshold = l0QueueStallThreshold};

    memset(config.comparator_name, 0, TDB_MAX_COMPARATOR_NAME);
    if (compName != NULL && strlen(compName) > 0)
    {
        strncpy(config.comparator_name, compName, TDB_MAX_COMPARATOR_NAME - 1);
    }

    int result = tidesdb_create_column_family(db, cfName, &config);

    (*env)->ReleaseStringUTFChars(env, name, cfName);
    if (compName != NULL)
    {
        (*env)->ReleaseStringUTFChars(env, comparatorName, compName);
    }

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeDropColumnFamily(JNIEnv *env, jclass cls,
                                                                       jlong handle, jstring name)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    const char *cfName = (*env)->GetStringUTFChars(env, name, NULL);
    if (cfName == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get column family name");
        return;
    }

    int result = tidesdb_drop_column_family(db, cfName);

    (*env)->ReleaseStringUTFChars(env, name, cfName);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeGetColumnFamily(JNIEnv *env, jclass cls,
                                                                       jlong handle, jstring name)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    const char *cfName = (*env)->GetStringUTFChars(env, name, NULL);
    if (cfName == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get column family name");
        return 0;
    }

    tidesdb_column_family_t *cf = tidesdb_get_column_family(db, cfName);

    (*env)->ReleaseStringUTFChars(env, name, cfName);

    if (cf == NULL)
    {
        throwTidesDBException(env, TDB_ERR_NOT_FOUND, "Column family not found");
        return 0;
    }

    return (jlong)(uintptr_t)cf;
}

JNIEXPORT jobjectArray JNICALL Java_com_tidesdb_TidesDB_nativeListColumnFamilies(JNIEnv *env,
                                                                                 jclass cls,
                                                                                 jlong handle)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    char **names = NULL;
    int count = 0;

    int result = tidesdb_list_column_families(db, &names, &count);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return NULL;
    }

    jclass stringClass = (*env)->FindClass(env, "java/lang/String");
    jobjectArray array = (*env)->NewObjectArray(env, count, stringClass, NULL);

    for (int i = 0; i < count; i++)
    {
        jstring str = (*env)->NewStringUTF(env, names[i]);
        (*env)->SetObjectArrayElement(env, array, i, str);
        free(names[i]);
    }
    free(names);

    return array;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransaction(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    tidesdb_txn_t *txn = NULL;

    int result = tidesdb_txn_begin(db, &txn);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return 0;
    }

    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransactionWithIsolation(
    JNIEnv *env, jclass cls, jlong handle, jint isolationLevel)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    tidesdb_txn_t *txn = NULL;

    int result =
        tidesdb_txn_begin_with_isolation(db, (tidesdb_isolation_level_t)isolationLevel, &txn);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return 0;
    }

    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_TidesDB_nativeGetCacheStats(JNIEnv *env, jclass cls,
                                                                       jlong handle)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    tidesdb_cache_stats_t stats;

    int result = tidesdb_get_cache_stats(db, &stats);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return NULL;
    }

    jclass cacheStatsClass = (*env)->FindClass(env, "com/tidesdb/CacheStats");
    jmethodID constructor = (*env)->GetMethodID(env, cacheStatsClass, "<init>", "(ZJJJJDJ)V");

    return (*env)->NewObject(env, cacheStatsClass, constructor, stats.enabled != 0,
                             (jlong)stats.total_entries, (jlong)stats.total_bytes,
                             (jlong)stats.hits, (jlong)stats.misses, stats.hit_rate,
                             (jlong)stats.num_partitions);
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeRegisterComparator(JNIEnv *env, jclass cls,
                                                                         jlong handle, jstring name,
                                                                         jstring context)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    const char *compName = (*env)->GetStringUTFChars(env, name, NULL);
    if (compName == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get comparator name");
        return;
    }

    const char *ctx = NULL;
    if (context != NULL)
    {
        ctx = (*env)->GetStringUTFChars(env, context, NULL);
    }

    int result = tidesdb_register_comparator(db, compName, NULL, ctx, NULL);

    (*env)->ReleaseStringUTFChars(env, name, compName);
    if (ctx != NULL)
    {
        (*env)->ReleaseStringUTFChars(env, context, ctx);
    }

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeBackup(JNIEnv *env, jclass cls, jlong handle,
                                                             jstring dir)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    const char *backupDir = (*env)->GetStringUTFChars(env, dir, NULL);
    if (backupDir == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get backup directory");
        return;
    }

    int result = tidesdb_backup(db, (char *)backupDir);

    (*env)->ReleaseStringUTFChars(env, dir, backupDir);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeRenameColumnFamily(JNIEnv *env, jclass cls,
                                                                          jlong handle,
                                                                          jstring oldName,
                                                                          jstring newName)
{
    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;
    const char *oldCfName = (*env)->GetStringUTFChars(env, oldName, NULL);
    if (oldCfName == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get old column family name");
        return;
    }

    const char *newCfName = (*env)->GetStringUTFChars(env, newName, NULL);
    if (newCfName == NULL)
    {
        (*env)->ReleaseStringUTFChars(env, oldName, oldCfName);
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to get new column family name");
        return;
    }

    int result = tidesdb_rename_column_family(db, oldCfName, newCfName);

    (*env)->ReleaseStringUTFChars(env, oldName, oldCfName);
    (*env)->ReleaseStringUTFChars(env, newName, newCfName);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_ColumnFamily_nativeGetStats(JNIEnv *env, jclass cls,
                                                                       jlong handle)
{
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)handle;
    tidesdb_stats_t *stats = NULL;

    int result = tidesdb_get_stats(cf, &stats);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return NULL;
    }

    jlongArray levelSizes = (*env)->NewLongArray(env, stats->num_levels);
    if (stats->level_sizes != NULL)
    {
        jlong *sizes = malloc(stats->num_levels * sizeof(jlong));
        for (int i = 0; i < stats->num_levels; i++)
        {
            sizes[i] = (jlong)stats->level_sizes[i];
        }
        (*env)->SetLongArrayRegion(env, levelSizes, 0, stats->num_levels, sizes);
        free(sizes);
    }

    jintArray levelNumSSTables = (*env)->NewIntArray(env, stats->num_levels);
    if (stats->level_num_sstables != NULL)
    {
        jint *nums = malloc(stats->num_levels * sizeof(jint));
        for (int i = 0; i < stats->num_levels; i++)
        {
            nums[i] = stats->level_num_sstables[i];
        }
        (*env)->SetIntArrayRegion(env, levelNumSSTables, 0, stats->num_levels, nums);
        free(nums);
    }

    jclass statsClass = (*env)->FindClass(env, "com/tidesdb/Stats");
    jmethodID constructor =
        (*env)->GetMethodID(env, statsClass, "<init>", "(IJ[J[ILcom/tidesdb/ColumnFamilyConfig;)V");

    jobject statsObj =
        (*env)->NewObject(env, statsClass, constructor, stats->num_levels,
                          (jlong)stats->memtable_size, levelSizes, levelNumSSTables, NULL);

    tidesdb_free_stats(stats);

    return statsObj;
}

JNIEXPORT void JNICALL Java_com_tidesdb_ColumnFamily_nativeCompact(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)handle;
    int result = tidesdb_compact(cf);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_ColumnFamily_nativeFlushMemtable(JNIEnv *env, jclass cls,
                                                                         jlong handle)
{
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)handle;
    int result = tidesdb_flush_memtable(cf);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_ColumnFamily_nativeIsFlushing(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)handle;
    return tidesdb_is_flushing(cf) != 0;
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_ColumnFamily_nativeIsCompacting(JNIEnv *env, jclass cls,
                                                                            jlong handle)
{
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)handle;
    return tidesdb_is_compacting(cf) != 0;
}

JNIEXPORT void JNICALL Java_com_tidesdb_ColumnFamily_nativeUpdateRuntimeConfig(
    JNIEnv *env, jclass cls, jlong handle, jlong writeBufferSize, jint skipListMaxLevel,
    jfloat skipListProbability, jdouble bloomFPR, jint indexSampleRatio, jint syncMode,
    jlong syncIntervalUs, jboolean persistToDisk)
{
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)handle;

    tidesdb_column_family_config_t config = {.write_buffer_size = (size_t)writeBufferSize,
                                             .skip_list_max_level = skipListMaxLevel,
                                             .skip_list_probability = skipListProbability,
                                             .bloom_fpr = bloomFPR,
                                             .index_sample_ratio = indexSampleRatio,
                                             .sync_mode = syncMode,
                                             .sync_interval_us = (uint64_t)syncIntervalUs};

    int result = tidesdb_cf_update_runtime_config(cf, &config, persistToDisk ? 1 : 0);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativePut(JNIEnv *env, jclass cls, jlong handle,
                                                              jlong cfHandle, jbyteArray key,
                                                              jbyteArray value, jlong ttl)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)cfHandle;

    jsize keyLen = (*env)->GetArrayLength(env, key);
    jsize valueLen = (*env)->GetArrayLength(env, value);

    jbyte *keyBytes = (*env)->GetByteArrayElements(env, key, NULL);
    jbyte *valueBytes = (*env)->GetByteArrayElements(env, value, NULL);

    int result = tidesdb_txn_put(txn, cf, (uint8_t *)keyBytes, keyLen, (uint8_t *)valueBytes,
                                 valueLen, (time_t)ttl);

    (*env)->ReleaseByteArrayElements(env, key, keyBytes, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, value, valueBytes, JNI_ABORT);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_Transaction_nativeGet(JNIEnv *env, jclass cls,
                                                                    jlong handle, jlong cfHandle,
                                                                    jbyteArray key)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)cfHandle;

    jsize keyLen = (*env)->GetArrayLength(env, key);
    jbyte *keyBytes = (*env)->GetByteArrayElements(env, key, NULL);

    uint8_t *value = NULL;
    size_t valueLen = 0;

    int result = tidesdb_txn_get(txn, cf, (uint8_t *)keyBytes, keyLen, &value, &valueLen);

    (*env)->ReleaseByteArrayElements(env, key, keyBytes, JNI_ABORT);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return NULL;
    }

    jbyteArray resultArray = (*env)->NewByteArray(env, valueLen);
    (*env)->SetByteArrayRegion(env, resultArray, 0, valueLen, (jbyte *)value);
    free(value);

    return resultArray;
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeDelete(JNIEnv *env, jclass cls,
                                                                 jlong handle, jlong cfHandle,
                                                                 jbyteArray key)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)cfHandle;

    jsize keyLen = (*env)->GetArrayLength(env, key);
    jbyte *keyBytes = (*env)->GetByteArrayElements(env, key, NULL);

    int result = tidesdb_txn_delete(txn, cf, (uint8_t *)keyBytes, keyLen);

    (*env)->ReleaseByteArrayElements(env, key, keyBytes, JNI_ABORT);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeCommit(JNIEnv *env, jclass cls,
                                                                 jlong handle)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    int result = tidesdb_txn_commit(txn);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeRollback(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    int result = tidesdb_txn_rollback(txn);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeSavepoint(JNIEnv *env, jclass cls,
                                                                    jlong handle, jstring name)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    const char *spName = (*env)->GetStringUTFChars(env, name, NULL);

    int result = tidesdb_txn_savepoint(txn, spName);

    (*env)->ReleaseStringUTFChars(env, name, spName);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeRollbackToSavepoint(JNIEnv *env,
                                                                              jclass cls,
                                                                              jlong handle,
                                                                              jstring name)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    const char *spName = (*env)->GetStringUTFChars(env, name, NULL);

    int result = tidesdb_txn_rollback_to_savepoint(txn, spName);

    (*env)->ReleaseStringUTFChars(env, name, spName);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeReleaseSavepoint(JNIEnv *env, jclass cls,
                                                                           jlong handle,
                                                                           jstring name)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    const char *spName = (*env)->GetStringUTFChars(env, name, NULL);

    int result = tidesdb_txn_release_savepoint(txn, spName);

    (*env)->ReleaseStringUTFChars(env, name, spName);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_Transaction_nativeNewIterator(JNIEnv *env, jclass cls,
                                                                       jlong handle, jlong cfHandle)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)cfHandle;
    tidesdb_iter_t *iter = NULL;

    int result = tidesdb_iter_new(txn, cf, &iter);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return 0;
    }

    return (jlong)(uintptr_t)iter;
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeFree(JNIEnv *env, jclass cls,
                                                               jlong handle)
{
    tidesdb_txn_t *txn = (tidesdb_txn_t *)(uintptr_t)handle;
    if (txn != NULL)
    {
        tidesdb_txn_free(txn);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeekToFirst(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    int result = tidesdb_iter_seek_to_first(iter);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeekToLast(JNIEnv *env, jclass cls,
                                                                         jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    int result = tidesdb_iter_seek_to_last(iter);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeek(JNIEnv *env, jclass cls,
                                                                   jlong handle, jbyteArray key)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    jsize keyLen = (*env)->GetArrayLength(env, key);
    jbyte *keyBytes = (*env)->GetByteArrayElements(env, key, NULL);

    int result = tidesdb_iter_seek(iter, (uint8_t *)keyBytes, keyLen);

    (*env)->ReleaseByteArrayElements(env, key, keyBytes, JNI_ABORT);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeekForPrev(JNIEnv *env, jclass cls,
                                                                          jlong handle,
                                                                          jbyteArray key)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    jsize keyLen = (*env)->GetArrayLength(env, key);
    jbyte *keyBytes = (*env)->GetByteArrayElements(env, key, NULL);

    int result = tidesdb_iter_seek_for_prev(iter, (uint8_t *)keyBytes, keyLen);

    (*env)->ReleaseByteArrayElements(env, key, keyBytes, JNI_ABORT);

    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_TidesDBIterator_nativeValid(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    return tidesdb_iter_valid(iter) != 0;
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeNext(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    int result = tidesdb_iter_next(iter);

    /* TDB_ERR_NOT_FOUND is expected when reaching end of iteration -- iterator becomes invalid */
    if (result != TDB_SUCCESS && result != TDB_ERR_NOT_FOUND)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativePrev(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    int result = tidesdb_iter_prev(iter);

    /* TDB_ERR_NOT_FOUND is expected when reaching start of iteration -- iterator becomes invalid */
    if (result != TDB_SUCCESS && result != TDB_ERR_NOT_FOUND)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
    }
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_TidesDBIterator_nativeKey(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    uint8_t *key = NULL;
    size_t keyLen = 0;

    int result = tidesdb_iter_key(iter, &key, &keyLen);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return NULL;
    }

    jbyteArray resultArray = (*env)->NewByteArray(env, keyLen);
    (*env)->SetByteArrayRegion(env, resultArray, 0, keyLen, (jbyte *)key);

    return resultArray;
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_TidesDBIterator_nativeValue(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    uint8_t *value = NULL;
    size_t valueLen = 0;

    int result = tidesdb_iter_value(iter, &value, &valueLen);
    if (result != TDB_SUCCESS)
    {
        throwTidesDBException(env, result, getErrorMessage(result));
        return NULL;
    }

    jbyteArray resultArray = (*env)->NewByteArray(env, valueLen);
    (*env)->SetByteArrayRegion(env, resultArray, 0, valueLen, (jbyte *)value);

    return resultArray;
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeFree(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    tidesdb_iter_t *iter = (tidesdb_iter_t *)(uintptr_t)handle;
    if (iter != NULL)
    {
        tidesdb_iter_free(iter);
    }
}
