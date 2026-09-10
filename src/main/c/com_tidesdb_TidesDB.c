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
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <tidesdb/db.h>

/* the largest Java array length. jsize is a signed 32-bit int, so this is INT32_MAX -- written out
 * rather than derived by shifting ~0, which sign-extends and yields -1 instead. */
#ifndef JSIZE_MAX
#define JSIZE_MAX ((jsize)0x7fffffff)
#endif

/* the most encoding chains tidesdb_get_{klog,vlog}_encoding_stats will report */
#define JNI_MAX_ENCODING_CHAINS 16

/* ===== error reporting ===== */

/**
 * Throws com.tidesdb.TidesDBException carrying the native result code. The
 * message comes from tidesdb_strerror, which is a static literal that is never
 * NULL, so every code describes itself without a local table to keep in sync.
 */
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
        (*env)->DeleteLocalRef(env, exClass);
        return;
    }

    jstring jMessage = (*env)->NewStringUTF(env, message);
    jthrowable exception =
        (jthrowable)(*env)->NewObject(env, exClass, constructor, jMessage, errorCode);
    if (exception != NULL)
    {
        (*env)->Throw(env, exception);
    }
    (*env)->DeleteLocalRef(env, exClass);
}

/** Throws for a non-success result code, using the library's own description. */
static void throwResult(JNIEnv *env, int result)
{
    throwTidesDBException(env, result, tidesdb_strerror(result));
}

static int jvm_exception_pending(JNIEnv *env)
{
    return (*env)->ExceptionCheck(env) == JNI_TRUE;
}

/* ===== argument marshalling ===== */

/**
 * A borrowed view of a jbyteArray. A NULL array yields a NULL pointer and a zero
 * length, which is what every optional bound in the API wants.
 */
typedef struct
{
    jbyteArray array;
    jbyte *data;
    jsize length;
} jni_bytes_t;

/**
 * Acquires the elements of a byte array. Returns 0 on success, -1 if the JVM
 * could not hand over the buffer (in which case an OutOfMemoryError is already
 * pending and the caller must return without throwing its own).
 */
static int acquireBytes(JNIEnv *env, jbyteArray array, jni_bytes_t *out)
{
    out->array = array;
    out->data = NULL;
    out->length = 0;

    if (array == NULL)
    {
        return 0;
    }

    out->length = (*env)->GetArrayLength(env, array);
    out->data = (*env)->GetByteArrayElements(env, array, NULL);
    if (out->data == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to acquire byte array");
        }
        return -1;
    }
    return 0;
}

/** Releases a view acquired by acquireBytes without copying anything back. */
static void releaseBytes(JNIEnv *env, jni_bytes_t *bytes)
{
    if (bytes->data != NULL)
    {
        (*env)->ReleaseByteArrayElements(env, bytes->array, bytes->data, JNI_ABORT);
        bytes->data = NULL;
    }
}

/**
 * Copies a library-allocated buffer into a new Java byte array and frees the
 * original with tidesdb_free. Returns NULL with an exception pending on failure.
 */
static jbyteArray toByteArrayAndFree(JNIEnv *env, uint8_t *buffer, size_t size)
{
    if (size > (size_t)JSIZE_MAX)
    {
        tidesdb_free(buffer);
        throwTidesDBException(env, TDB_ERR_TOO_LARGE, "Value exceeds the maximum Java array length");
        return NULL;
    }

    jbyteArray result = (*env)->NewByteArray(env, (jsize)size);
    if (result == NULL)
    {
        tidesdb_free(buffer);
        return NULL;
    }
    if (size > 0)
    {
        (*env)->SetByteArrayRegion(env, result, 0, (jsize)size, (const jbyte *)buffer);
    }
    tidesdb_free(buffer);
    return result;
}

/** Builds a long[] from a native array. Returns NULL with an exception pending on failure. */
static jlongArray newLongArray(JNIEnv *env, const uint64_t *values, jsize count)
{
    jlongArray array = (*env)->NewLongArray(env, count);
    if (array == NULL)
    {
        return NULL;
    }
    jlong stack[TDB_MAX_LEVELS];
    for (jsize i = 0; i < count; i++)
    {
        stack[i] = (jlong)values[i];
    }
    (*env)->SetLongArrayRegion(env, array, 0, count, stack);
    return array;
}

/** Builds a long[] from a native size_t array. */
static jlongArray newLongArrayFromSizes(JNIEnv *env, const size_t *values, jsize count)
{
    jlongArray array = (*env)->NewLongArray(env, count);
    if (array == NULL)
    {
        return NULL;
    }
    jlong stack[TDB_MAX_LEVELS];
    for (jsize i = 0; i < count; i++)
    {
        stack[i] = (jlong)values[i];
    }
    (*env)->SetLongArrayRegion(env, array, 0, count, stack);
    return array;
}

/** Builds an int[] from a native int array. */
static jintArray newIntArray(JNIEnv *env, const int *values, jsize count)
{
    jintArray array = (*env)->NewIntArray(env, count);
    if (array == NULL)
    {
        return NULL;
    }
    jint stack[TDB_MAX_LEVELS];
    for (jsize i = 0; i < count; i++)
    {
        stack[i] = (jint)values[i];
    }
    (*env)->SetIntArrayRegion(env, array, 0, count, stack);
    return array;
}

/** Builds an int[] from a native uint8_t array, used for encoding pipelines. */
static jintArray newIntArrayFromBytes(JNIEnv *env, const uint8_t *values, jsize count)
{
    jintArray array = (*env)->NewIntArray(env, count);
    if (array == NULL)
    {
        return NULL;
    }
    jint stack[TDB_ENCODING_PIPELINE_MAX];
    for (jsize i = 0; i < count; i++)
    {
        stack[i] = (jint)values[i];
    }
    (*env)->SetIntArrayRegion(env, array, 0, count, stack);
    return array;
}

/**
 * Copies a Java int[] of encoding ids into a config's fixed pipeline slot.
 * Returns 0 on success, -1 with an exception pending otherwise. The Java builder
 * already bounds the length and each id, so a violation here is a programming
 * error rather than user input.
 */
static int fillEncodingPipeline(JNIEnv *env, jintArray ids, uint8_t *pipeline, uint8_t *count)
{
    *count = 0;
    if (ids == NULL)
    {
        return 0;
    }

    jsize length = (*env)->GetArrayLength(env, ids);
    if (length > TDB_ENCODING_PIPELINE_MAX)
    {
        throwTidesDBException(env, TDB_ERR_INVALID_ARGS, "Encoding pipeline is too long");
        return -1;
    }
    if (length == 0)
    {
        return 0;
    }

    jint stack[TDB_ENCODING_PIPELINE_MAX];
    (*env)->GetIntArrayRegion(env, ids, 0, length, stack);
    if (jvm_exception_pending(env))
    {
        return -1;
    }

    for (jsize i = 0; i < length; i++)
    {
        if (stack[i] < 0 || stack[i] > 255)
        {
            throwTidesDBException(env, TDB_ERR_INVALID_ARGS, "Encoding id is outside [0, 255]");
            return -1;
        }
        pipeline[i] = (uint8_t)stack[i];
    }
    *count = (uint8_t)length;
    return 0;
}

/**
 * Populates a column family config from the flat field list the Java side sends.
 * The struct is zeroed first, so the commit hook fields stay NULL and the name
 * stays empty -- the create and update calls take the name from elsewhere.
 */
static int fillCfConfig(JNIEnv *env, tidesdb_column_family_config_t *cfg, jlong levelSizeRatio,
                        jint minLevels, jint dividingLevelOffset, jboolean keepValuesInline,
                        jlong btreeKlogBlockSize, jintArray encodingPipeline,
                        jboolean enableBloomFilter, jdouble bloomFpr, jint defaultIsolationLevel,
                        jint l1FileCountTrigger, jdouble tombstoneDensityTrigger,
                        jlong tombstoneDensityMinEntries)
{
    memset(cfg, 0, sizeof(*cfg));

    cfg->level_size_ratio = (size_t)levelSizeRatio;
    cfg->min_levels = (int)minLevels;
    cfg->dividing_level_offset = (int)dividingLevelOffset;
    cfg->keep_values_inline = keepValuesInline ? 1 : 0;
    cfg->btree_klog_block_size = (size_t)btreeKlogBlockSize;
    cfg->enable_bloom_filter = enableBloomFilter ? 1 : 0;
    cfg->bloom_fpr = (double)bloomFpr;
    cfg->default_isolation_level = (tidesdb_isolation_level_t)defaultIsolationLevel;
    cfg->l1_file_count_trigger = (int)l1FileCountTrigger;
    cfg->tombstone_density_trigger = (double)tombstoneDensityTrigger;
    cfg->tombstone_density_min_entries = (uint64_t)tombstoneDensityMinEntries;

    return fillEncodingPipeline(env, encodingPipeline, cfg->encoding_pipeline,
                                &cfg->encoding_count);
}

/* ===== object builders ===== */

/** Builds a com.tidesdb.ColumnFamilyConfig mirroring a native config. */
static jobject buildCfConfigObject(JNIEnv *env, const tidesdb_column_family_config_t *cfg)
{
    jclass cls = (*env)->FindClass(env, "com/tidesdb/ColumnFamilyConfig");
    if (cls == NULL) return NULL;

    jmethodID factory = (*env)->GetStaticMethodID(
        env, cls, "fromNative",
        "(Ljava/lang/String;JIIZJ[IZDIIDJ)Lcom/tidesdb/ColumnFamilyConfig;");
    if (factory == NULL)
    {
        (*env)->DeleteLocalRef(env, cls);
        return NULL;
    }

    /* the name is a fixed char array; bound the read in case it is not terminated */
    char nameBuf[TDB_MAX_CF_NAME_LEN + 1];
    memcpy(nameBuf, cfg->name, TDB_MAX_CF_NAME_LEN);
    nameBuf[TDB_MAX_CF_NAME_LEN] = '\0';

    jstring name = (*env)->NewStringUTF(env, nameBuf);
    if (name == NULL)
    {
        (*env)->DeleteLocalRef(env, cls);
        return NULL;
    }

    uint8_t pipelineCount = cfg->encoding_count;
    if (pipelineCount > TDB_ENCODING_PIPELINE_MAX) pipelineCount = TDB_ENCODING_PIPELINE_MAX;
    jintArray pipeline = newIntArrayFromBytes(env, cfg->encoding_pipeline, (jsize)pipelineCount);
    if (pipeline == NULL)
    {
        (*env)->DeleteLocalRef(env, name);
        (*env)->DeleteLocalRef(env, cls);
        return NULL;
    }

    jobject result = (*env)->CallStaticObjectMethod(
        env, cls, factory, name, (jlong)cfg->level_size_ratio, (jint)cfg->min_levels,
        (jint)cfg->dividing_level_offset, cfg->keep_values_inline ? JNI_TRUE : JNI_FALSE,
        (jlong)cfg->btree_klog_block_size, pipeline,
        cfg->enable_bloom_filter ? JNI_TRUE : JNI_FALSE, (jdouble)cfg->bloom_fpr,
        (jint)cfg->default_isolation_level, (jint)cfg->l1_file_count_trigger,
        (jdouble)cfg->tombstone_density_trigger, (jlong)cfg->tombstone_density_min_entries);

    (*env)->DeleteLocalRef(env, pipeline);
    (*env)->DeleteLocalRef(env, name);
    (*env)->DeleteLocalRef(env, cls);
    return result;
}

/** Builds a com.tidesdb.Config mirroring a native config. */
static jobject buildConfigObject(JNIEnv *env, const tidesdb_config_t *cfg)
{
    jclass cls = (*env)->FindClass(env, "com/tidesdb/Config");
    if (cls == NULL) return NULL;

    jmethodID factory =
        (*env)->GetStaticMethodID(env, cls, "fromNative", "(IIIJJZJJIFIJJJIIJ)Lcom/tidesdb/Config;");
    if (factory == NULL)
    {
        (*env)->DeleteLocalRef(env, cls);
        return NULL;
    }

    jobject result = (*env)->CallStaticObjectMethod(
        env, cls, factory, (jint)cfg->num_flush_threads, (jint)cfg->num_compaction_threads,
        (jint)cfg->log_level, (jlong)cfg->block_cache_size, (jlong)cfg->max_open_sstables,
        cfg->log_to_file ? JNI_TRUE : JNI_FALSE, (jlong)cfg->log_truncation_at,
        (jlong)cfg->memtable_write_buffer_size, (jint)cfg->memtable_skip_list_max_level,
        (jfloat)cfg->memtable_skip_list_probability, (jint)cfg->memtable_sync_mode,
        (jlong)cfg->memtable_sync_interval_us, (jlong)cfg->value_separation_threshold,
        (jlong)cfg->vlog_segment_size, (jint)cfg->memtable_l0_queue_stall_threshold,
        (jint)cfg->memtable_idle_flush_seconds, (jlong)cfg->txn_timeout_seconds);

    (*env)->DeleteLocalRef(env, cls);
    return result;
}

/** Builds a com.tidesdb.CfStats mirroring native per-column-family statistics. */
static jobject buildCfStatsObject(JNIEnv *env, const tidesdb_cf_stats_t *stats)
{
    jclass cls = (*env)->FindClass(env, "com/tidesdb/CfStats");
    if (cls == NULL) return NULL;

    jmethodID ctor = (*env)->GetMethodID(
        env, cls, "<init>",
        "(ILcom/tidesdb/ColumnFamilyConfig;[J[I[J[JJJDDDJJDJDDIJJJJJJJJ)V");
    if (ctor == NULL)
    {
        (*env)->DeleteLocalRef(env, cls);
        return NULL;
    }

    jobject config = buildCfConfigObject(env, &stats->config);
    jlongArray levelSizes = newLongArrayFromSizes(env, stats->level_sizes, TDB_MAX_LEVELS);
    jintArray levelSstables = newIntArray(env, stats->level_num_sstables, TDB_MAX_LEVELS);
    jlongArray levelKeys = newLongArray(env, stats->level_key_counts, TDB_MAX_LEVELS);
    jlongArray levelTombstones = newLongArray(env, stats->level_tombstone_counts, TDB_MAX_LEVELS);

    jobject result = NULL;
    if (config != NULL && levelSizes != NULL && levelSstables != NULL && levelKeys != NULL &&
        levelTombstones != NULL)
    {
        result = (*env)->NewObject(
            env, cls, ctor, (jint)stats->num_levels, config, levelSizes, levelSstables, levelKeys,
            levelTombstones, (jlong)stats->total_keys, (jlong)stats->total_data_size,
            (jdouble)stats->avg_key_size, (jdouble)stats->avg_value_size, (jdouble)stats->read_amp,
            (jlong)stats->btree_total_nodes, (jlong)stats->btree_max_height,
            (jdouble)stats->btree_avg_height, (jlong)stats->total_tombstones,
            (jdouble)stats->tombstone_ratio, (jdouble)stats->max_sst_density,
            (jint)stats->max_sst_density_level, (jlong)stats->wal_bytes_written,
            (jlong)stats->flush_bytes_written, (jlong)stats->compaction_bytes_written,
            (jlong)stats->compaction_bytes_read, (jlong)stats->user_bytes_written,
            (jlong)stats->compaction_count, (jlong)stats->unflushed_key_count,
            (jlong)stats->filter_resident_bytes);
    }

    if (levelTombstones != NULL) (*env)->DeleteLocalRef(env, levelTombstones);
    if (levelKeys != NULL) (*env)->DeleteLocalRef(env, levelKeys);
    if (levelSstables != NULL) (*env)->DeleteLocalRef(env, levelSstables);
    if (levelSizes != NULL) (*env)->DeleteLocalRef(env, levelSizes);
    if (config != NULL) (*env)->DeleteLocalRef(env, config);
    (*env)->DeleteLocalRef(env, cls);
    return result;
}

/** Builds a com.tidesdb.DbStats mirroring native database statistics. */
static jobject buildDbStatsObject(JNIEnv *env, const tidesdb_db_stats_t *stats)
{
    jclass cls = (*env)->FindClass(env, "com/tidesdb/DbStats");
    if (cls == NULL) return NULL;

    jmethodID ctor = (*env)->GetMethodID(
        env, cls, "<init>", "(IIIIJIJJIJJZJJJJJJJJJJJJJJJJJJJJJJJJJ)V");
    if (ctor == NULL)
    {
        (*env)->DeleteLocalRef(env, cls);
        return NULL;
    }

    jobject result = (*env)->NewObject(
        env, cls, ctor, (jint)stats->num_column_families, (jint)stats->immutable_memtable_count,
        (jint)stats->compaction_pending_count, (jint)stats->total_sstable_count,
        (jlong)stats->total_data_size_bytes, (jint)stats->num_open_sstables,
        (jlong)stats->global_seq, (jlong)stats->min_snapshot_seq, (jint)stats->active_txn_count,
        (jlong)stats->txn_memory_bytes, (jlong)stats->memtable_bytes,
        stats->is_flushing ? JNI_TRUE : JNI_FALSE, (jlong)stats->next_cf_index,
        (jlong)stats->wal_generation, (jlong)stats->flush_count, (jlong)stats->compaction_count,
        (jlong)stats->flush_bytes_written, (jlong)stats->compaction_bytes_written,
        (jlong)stats->compaction_bytes_read, (jlong)stats->wal_bytes_written,
        (jlong)stats->user_bytes_written, (jlong)stats->vlog_file_size,
        (jlong)stats->vlog_value_count, (jlong)stats->vlog_used_bytes,
        (jlong)stats->vlog_stored_bytes, (jlong)stats->vlog_live_bytes,
        (jlong)stats->vlog_segment_count, (jlong)stats->vlog_bytes_written,
        (jlong)stats->vlog_dead_bytes, (jlong)stats->vlog_reclaim_calls,
        (jlong)stats->vlog_reclaim_passes, (jlong)stats->vlog_segments_retired,
        (jlong)stats->vlog_segments_drainable, (jlong)stats->writes_throttled,
        (jlong)stats->writes_blocked, (jlong)stats->write_stall_us,
        (jlong)stats->write_stall_ceiling_hits);

    (*env)->DeleteLocalRef(env, cls);
    return result;
}

/* ===== com.tidesdb.Config ===== */

JNIEXPORT jobject JNICALL Java_com_tidesdb_Config_nativeDefaultConfig(JNIEnv *env, jclass cls)
{
    (void)cls;
    tidesdb_config_t config = tidesdb_default_config();
    return buildConfigObject(env, &config);
}

/* ===== com.tidesdb.ColumnFamilyConfig ===== */

JNIEXPORT jobject JNICALL Java_com_tidesdb_ColumnFamilyConfig_nativeDefaultConfig(JNIEnv *env,
                                                                                  jclass cls)
{
    (void)cls;
    tidesdb_column_family_config_t config = tidesdb_default_column_family_config();
    return buildCfConfigObject(env, &config);
}

/* ===== com.tidesdb.TidesDB : lifecycle ===== */

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeOpen(
    JNIEnv *env, jclass cls, jstring dbPath, jint numFlushThreads, jint numCompactionThreads,
    jint logLevel, jlong blockCacheSize, jlong maxOpenSSTables, jboolean logToFile,
    jlong logTruncationAt, jlong memtableWriteBufferSize, jint memtableSkipListMaxLevel,
    jfloat memtableSkipListProbability, jint memtableSyncMode, jlong memtableSyncIntervalUs,
    jlong valueSeparationThreshold, jlong vlogSegmentSize, jint memtableL0QueueStallThreshold,
    jint memtableIdleFlushSeconds, jlong txnTimeoutSeconds)
{
    (void)cls;

    const char *path = (*env)->GetStringUTFChars(env, dbPath, NULL);
    if (path == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the database path");
        }
        return 0;
    }

    tidesdb_config_t config;
    memset(&config, 0, sizeof(config));
    config.db_path = (char *)path;
    config.num_flush_threads = (int)numFlushThreads;
    config.num_compaction_threads = (int)numCompactionThreads;
    config.log_level = (tidesdb_log_level_t)logLevel;
    config.block_cache_size = (size_t)blockCacheSize;
    config.max_open_sstables = (size_t)maxOpenSSTables;
    config.log_to_file = logToFile ? 1 : 0;
    config.log_truncation_at = (size_t)logTruncationAt;
    config.memtable_write_buffer_size = (size_t)memtableWriteBufferSize;
    config.memtable_skip_list_max_level = (int)memtableSkipListMaxLevel;
    config.memtable_skip_list_probability = (float)memtableSkipListProbability;
    config.memtable_sync_mode = (int)memtableSyncMode;
    config.memtable_sync_interval_us = (uint64_t)memtableSyncIntervalUs;
    config.value_separation_threshold = (size_t)valueSeparationThreshold;
    config.vlog_segment_size = (size_t)vlogSegmentSize;
    config.memtable_l0_queue_stall_threshold = (int)memtableL0QueueStallThreshold;
    config.memtable_idle_flush_seconds = (int)memtableIdleFlushSeconds;
    config.txn_timeout_seconds = (int64_t)txnTimeoutSeconds;

    tidesdb_t *db = NULL;
    int result = tidesdb_open(&config, &db);

    (*env)->ReleaseStringUTFChars(env, dbPath, path);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }

    return (jlong)(uintptr_t)db;
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeClose(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    tidesdb_close((tidesdb_t *)(uintptr_t)handle);
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_TidesDB_nativeCompressionAvailable(JNIEnv *env,
                                                                               jclass cls,
                                                                               jint algorithm)
{
    (void)env;
    (void)cls;
    return tidesdb_compression_available((tidesdb_compression_algorithm_t)algorithm) ? JNI_TRUE
                                                                                    : JNI_FALSE;
}

JNIEXPORT jstring JNICALL Java_com_tidesdb_TidesDB_nativeStrerror(JNIEnv *env, jclass cls, jint code)
{
    (void)cls;
    return (*env)->NewStringUTF(env, tidesdb_strerror((int)code));
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeRaiseOpenFileLimit(JNIEnv *env, jclass cls,
                                                                          jlong desired)
{
    (void)env;
    (void)cls;
    return (jlong)tidesdb_raise_open_file_limit((long)desired);
}

/* ===== com.tidesdb.TidesDB : column families ===== */

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeCreateColumnFamily(
    JNIEnv *env, jclass cls, jlong handle, jstring name, jlong levelSizeRatio, jint minLevels,
    jint dividingLevelOffset, jboolean keepValuesInline, jlong btreeKlogBlockSize,
    jintArray encodingPipeline, jboolean enableBloomFilter, jdouble bloomFpr,
    jint defaultIsolationLevel, jint l1FileCountTrigger, jdouble tombstoneDensityTrigger,
    jlong tombstoneDensityMinEntries)
{
    (void)cls;

    tidesdb_column_family_config_t config;
    if (fillCfConfig(env, &config, levelSizeRatio, minLevels, dividingLevelOffset,
                     keepValuesInline, btreeKlogBlockSize, encodingPipeline, enableBloomFilter,
                     bloomFpr, defaultIsolationLevel, l1FileCountTrigger, tombstoneDensityTrigger,
                     tombstoneDensityMinEntries) != 0)
    {
        return;
    }

    const char *cfName = (*env)->GetStringUTFChars(env, name, NULL);
    if (cfName == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return;
    }

    int result = tidesdb_create_column_family((tidesdb_t *)(uintptr_t)handle, cfName, &config);
    (*env)->ReleaseStringUTFChars(env, name, cfName);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeDropColumnFamily(JNIEnv *env, jclass cls,
                                                                        jlong handle, jstring name)
{
    (void)cls;

    const char *cfName = (*env)->GetStringUTFChars(env, name, NULL);
    if (cfName == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return;
    }

    int result = tidesdb_drop_column_family((tidesdb_t *)(uintptr_t)handle, cfName);
    (*env)->ReleaseStringUTFChars(env, name, cfName);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeRenameColumnFamily(JNIEnv *env, jclass cls,
                                                                          jlong handle,
                                                                          jstring oldName,
                                                                          jstring newName)
{
    (void)cls;

    const char *from = (*env)->GetStringUTFChars(env, oldName, NULL);
    if (from == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return;
    }

    const char *to = (*env)->GetStringUTFChars(env, newName, NULL);
    if (to == NULL)
    {
        (*env)->ReleaseStringUTFChars(env, oldName, from);
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return;
    }

    int result = tidesdb_rename_column_family((tidesdb_t *)(uintptr_t)handle, from, to);

    (*env)->ReleaseStringUTFChars(env, newName, to);
    (*env)->ReleaseStringUTFChars(env, oldName, from);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeCloneColumnFamily(JNIEnv *env, jclass cls,
                                                                         jlong handle,
                                                                         jstring sourceName,
                                                                         jstring destName)
{
    (void)cls;

    const char *src = (*env)->GetStringUTFChars(env, sourceName, NULL);
    if (src == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return;
    }

    const char *dst = (*env)->GetStringUTFChars(env, destName, NULL);
    if (dst == NULL)
    {
        (*env)->ReleaseStringUTFChars(env, sourceName, src);
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return;
    }

    int result = tidesdb_clone_column_family((tidesdb_t *)(uintptr_t)handle, src, dst);

    (*env)->ReleaseStringUTFChars(env, destName, dst);
    (*env)->ReleaseStringUTFChars(env, sourceName, src);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeGetColumnFamily(JNIEnv *env, jclass cls,
                                                                        jlong handle, jstring name)
{
    (void)cls;

    const char *cfName = (*env)->GetStringUTFChars(env, name, NULL);
    if (cfName == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the column family name");
        }
        return 0;
    }

    tidesdb_column_family_t *cf =
        tidesdb_get_column_family((tidesdb_t *)(uintptr_t)handle, cfName);
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
    (void)cls;

    char **names = NULL;
    int count = 0;
    int result = tidesdb_list_column_families((tidesdb_t *)(uintptr_t)handle, &names, &count);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jclass stringClass = (*env)->FindClass(env, "java/lang/String");
    jobjectArray array = NULL;
    if (stringClass != NULL && count >= 0 && count <= JSIZE_MAX)
    {
        array = (*env)->NewObjectArray(env, (jsize)count, stringClass, NULL);
    }

    if (array != NULL)
    {
        for (int i = 0; i < count; i++)
        {
            jstring element = (*env)->NewStringUTF(env, names[i] != NULL ? names[i] : "");
            if (element == NULL)
            {
                array = NULL;
                break;
            }
            (*env)->SetObjectArrayElement(env, array, (jsize)i, element);
            (*env)->DeleteLocalRef(env, element);
        }
    }

    for (int i = 0; i < count; i++)
    {
        tidesdb_free(names[i]);
    }
    tidesdb_free(names);

    if (stringClass != NULL) (*env)->DeleteLocalRef(env, stringClass);
    return array;
}

/* ===== com.tidesdb.TidesDB : transactions ===== */

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransaction(JNIEnv *env, jclass cls,
                                                                         jlong handle)
{
    (void)cls;

    tidesdb_txn_t *txn = NULL;
    int result = tidesdb_txn_begin((tidesdb_t *)(uintptr_t)handle, &txn);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransactionWithIsolation(
    JNIEnv *env, jclass cls, jlong handle, jint isolationLevel)
{
    (void)cls;

    tidesdb_txn_t *txn = NULL;
    int result = tidesdb_txn_begin_with_isolation(
        (tidesdb_t *)(uintptr_t)handle, (tidesdb_isolation_level_t)isolationLevel, &txn);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransactionCf(JNIEnv *env, jclass cls,
                                                                          jlong handle,
                                                                          jlong cfHandle)
{
    (void)cls;

    tidesdb_txn_t *txn = NULL;
    int result = tidesdb_txn_begin_cf((tidesdb_t *)(uintptr_t)handle,
                                      (tidesdb_column_family_t *)(uintptr_t)cfHandle, &txn);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeSnapshotCreate(JNIEnv *env, jclass cls,
                                                                       jlong handle)
{
    (void)cls;

    tidesdb_snapshot_t *snapshot = NULL;
    int result = tidesdb_snapshot_create((tidesdb_t *)(uintptr_t)handle, &snapshot);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)snapshot;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransactionAtSnapshot(
    JNIEnv *env, jclass cls, jlong handle, jlong snapshotHandle)
{
    (void)cls;

    tidesdb_txn_t *txn = NULL;
    int result = tidesdb_txn_begin_at_snapshot((tidesdb_t *)(uintptr_t)handle,
                                               (tidesdb_snapshot_t *)(uintptr_t)snapshotHandle,
                                               &txn);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeBeginTransactionAtSeq(JNIEnv *env, jclass cls,
                                                                             jlong handle,
                                                                             jlong seq)
{
    (void)cls;

    tidesdb_txn_t *txn = NULL;
    int result = tidesdb_txn_begin_at_seq((tidesdb_t *)(uintptr_t)handle, (uint64_t)seq, &txn);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)txn;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_TidesDB_nativeOldestReadableSeq(JNIEnv *env, jclass cls,
                                                                         jlong handle)
{
    (void)env;
    (void)cls;
    return (jlong)tidesdb_oldest_readable_seq((const tidesdb_t *)(uintptr_t)handle);
}

JNIEXPORT jobjectArray JNICALL Java_com_tidesdb_TidesDB_nativeRecoverPrepared(JNIEnv *env,
                                                                              jclass cls,
                                                                              jlong handle)
{
    (void)cls;

    tidesdb_t *db = (tidesdb_t *)(uintptr_t)handle;

    /* the set is fixed when the database opens, so size it first and then fill it */
    int count = 0;
    int result = tidesdb_recover_prepared(db, NULL, 0, &count);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jclass preparedClass = (*env)->FindClass(env, "com/tidesdb/PreparedTransaction");
    if (preparedClass == NULL) return NULL;

    if (count <= 0)
    {
        jobjectArray empty = (*env)->NewObjectArray(env, 0, preparedClass, NULL);
        (*env)->DeleteLocalRef(env, preparedClass);
        return empty;
    }

    tidesdb_prepared_txn_t *entries =
        (tidesdb_prepared_txn_t *)calloc((size_t)count, sizeof(tidesdb_prepared_txn_t));
    if (entries == NULL)
    {
        (*env)->DeleteLocalRef(env, preparedClass);
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to allocate the recovery buffer");
        return NULL;
    }

    int filled = 0;
    result = tidesdb_recover_prepared(db, entries, count, &filled);
    if (result != TDB_SUCCESS)
    {
        free(entries);
        (*env)->DeleteLocalRef(env, preparedClass);
        throwResult(env, result);
        return NULL;
    }
    if (filled > count) filled = count;

    jclass txnClass = (*env)->FindClass(env, "com/tidesdb/Transaction");
    jmethodID txnCtor =
        txnClass != NULL ? (*env)->GetMethodID(env, txnClass, "<init>", "(J)V") : NULL;
    jmethodID preparedCtor =
        (*env)->GetMethodID(env, preparedClass, "<init>", "(Lcom/tidesdb/Transaction;[B)V");

    jobjectArray array = NULL;
    if (txnCtor != NULL && preparedCtor != NULL)
    {
        array = (*env)->NewObjectArray(env, (jsize)filled, preparedClass, NULL);
    }

    if (array != NULL)
    {
        for (int i = 0; i < filled; i++)
        {
            jobject txn =
                (*env)->NewObject(env, txnClass, txnCtor, (jlong)(uintptr_t)entries[i].txn);
            if (txn == NULL)
            {
                array = NULL;
                break;
            }

            jsize xidSize = entries[i].xid_size > (size_t)JSIZE_MAX ? JSIZE_MAX
                                                                    : (jsize)entries[i].xid_size;
            jbyteArray xid = (*env)->NewByteArray(env, xidSize);
            if (xid == NULL)
            {
                (*env)->DeleteLocalRef(env, txn);
                array = NULL;
                break;
            }
            if (xidSize > 0)
            {
                (*env)->SetByteArrayRegion(env, xid, 0, xidSize, (const jbyte *)entries[i].xid);
            }

            jobject prepared = (*env)->NewObject(env, preparedClass, preparedCtor, txn, xid);
            if (prepared == NULL)
            {
                (*env)->DeleteLocalRef(env, xid);
                (*env)->DeleteLocalRef(env, txn);
                array = NULL;
                break;
            }

            (*env)->SetObjectArrayElement(env, array, (jsize)i, prepared);
            (*env)->DeleteLocalRef(env, prepared);
            (*env)->DeleteLocalRef(env, xid);
            (*env)->DeleteLocalRef(env, txn);
        }
    }

    free(entries);
    if (txnClass != NULL) (*env)->DeleteLocalRef(env, txnClass);
    (*env)->DeleteLocalRef(env, preparedClass);
    return array;
}

/* ===== com.tidesdb.TidesDB : maintenance ===== */

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeFlushMemtable(JNIEnv *env, jclass cls,
                                                                     jlong handle)
{
    (void)cls;
    int result = tidesdb_flush_memtable((tidesdb_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_TidesDB_nativeIsFlushing(JNIEnv *env, jclass cls,
                                                                      jlong handle)
{
    (void)env;
    (void)cls;
    return tidesdb_is_flushing((tidesdb_t *)(uintptr_t)handle) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeSyncWal(JNIEnv *env, jclass cls, jlong handle)
{
    (void)cls;
    int result = tidesdb_sync_wal((tidesdb_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeBackup(JNIEnv *env, jclass cls, jlong handle,
                                                              jstring dir)
{
    (void)cls;

    const char *path = (*env)->GetStringUTFChars(env, dir, NULL);
    if (path == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the backup directory");
        }
        return;
    }

    int result = tidesdb_backup((tidesdb_t *)(uintptr_t)handle, path);
    (*env)->ReleaseStringUTFChars(env, dir, path);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDB_nativeCheckpoint(JNIEnv *env, jclass cls,
                                                                  jlong handle)
{
    (void)cls;
    int result = tidesdb_checkpoint((tidesdb_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

/* ===== com.tidesdb.TidesDB : statistics ===== */

JNIEXPORT jobject JNICALL Java_com_tidesdb_TidesDB_nativeGetDbStats(JNIEnv *env, jclass cls,
                                                                     jlong handle)
{
    (void)cls;

    tidesdb_db_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    int result = tidesdb_get_db_stats((tidesdb_t *)(uintptr_t)handle, &stats);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }
    return buildDbStatsObject(env, &stats);
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_TidesDB_nativeGetCacheStats(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    (void)cls;

    tidesdb_cache_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    int result = tidesdb_get_cache_stats((tidesdb_t *)(uintptr_t)handle, &stats);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jclass cls_ = (*env)->FindClass(env, "com/tidesdb/CacheStats");
    if (cls_ == NULL) return NULL;

    jmethodID ctor = (*env)->GetMethodID(env, cls_, "<init>", "(ZJJJJDJ)V");
    if (ctor == NULL)
    {
        (*env)->DeleteLocalRef(env, cls_);
        return NULL;
    }

    jobject result_obj = (*env)->NewObject(
        env, cls_, ctor, stats.enabled ? JNI_TRUE : JNI_FALSE, (jlong)stats.total_entries,
        (jlong)stats.total_bytes, (jlong)stats.hits, (jlong)stats.misses, (jdouble)stats.hit_rate,
        (jlong)stats.num_partitions);

    (*env)->DeleteLocalRef(env, cls_);
    return result_obj;
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_TidesDB_nativeGetStallStats(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    (void)cls;

    tidesdb_stall_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    int result = tidesdb_get_stall_stats((tidesdb_t *)(uintptr_t)handle, &stats);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jclass statClass = (*env)->FindClass(env, "com/tidesdb/StallStat");
    if (statClass == NULL) return NULL;

    jmethodID statCtor = (*env)->GetMethodID(env, statClass, "<init>", "(JJJ)V");
    jobjectArray array =
        statCtor != NULL ? (*env)->NewObjectArray(env, TDB_STALL_COUNT, statClass, NULL) : NULL;

    if (array != NULL)
    {
        for (int i = 0; i < TDB_STALL_COUNT; i++)
        {
            jobject stat = (*env)->NewObject(env, statClass, statCtor,
                                             (jlong)stats.reasons[i].count,
                                             (jlong)stats.reasons[i].total_us,
                                             (jlong)stats.reasons[i].max_us);
            if (stat == NULL)
            {
                array = NULL;
                break;
            }
            (*env)->SetObjectArrayElement(env, array, (jsize)i, stat);
            (*env)->DeleteLocalRef(env, stat);
        }
    }
    (*env)->DeleteLocalRef(env, statClass);
    if (array == NULL) return NULL;

    jclass statsClass = (*env)->FindClass(env, "com/tidesdb/StallStats");
    if (statsClass == NULL) return NULL;

    jmethodID statsCtor =
        (*env)->GetMethodID(env, statsClass, "<init>", "([Lcom/tidesdb/StallStat;)V");
    jobject result_obj =
        statsCtor != NULL ? (*env)->NewObject(env, statsClass, statsCtor, array) : NULL;

    (*env)->DeleteLocalRef(env, statsClass);
    return result_obj;
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_TidesDB_nativeGetIoStats(JNIEnv *env, jclass cls,
                                                                     jlong handle)
{
    (void)cls;

    tidesdb_io_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    int result = tidesdb_get_io_stats((tidesdb_t *)(uintptr_t)handle, &stats);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jclass statClass = (*env)->FindClass(env, "com/tidesdb/IoStat");
    if (statClass == NULL) return NULL;

    jmethodID statCtor = (*env)->GetMethodID(env, statClass, "<init>", "(JJJJ)V");
    jobjectArray array =
        statCtor != NULL ? (*env)->NewObjectArray(env, TDB_IO_COUNT, statClass, NULL) : NULL;

    if (array != NULL)
    {
        for (int i = 0; i < TDB_IO_COUNT; i++)
        {
            jobject stat = (*env)->NewObject(env, statClass, statCtor, (jlong)stats.classes[i].ops,
                                             (jlong)stats.classes[i].bytes,
                                             (jlong)stats.classes[i].total_us,
                                             (jlong)stats.classes[i].max_us);
            if (stat == NULL)
            {
                array = NULL;
                break;
            }
            (*env)->SetObjectArrayElement(env, array, (jsize)i, stat);
            (*env)->DeleteLocalRef(env, stat);
        }
    }
    (*env)->DeleteLocalRef(env, statClass);
    if (array == NULL) return NULL;

    jclass statsClass = (*env)->FindClass(env, "com/tidesdb/IoStats");
    if (statsClass == NULL) return NULL;

    jmethodID statsCtor = (*env)->GetMethodID(env, statsClass, "<init>", "([Lcom/tidesdb/IoStat;)V");
    jobject result_obj =
        statsCtor != NULL ? (*env)->NewObject(env, statsClass, statsCtor, array) : NULL;

    (*env)->DeleteLocalRef(env, statsClass);
    return result_obj;
}

/**
 * Shared body for the key-log and value-log encoding stats calls, which differ
 * only in which collector they run.
 */
static jobjectArray buildEncodingStatsArray(JNIEnv *env, tidesdb_encoding_stats_t *entries,
                                            size_t count)
{
    jclass cls = (*env)->FindClass(env, "com/tidesdb/EncodingStats");
    if (cls == NULL) return NULL;

    jmethodID ctor = (*env)->GetMethodID(env, cls, "<init>", "([IJJJ)V");
    jobjectArray array = NULL;
    if (ctor != NULL && count <= (size_t)JSIZE_MAX)
    {
        array = (*env)->NewObjectArray(env, (jsize)count, cls, NULL);
    }

    if (array != NULL)
    {
        for (size_t i = 0; i < count; i++)
        {
            int idCount = entries[i].id_count;
            if (idCount < 0) idCount = 0;
            if (idCount > TDB_ENCODING_PIPELINE_MAX) idCount = TDB_ENCODING_PIPELINE_MAX;

            jintArray ids = newIntArrayFromBytes(env, entries[i].ids, (jsize)idCount);
            if (ids == NULL)
            {
                array = NULL;
                break;
            }

            jobject stat =
                (*env)->NewObject(env, cls, ctor, ids, (jlong)entries[i].logical_bytes,
                                  (jlong)entries[i].stored_bytes, (jlong)entries[i].item_count);
            if (stat == NULL)
            {
                (*env)->DeleteLocalRef(env, ids);
                array = NULL;
                break;
            }

            (*env)->SetObjectArrayElement(env, array, (jsize)i, stat);
            (*env)->DeleteLocalRef(env, stat);
            (*env)->DeleteLocalRef(env, ids);
        }
    }

    (*env)->DeleteLocalRef(env, cls);
    return array;
}

JNIEXPORT jobjectArray JNICALL Java_com_tidesdb_TidesDB_nativeGetKlogEncodingStats(JNIEnv *env,
                                                                                   jclass cls,
                                                                                   jlong handle)
{
    (void)cls;

    tidesdb_encoding_stats_t entries[JNI_MAX_ENCODING_CHAINS];
    memset(entries, 0, sizeof(entries));
    size_t count = 0;
    int result = tidesdb_get_klog_encoding_stats((tidesdb_t *)(uintptr_t)handle, entries,
                                                 JNI_MAX_ENCODING_CHAINS, &count);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }
    if (count > JNI_MAX_ENCODING_CHAINS) count = JNI_MAX_ENCODING_CHAINS;
    return buildEncodingStatsArray(env, entries, count);
}

JNIEXPORT jobjectArray JNICALL Java_com_tidesdb_TidesDB_nativeGetVlogEncodingStats(JNIEnv *env,
                                                                                   jclass cls,
                                                                                   jlong handle)
{
    (void)cls;

    tidesdb_encoding_stats_t entries[JNI_MAX_ENCODING_CHAINS];
    memset(entries, 0, sizeof(entries));
    size_t count = 0;
    int result = tidesdb_get_vlog_encoding_stats((tidesdb_t *)(uintptr_t)handle, entries,
                                                 JNI_MAX_ENCODING_CHAINS, &count);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }
    if (count > JNI_MAX_ENCODING_CHAINS) count = JNI_MAX_ENCODING_CHAINS;
    return buildEncodingStatsArray(env, entries, count);
}

/* ===== com.tidesdb.ColumnFamily ===== */

JNIEXPORT jobject JNICALL Java_com_tidesdb_ColumnFamily_nativeGetStats(JNIEnv *env, jclass cls,
                                                                        jlong cfHandle)
{
    (void)cls;

    tidesdb_cf_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    int result = tidesdb_get_cf_stats((tidesdb_column_family_t *)(uintptr_t)cfHandle, &stats);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }
    return buildCfStatsObject(env, &stats);
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_ColumnFamily_nativeEstimateCardinality(JNIEnv *env,
                                                                                jclass cls,
                                                                                jlong cfHandle)
{
    (void)cls;

    uint64_t estimate = 0;
    int result = tidesdb_cf_estimate_cardinality((tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                                 &estimate);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)estimate;
}

JNIEXPORT void JNICALL Java_com_tidesdb_ColumnFamily_nativeCompact(JNIEnv *env, jclass cls,
                                                                    jlong dbHandle, jlong cfHandle)
{
    (void)cls;
    int result = tidesdb_compact((tidesdb_t *)(uintptr_t)dbHandle,
                                 (tidesdb_column_family_t *)(uintptr_t)cfHandle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_ColumnFamily_nativeCompactRange(JNIEnv *env, jclass cls,
                                                                         jlong dbHandle,
                                                                         jlong cfHandle,
                                                                         jbyteArray startKey,
                                                                         jbyteArray endKey)
{
    (void)cls;

    jni_bytes_t start;
    jni_bytes_t end;
    if (acquireBytes(env, startKey, &start) != 0) return;
    if (acquireBytes(env, endKey, &end) != 0)
    {
        releaseBytes(env, &start);
        return;
    }

    int result = tidesdb_compact_range(
        (tidesdb_t *)(uintptr_t)dbHandle, (tidesdb_column_family_t *)(uintptr_t)cfHandle,
        (const uint8_t *)start.data, (size_t)start.length, (const uint8_t *)end.data,
        (size_t)end.length);

    releaseBytes(env, &end);
    releaseBytes(env, &start);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_ColumnFamily_nativeIsCompacting(JNIEnv *env, jclass cls,
                                                                            jlong cfHandle)
{
    (void)env;
    (void)cls;
    return tidesdb_is_compacting((tidesdb_column_family_t *)(uintptr_t)cfHandle) ? JNI_TRUE
                                                                                : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_com_tidesdb_ColumnFamily_nativeUpdateRuntimeConfig(
    JNIEnv *env, jclass cls, jlong dbHandle, jlong cfHandle, jlong levelSizeRatio, jint minLevels,
    jint dividingLevelOffset, jboolean keepValuesInline, jlong btreeKlogBlockSize,
    jintArray encodingPipeline, jboolean enableBloomFilter, jdouble bloomFpr,
    jint defaultIsolationLevel, jint l1FileCountTrigger, jdouble tombstoneDensityTrigger,
    jlong tombstoneDensityMinEntries, jboolean persistToDisk)
{
    (void)cls;

    tidesdb_column_family_config_t config;
    if (fillCfConfig(env, &config, levelSizeRatio, minLevels, dividingLevelOffset,
                     keepValuesInline, btreeKlogBlockSize, encodingPipeline, enableBloomFilter,
                     bloomFpr, defaultIsolationLevel, l1FileCountTrigger, tombstoneDensityTrigger,
                     tombstoneDensityMinEntries) != 0)
    {
        return;
    }

    int result = tidesdb_cf_update_runtime_config(
        (tidesdb_t *)(uintptr_t)dbHandle, (tidesdb_column_family_t *)(uintptr_t)cfHandle, &config,
        persistToDisk ? 1 : 0);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_ColumnFamily_nativeRangeStats(JNIEnv *env, jclass cls,
                                                                          jlong dbHandle,
                                                                          jlong cfHandle,
                                                                          jbyteArray keyA,
                                                                          jbyteArray keyB)
{
    (void)cls;

    jni_bytes_t a;
    jni_bytes_t b;
    if (acquireBytes(env, keyA, &a) != 0) return NULL;
    if (acquireBytes(env, keyB, &b) != 0)
    {
        releaseBytes(env, &a);
        return NULL;
    }

    tidesdb_range_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    int result = tidesdb_range_stats(
        (tidesdb_t *)(uintptr_t)dbHandle, (tidesdb_column_family_t *)(uintptr_t)cfHandle,
        (const uint8_t *)a.data, (size_t)a.length, (const uint8_t *)b.data, (size_t)b.length,
        &stats);

    releaseBytes(env, &b);
    releaseBytes(env, &a);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jclass statsClass = (*env)->FindClass(env, "com/tidesdb/RangeStats");
    if (statsClass == NULL) return NULL;

    jmethodID ctor = (*env)->GetMethodID(env, statsClass, "<init>", "(JJZ)V");
    jobject result_obj = NULL;
    if (ctor != NULL)
    {
        result_obj = (*env)->NewObject(env, statsClass, ctor, (jlong)stats.sstables_overlapping,
                                       (jlong)stats.estimated_keys,
                                       stats.keys_exact ? JNI_TRUE : JNI_FALSE);
    }

    (*env)->DeleteLocalRef(env, statsClass);
    return result_obj;
}

/* ===== commit hooks ===== */

/**
 * Context stored as the commit hook ctx pointer. Holds the JavaVM and a global
 * reference to the Java CommitHook object, with reference-counted quiescent
 * retirement so a hook being replaced cannot be freed under a callback that is
 * still inside it.
 */
typedef struct
{
    JavaVM *jvm;
    jobject hook_obj; /* global reference to CommitHook */
    int refcount;     /* callbacks currently inside the trampoline */
    int retired;      /* 0 = active, 1 = retired (do not enter) */
    pthread_mutex_t lock;
    pthread_cond_t zero_cond;
} java_hook_ctx_t;

/**
 * Retires and destroys a hook context, waiting for in-flight callbacks to drain
 * before deleting the global reference. Must only be called once the context has
 * been detached from tidesdb, so no new callback can arrive.
 */
static void retire_and_destroy_hook_ctx(JNIEnv *env, java_hook_ctx_t *ctx)
{
    if (ctx == NULL) return;

    pthread_mutex_lock(&ctx->lock);
    ctx->retired = 1;
    while (ctx->refcount > 0)
    {
        pthread_cond_wait(&ctx->zero_cond, &ctx->lock);
    }
    pthread_mutex_unlock(&ctx->lock);

    (*env)->DeleteGlobalRef(env, ctx->hook_obj);
    pthread_mutex_destroy(&ctx->lock);
    pthread_cond_destroy(&ctx->zero_cond);
    free(ctx);
}

/** Drops a trampoline's claim on the context and wakes a waiting retirement. */
static void release_hook_ctx(java_hook_ctx_t *ctx)
{
    pthread_mutex_lock(&ctx->lock);
    ctx->refcount--;
    if (ctx->refcount == 0 && ctx->retired) pthread_cond_signal(&ctx->zero_cond);
    pthread_mutex_unlock(&ctx->lock);
}

/**
 * Bridges tidesdb_commit_hook_fn to CommitHook.onCommit. Fires synchronously on
 * the committing thread, which is normally a Java thread but is attached here
 * anyway so an engine-internal caller is also safe.
 */
static int java_commit_hook_trampoline(const tidesdb_commit_op_t *ops, int num_ops,
                                       uint64_t commit_seq, void *ctx)
{
    java_hook_ctx_t *hctx = (java_hook_ctx_t *)ctx;
    JNIEnv *env = NULL;
    int need_detach = 0;

    pthread_mutex_lock(&hctx->lock);
    if (hctx->retired)
    {
        pthread_mutex_unlock(&hctx->lock);
        return -1;
    }
    hctx->refcount++;
    pthread_mutex_unlock(&hctx->lock);

    jint rc = (*hctx->jvm)->GetEnv(hctx->jvm, (void **)&env, JNI_VERSION_1_6);
    if (rc == JNI_EDETACHED)
    {
        if ((*hctx->jvm)->AttachCurrentThread(hctx->jvm, (void **)&env, NULL) != 0)
        {
            release_hook_ctx(hctx);
            return -1;
        }
        need_detach = 1;
    }
    else if (rc != JNI_OK)
    {
        release_hook_ctx(hctx);
        return -1;
    }

    jint ret = -1;
    jclass commitOpClass = NULL;
    jobjectArray opsArray = NULL;
    jclass hookClass = NULL;

    commitOpClass = (*env)->FindClass(env, "com/tidesdb/CommitOp");
    if (commitOpClass == NULL) goto cleanup;

    jmethodID opCtor = (*env)->GetMethodID(env, commitOpClass, "<init>", "([B[BJZ)V");
    if (opCtor == NULL) goto cleanup;

    if (num_ops < 0) num_ops = 0;
    opsArray = (*env)->NewObjectArray(env, (jsize)num_ops, commitOpClass, NULL);
    if (opsArray == NULL) goto cleanup;

    for (int i = 0; i < num_ops; i++)
    {
        jbyteArray jkey = (*env)->NewByteArray(env, (jsize)ops[i].key_size);
        if (jkey == NULL) goto cleanup;
        (*env)->SetByteArrayRegion(env, jkey, 0, (jsize)ops[i].key_size,
                                   (const jbyte *)ops[i].key);

        jbyteArray jvalue = NULL;
        if (ops[i].value != NULL)
        {
            jvalue = (*env)->NewByteArray(env, (jsize)ops[i].value_size);
            if (jvalue == NULL)
            {
                (*env)->DeleteLocalRef(env, jkey);
                goto cleanup;
            }
            if (ops[i].value_size > 0)
            {
                (*env)->SetByteArrayRegion(env, jvalue, 0, (jsize)ops[i].value_size,
                                           (const jbyte *)ops[i].value);
            }
        }

        jobject opObj = (*env)->NewObject(env, commitOpClass, opCtor, jkey, jvalue,
                                          (jlong)ops[i].ttl,
                                          ops[i].is_delete ? JNI_TRUE : JNI_FALSE);
        if (opObj == NULL)
        {
            if (jvalue != NULL) (*env)->DeleteLocalRef(env, jvalue);
            (*env)->DeleteLocalRef(env, jkey);
            goto cleanup;
        }

        (*env)->SetObjectArrayElement(env, opsArray, i, opObj);
        (*env)->DeleteLocalRef(env, opObj);
        if (jvalue != NULL) (*env)->DeleteLocalRef(env, jvalue);
        (*env)->DeleteLocalRef(env, jkey);
    }

    hookClass = (*env)->GetObjectClass(env, hctx->hook_obj);
    if (hookClass == NULL) goto cleanup;

    jmethodID onCommit =
        (*env)->GetMethodID(env, hookClass, "onCommit", "([Lcom/tidesdb/CommitOp;J)I");
    if (onCommit == NULL) goto cleanup;

    ret = (*env)->CallIntMethod(env, hctx->hook_obj, onCommit, opsArray, (jlong)commit_seq);

    if ((*env)->ExceptionCheck(env))
    {
        (*env)->ExceptionClear(env);
        ret = -1;
    }

cleanup:
    if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
    if (hookClass != NULL) (*env)->DeleteLocalRef(env, hookClass);
    if (opsArray != NULL) (*env)->DeleteLocalRef(env, opsArray);
    if (commitOpClass != NULL) (*env)->DeleteLocalRef(env, commitOpClass);
    if (need_detach) (*hctx->jvm)->DetachCurrentThread(hctx->jvm);

    release_hook_ctx(hctx);
    return (int)ret;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_ColumnFamily_nativeSetCommitHook(JNIEnv *env, jclass cls,
                                                                           jlong dbHandle,
                                                                           jlong cfHandle,
                                                                           jobject hook,
                                                                           jlong oldCtxHandle)
{
    (void)cls;

    tidesdb_t *db = (tidesdb_t *)(uintptr_t)dbHandle;
    tidesdb_column_family_t *cf = (tidesdb_column_family_t *)(uintptr_t)cfHandle;

    /* a NULL hook clears the callback */
    if (hook == NULL)
    {
        int result = tidesdb_cf_set_commit_hook(db, cf, NULL, NULL);
        if (result != TDB_SUCCESS)
        {
            throwResult(env, result);
            return oldCtxHandle; /* the old context stays active */
        }

        if (oldCtxHandle != 0)
        {
            retire_and_destroy_hook_ctx(env, (java_hook_ctx_t *)(uintptr_t)oldCtxHandle);
        }
        return 0;
    }

    java_hook_ctx_t *new_ctx = (java_hook_ctx_t *)malloc(sizeof(java_hook_ctx_t));
    if (new_ctx == NULL)
    {
        throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to allocate the commit hook context");
        return oldCtxHandle;
    }

    new_ctx->refcount = 0;
    new_ctx->retired = 0;
    new_ctx->hook_obj = NULL;
    pthread_mutex_init(&new_ctx->lock, NULL);
    pthread_cond_init(&new_ctx->zero_cond, NULL);

    if ((*env)->GetJavaVM(env, &new_ctx->jvm) != 0)
    {
        pthread_mutex_destroy(&new_ctx->lock);
        pthread_cond_destroy(&new_ctx->zero_cond);
        free(new_ctx);
        throwTidesDBException(env, TDB_ERR_UNKNOWN, "Failed to reach the JavaVM");
        return oldCtxHandle;
    }

    new_ctx->hook_obj = (*env)->NewGlobalRef(env, hook);
    if (new_ctx->hook_obj == NULL)
    {
        if (jvm_exception_pending(env)) (*env)->ExceptionClear(env);
        pthread_mutex_destroy(&new_ctx->lock);
        pthread_cond_destroy(&new_ctx->zero_cond);
        free(new_ctx);
        throwTidesDBException(env, TDB_ERR_MEMORY,
                              "Failed to create a global reference for the commit hook");
        return oldCtxHandle;
    }

    int result = tidesdb_cf_set_commit_hook(db, cf, java_commit_hook_trampoline, new_ctx);
    if (result != TDB_SUCCESS)
    {
        (*env)->DeleteGlobalRef(env, new_ctx->hook_obj);
        pthread_mutex_destroy(&new_ctx->lock);
        pthread_cond_destroy(&new_ctx->zero_cond);
        free(new_ctx);
        throwResult(env, result);
        return oldCtxHandle; /* the old context stays active */
    }

    /* the engine now holds the new context, so the old one can no longer be entered */
    if (oldCtxHandle != 0)
    {
        retire_and_destroy_hook_ctx(env, (java_hook_ctx_t *)(uintptr_t)oldCtxHandle);
    }

    return (jlong)(uintptr_t)new_ctx;
}

/* ===== com.tidesdb.Transaction ===== */

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativePut(JNIEnv *env, jclass cls, jlong handle,
                                                               jlong cfHandle, jbyteArray key,
                                                               jbyteArray value, jlong ttlSeconds)
{
    (void)cls;

    jni_bytes_t k;
    jni_bytes_t v;
    if (acquireBytes(env, key, &k) != 0) return;
    if (acquireBytes(env, value, &v) != 0)
    {
        releaseBytes(env, &k);
        return;
    }

    int result = tidesdb_txn_put((tidesdb_txn_t *)(uintptr_t)handle,
                                 (tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                 (const uint8_t *)k.data, (size_t)k.length,
                                 (const uint8_t *)v.data, (size_t)v.length, (time_t)ttlSeconds);

    releaseBytes(env, &v);
    releaseBytes(env, &k);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

/** Shared body for the tracking and non-tracking reads, which differ only in the call. */
static jbyteArray transactionRead(JNIEnv *env, jlong handle, jlong cfHandle, jbyteArray key,
                                  int track)
{
    jni_bytes_t k;
    if (acquireBytes(env, key, &k) != 0) return NULL;

    uint8_t *value = NULL;
    size_t valueSize = 0;
    int result;
    if (track)
    {
        result = tidesdb_txn_get((tidesdb_txn_t *)(uintptr_t)handle,
                                 (tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                 (const uint8_t *)k.data, (size_t)k.length, &value, &valueSize);
    }
    else
    {
        result = tidesdb_txn_get_notrack(
            (tidesdb_txn_t *)(uintptr_t)handle, (tidesdb_column_family_t *)(uintptr_t)cfHandle,
            (const uint8_t *)k.data, (size_t)k.length, &value, &valueSize);
    }

    releaseBytes(env, &k);

    if (result == TDB_ERR_NOT_FOUND)
    {
        return NULL; /* absence is reported as a null return, not an exception */
    }
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    return toByteArrayAndFree(env, value, valueSize);
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_Transaction_nativeGet(JNIEnv *env, jclass cls,
                                                                     jlong handle, jlong cfHandle,
                                                                     jbyteArray key)
{
    (void)cls;
    return transactionRead(env, handle, cfHandle, key, 1);
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_Transaction_nativeGetNoTrack(JNIEnv *env, jclass cls,
                                                                            jlong handle,
                                                                            jlong cfHandle,
                                                                            jbyteArray key)
{
    (void)cls;
    return transactionRead(env, handle, cfHandle, key, 0);
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_Transaction_nativeContains(JNIEnv *env, jclass cls,
                                                                        jlong handle,
                                                                        jlong cfHandle,
                                                                        jbyteArray key)
{
    (void)cls;

    jni_bytes_t k;
    if (acquireBytes(env, key, &k) != 0) return JNI_FALSE;

    int result = tidesdb_txn_contains((tidesdb_txn_t *)(uintptr_t)handle,
                                      (tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                      (const uint8_t *)k.data, (size_t)k.length);

    releaseBytes(env, &k);

    if (result == TDB_SUCCESS) return JNI_TRUE;
    if (result == TDB_ERR_NOT_FOUND) return JNI_FALSE;

    throwResult(env, result);
    return JNI_FALSE;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_Transaction_nativeReadSnapshot(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    (void)env;
    (void)cls;
    return (jlong)tidesdb_txn_read_snapshot((const tidesdb_txn_t *)(uintptr_t)handle);
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeDelete(JNIEnv *env, jclass cls,
                                                                  jlong handle, jlong cfHandle,
                                                                  jbyteArray key)
{
    (void)cls;

    jni_bytes_t k;
    if (acquireBytes(env, key, &k) != 0) return;

    int result = tidesdb_txn_delete((tidesdb_txn_t *)(uintptr_t)handle,
                                    (tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                    (const uint8_t *)k.data, (size_t)k.length);

    releaseBytes(env, &k);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeSingleDelete(JNIEnv *env, jclass cls,
                                                                        jlong handle,
                                                                        jlong cfHandle,
                                                                        jbyteArray key)
{
    (void)cls;

    jni_bytes_t k;
    if (acquireBytes(env, key, &k) != 0) return;

    int result = tidesdb_txn_single_delete((tidesdb_txn_t *)(uintptr_t)handle,
                                           (tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                           (const uint8_t *)k.data, (size_t)k.length);

    releaseBytes(env, &k);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeDeleteRange(JNIEnv *env, jclass cls,
                                                                       jlong handle, jlong cfHandle,
                                                                       jbyteArray lo, jbyteArray hi)
{
    (void)cls;

    jni_bytes_t lower;
    jni_bytes_t upper;
    if (acquireBytes(env, lo, &lower) != 0) return;
    if (acquireBytes(env, hi, &upper) != 0)
    {
        releaseBytes(env, &lower);
        return;
    }

    int result = tidesdb_txn_delete_range(
        (tidesdb_txn_t *)(uintptr_t)handle, (tidesdb_column_family_t *)(uintptr_t)cfHandle,
        (const uint8_t *)lower.data, (size_t)lower.length, (const uint8_t *)upper.data,
        (size_t)upper.length);

    releaseBytes(env, &upper);
    releaseBytes(env, &lower);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeDeletePrefix(JNIEnv *env, jclass cls,
                                                                        jlong handle,
                                                                        jlong cfHandle,
                                                                        jbyteArray prefix)
{
    (void)cls;

    jni_bytes_t p;
    if (acquireBytes(env, prefix, &p) != 0) return;

    int result = tidesdb_txn_delete_prefix((tidesdb_txn_t *)(uintptr_t)handle,
                                           (tidesdb_column_family_t *)(uintptr_t)cfHandle,
                                           (const uint8_t *)p.data, (size_t)p.length);

    releaseBytes(env, &p);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_Transaction_nativeNewIterator(JNIEnv *env, jclass cls,
                                                                        jlong handle,
                                                                        jlong cfHandle)
{
    (void)cls;

    tidesdb_iter_t *iter = NULL;
    int result = tidesdb_iter_new((tidesdb_txn_t *)(uintptr_t)handle,
                                  (tidesdb_column_family_t *)(uintptr_t)cfHandle, &iter);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)iter;
}

JNIEXPORT jlong JNICALL Java_com_tidesdb_Transaction_nativeNewRangeIterator(JNIEnv *env, jclass cls,
                                                                             jlong handle,
                                                                             jlong cfHandle,
                                                                             jbyteArray lower,
                                                                             jbyteArray upper)
{
    (void)cls;

    jni_bytes_t lo;
    jni_bytes_t hi;
    if (acquireBytes(env, lower, &lo) != 0) return 0;
    if (acquireBytes(env, upper, &hi) != 0)
    {
        releaseBytes(env, &lo);
        return 0;
    }

    tidesdb_iter_t *iter = NULL;
    int result = tidesdb_iter_new_range(
        (tidesdb_txn_t *)(uintptr_t)handle, (tidesdb_column_family_t *)(uintptr_t)cfHandle,
        (const uint8_t *)lo.data, (size_t)lo.length, (const uint8_t *)hi.data, (size_t)hi.length,
        &iter);

    releaseBytes(env, &hi);
    releaseBytes(env, &lo);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jlong)(uintptr_t)iter;
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeCommit(JNIEnv *env, jclass cls,
                                                                  jlong handle)
{
    (void)cls;
    int result = tidesdb_txn_commit((tidesdb_txn_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeRollback(JNIEnv *env, jclass cls,
                                                                    jlong handle)
{
    (void)cls;
    int result = tidesdb_txn_rollback((tidesdb_txn_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeSetTimeout(JNIEnv *env, jclass cls,
                                                                      jlong handle, jlong seconds)
{
    (void)cls;
    int result = tidesdb_txn_set_timeout((tidesdb_txn_t *)(uintptr_t)handle, (int64_t)seconds);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeRequestAbort(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    (void)env;
    (void)cls;
    tidesdb_txn_request_abort((tidesdb_txn_t *)(uintptr_t)handle);
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeReset(JNIEnv *env, jclass cls,
                                                                 jlong handle, jint isolationLevel)
{
    (void)cls;
    int result = tidesdb_txn_reset((tidesdb_txn_t *)(uintptr_t)handle,
                                   (tidesdb_isolation_level_t)isolationLevel);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT jint JNICALL Java_com_tidesdb_Transaction_nativeState(JNIEnv *env, jclass cls,
                                                                 jlong handle)
{
    (void)cls;

    tidesdb_txn_state_t state = TDB_TXN_STATE_ACTIVE;
    int result = tidesdb_txn_state((const tidesdb_txn_t *)(uintptr_t)handle, &state);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return 0;
    }
    return (jint)state;
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativePrepare(JNIEnv *env, jclass cls,
                                                                   jlong handle, jbyteArray xid)
{
    (void)cls;

    jni_bytes_t x;
    if (acquireBytes(env, xid, &x) != 0) return;

    int result = tidesdb_txn_prepare((tidesdb_txn_t *)(uintptr_t)handle, (const uint8_t *)x.data,
                                     (size_t)x.length);

    releaseBytes(env, &x);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeCommitPrepared(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    (void)cls;
    int result = tidesdb_txn_commit_prepared((tidesdb_txn_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeRollbackPrepared(JNIEnv *env, jclass cls,
                                                                            jlong handle)
{
    (void)cls;
    int result = tidesdb_txn_rollback_prepared((tidesdb_txn_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

/** Shared body for the three savepoint calls, which differ only in the operation. */
static void savepointCall(JNIEnv *env, jlong handle, jstring name,
                          int (*op)(tidesdb_txn_t *, const char *))
{
    const char *spName = (*env)->GetStringUTFChars(env, name, NULL);
    if (spName == NULL)
    {
        if (!jvm_exception_pending(env))
        {
            throwTidesDBException(env, TDB_ERR_MEMORY, "Failed to read the savepoint name");
        }
        return;
    }

    int result = op((tidesdb_txn_t *)(uintptr_t)handle, spName);
    (*env)->ReleaseStringUTFChars(env, name, spName);

    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeSavepoint(JNIEnv *env, jclass cls,
                                                                     jlong handle, jstring name)
{
    (void)cls;
    savepointCall(env, handle, name, tidesdb_txn_savepoint);
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeRollbackToSavepoint(JNIEnv *env,
                                                                               jclass cls,
                                                                               jlong handle,
                                                                               jstring name)
{
    (void)cls;
    savepointCall(env, handle, name, tidesdb_txn_rollback_to_savepoint);
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeReleaseSavepoint(JNIEnv *env, jclass cls,
                                                                            jlong handle,
                                                                            jstring name)
{
    (void)cls;
    savepointCall(env, handle, name, tidesdb_txn_release_savepoint);
}

JNIEXPORT void JNICALL Java_com_tidesdb_Transaction_nativeFree(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    tidesdb_txn_free((tidesdb_txn_t *)(uintptr_t)handle);
}

/* ===== com.tidesdb.TidesDBIterator =====
 *
 * every positioning call reports TDB_ERR_NOT_FOUND when the merged stream has no
 * entry where it was asked to stand. that is the end of the range rather than a
 * failure, so it is swallowed here and the iterator is simply left invalid --
 * isValid() is the single way to ask whether the cursor is on an entry. */

/** Runs one positioning call, reporting only the errors that are not end-of-range. */
static void iteratorSeek(JNIEnv *env, jlong handle, int (*op)(tidesdb_iter_t *))
{
    int result = op((tidesdb_iter_t *)(uintptr_t)handle);
    if (result != TDB_SUCCESS && result != TDB_ERR_NOT_FOUND)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeekToFirst(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    (void)cls;
    iteratorSeek(env, handle, tidesdb_iter_seek_to_first);
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeekToLast(JNIEnv *env, jclass cls,
                                                                         jlong handle)
{
    (void)cls;
    iteratorSeek(env, handle, tidesdb_iter_seek_to_last);
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeNext(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    (void)cls;
    iteratorSeek(env, handle, tidesdb_iter_next);
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativePrev(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    (void)cls;
    iteratorSeek(env, handle, tidesdb_iter_prev);
}

/** Runs one keyed positioning call, with the same end-of-range handling. */
static void iteratorSeekKey(JNIEnv *env, jlong handle, jbyteArray key,
                            int (*op)(tidesdb_iter_t *, const uint8_t *, size_t))
{
    jni_bytes_t k;
    if (acquireBytes(env, key, &k) != 0) return;

    int result = op((tidesdb_iter_t *)(uintptr_t)handle, (const uint8_t *)k.data, (size_t)k.length);

    releaseBytes(env, &k);

    if (result != TDB_SUCCESS && result != TDB_ERR_NOT_FOUND)
    {
        throwResult(env, result);
    }
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeek(JNIEnv *env, jclass cls,
                                                                   jlong handle, jbyteArray key)
{
    (void)cls;
    iteratorSeekKey(env, handle, key, tidesdb_iter_seek);
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeSeekForPrev(JNIEnv *env, jclass cls,
                                                                          jlong handle,
                                                                          jbyteArray key)
{
    (void)cls;
    iteratorSeekKey(env, handle, key, tidesdb_iter_seek_for_prev);
}

JNIEXPORT jboolean JNICALL Java_com_tidesdb_TidesDBIterator_nativeValid(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    (void)env;
    (void)cls;
    return tidesdb_iter_valid((tidesdb_iter_t *)(uintptr_t)handle) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_TidesDBIterator_nativeKey(JNIEnv *env, jclass cls,
                                                                        jlong handle)
{
    (void)cls;

    uint8_t *key = NULL;
    size_t keySize = 0;
    int result = tidesdb_iter_key((tidesdb_iter_t *)(uintptr_t)handle, &key, &keySize);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }
    return toByteArrayAndFree(env, key, keySize);
}

JNIEXPORT jbyteArray JNICALL Java_com_tidesdb_TidesDBIterator_nativeValue(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    (void)cls;

    uint8_t *value = NULL;
    size_t valueSize = 0;
    int result = tidesdb_iter_value((tidesdb_iter_t *)(uintptr_t)handle, &value, &valueSize);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }
    return toByteArrayAndFree(env, value, valueSize);
}

JNIEXPORT jobject JNICALL Java_com_tidesdb_TidesDBIterator_nativeKeyValue(JNIEnv *env, jclass cls,
                                                                          jlong handle)
{
    (void)cls;

    uint8_t *key = NULL;
    size_t keySize = 0;
    uint8_t *value = NULL;
    size_t valueSize = 0;
    int result = tidesdb_iter_key_value((tidesdb_iter_t *)(uintptr_t)handle, &key, &keySize, &value,
                                        &valueSize);
    if (result != TDB_SUCCESS)
    {
        throwResult(env, result);
        return NULL;
    }

    jbyteArray jKey = toByteArrayAndFree(env, key, keySize);
    jbyteArray jValue = toByteArrayAndFree(env, value, valueSize);
    if (jKey == NULL || jValue == NULL)
    {
        return NULL;
    }

    jclass cls_ = (*env)->FindClass(env, "com/tidesdb/KeyValue");
    if (cls_ == NULL) return NULL;

    jmethodID ctor = (*env)->GetMethodID(env, cls_, "<init>", "([B[B)V");
    jobject result_obj = ctor != NULL ? (*env)->NewObject(env, cls_, ctor, jKey, jValue) : NULL;

    (*env)->DeleteLocalRef(env, cls_);
    return result_obj;
}

JNIEXPORT void JNICALL Java_com_tidesdb_TidesDBIterator_nativeFree(JNIEnv *env, jclass cls,
                                                                   jlong handle)
{
    (void)env;
    (void)cls;
    tidesdb_iter_free((tidesdb_iter_t *)(uintptr_t)handle);
}

/* ===== com.tidesdb.StallReason / com.tidesdb.IoClass ===== */

JNIEXPORT jstring JNICALL Java_com_tidesdb_StallReason_nativeName(JNIEnv *env, jclass cls,
                                                                  jint reason)
{
    (void)cls;
    return (*env)->NewStringUTF(env, tidesdb_stall_reason_name((tidesdb_stall_reason_t)reason));
}

JNIEXPORT jstring JNICALL Java_com_tidesdb_IoClass_nativeName(JNIEnv *env, jclass cls, jint cls_id)
{
    (void)cls;
    return (*env)->NewStringUTF(env, tidesdb_io_class_name((tidesdb_io_class_t)cls_id));
}

/* ===== com.tidesdb.Snapshot ===== */

JNIEXPORT jlong JNICALL Java_com_tidesdb_Snapshot_nativeSeq(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    return (jlong)tidesdb_snapshot_seq((const tidesdb_snapshot_t *)(uintptr_t)handle);
}

JNIEXPORT void JNICALL Java_com_tidesdb_Snapshot_nativeRelease(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    tidesdb_snapshot_release((tidesdb_snapshot_t *)(uintptr_t)handle);
}
