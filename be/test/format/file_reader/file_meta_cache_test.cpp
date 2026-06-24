// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "io/fs/file_meta_cache.h"

#include <crc32c/crc32c.h>

#include <filesystem>
#include <fstream>
#include <vector>

#include "common/config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "runtime/exec_env.h"
#include "util/coding.h"
#include "util/defer_op.h"

namespace doris {
namespace {

constexpr size_t FILE_META_CACHE_DISK_HEADER_SIZE_FOR_TEST = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;

class ScopedFileCacheDiskResourceLimitConfig {
public:
    ScopedFileCacheDiskResourceLimitConfig()
            : _old_enter_percent(config::file_cache_enter_disk_resource_limit_mode_percent),
              _old_exit_percent(config::file_cache_exit_disk_resource_limit_mode_percent),
              _old_ttl_gc_interval_ms(config::file_cache_background_ttl_gc_interval_ms),
              _old_ttl_info_update_interval_ms(
                      config::file_cache_background_ttl_info_update_interval_ms),
              _old_leak_scan_interval_seconds(config::file_cache_leak_scan_interval_seconds),
              _old_block_lru_update_interval_ms(
                      config::file_cache_background_block_lru_update_interval_ms),
              _old_block_lru_update_qps_limit(
                      config::file_cache_background_block_lru_update_qps_limit) {
        config::file_cache_enter_disk_resource_limit_mode_percent = 101;
        config::file_cache_exit_disk_resource_limit_mode_percent = 100;
        config::file_cache_background_ttl_gc_interval_ms = 1;
        config::file_cache_background_ttl_info_update_interval_ms = 1;
        config::file_cache_leak_scan_interval_seconds = 0;
        config::file_cache_background_block_lru_update_interval_ms = 10;
        config::file_cache_background_block_lru_update_qps_limit = 1000;
    }

    ~ScopedFileCacheDiskResourceLimitConfig() {
        config::file_cache_enter_disk_resource_limit_mode_percent = _old_enter_percent;
        config::file_cache_exit_disk_resource_limit_mode_percent = _old_exit_percent;
        config::file_cache_background_ttl_gc_interval_ms = _old_ttl_gc_interval_ms;
        config::file_cache_background_ttl_info_update_interval_ms =
                _old_ttl_info_update_interval_ms;
        config::file_cache_leak_scan_interval_seconds = _old_leak_scan_interval_seconds;
        config::file_cache_background_block_lru_update_interval_ms =
                _old_block_lru_update_interval_ms;
        config::file_cache_background_block_lru_update_qps_limit = _old_block_lru_update_qps_limit;
    }

private:
    int32_t _old_enter_percent;
    int32_t _old_exit_percent;
    int64_t _old_ttl_gc_interval_ms;
    int64_t _old_ttl_info_update_interval_ms;
    int64_t _old_leak_scan_interval_seconds;
    int64_t _old_block_lru_update_interval_ms;
    int64_t _old_block_lru_update_qps_limit;
};

bool wait_for_cache_async_open(io::BlockFileCache* block_cache) {
    for (int i = 0; i < 5000; ++i) {
        if (block_cache->get_async_open_success()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return block_cache->get_async_open_success();
}

std::string build_disk_cache_value_for_test(FileMetaCacheFormat format, int64_t modification_time,
                                            int64_t file_size, std::string_view payload) {
    std::string value;
    value.reserve(FILE_META_CACHE_DISK_HEADER_SIZE_FOR_TEST + payload.size());
    value.append("DFMC", 4);
    value.push_back(1);
    value.push_back(static_cast<char>(format));
    value.push_back(0);
    value.push_back(0);
    put_fixed64_le(&value, static_cast<uint64_t>(file_size));
    put_fixed64_le(&value, static_cast<uint64_t>(modification_time));
    put_fixed64_le(&value, static_cast<uint64_t>(payload.size()));
    put_fixed32_le(&value, crc32c::Crc32c(payload.data(), payload.size()));
    value.append(payload.data(), payload.size());
    return value;
}

Status read_block_cache_value_for_test(io::BlockFileCache* block_cache,
                                       const io::UInt128Wrapper& hash, std::string* value) {
    value->clear();
    auto blocks = block_cache->get_blocks_by_key(hash);
    if (blocks.empty()) {
        return Status::NotFound("block cache value not found");
    }

    size_t current_pos = 0;
    for (const auto& [_, block] : blocks) {
        const auto& range = block->range();
        if (range.left != current_pos) {
            return Status::NotFound("block cache value has holes");
        }
        std::string block_value(range.size(), '\0');
        RETURN_IF_ERROR(block->read(Slice(block_value.data(), block_value.size()), 0));
        value->append(block_value);
        current_pos = range.right + 1;
    }
    return Status::OK();
}

class FileMetaCacheDiskTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        ExecEnv::GetInstance()->set_file_cache_open_fd_cache(std::make_unique<io::FDCache>());
    }

    static void TearDownTestSuite() {
        ExecEnv::GetInstance()->set_file_cache_open_fd_cache(nullptr);
    }
};

class MockFileReader : public io::FileReader {
public:
    MockFileReader(const std::string& file_name, size_t size) : _path(file_name), _size(size) {}
    ~MockFileReader() override = default;

    const io::Path& path() const override { return _path; }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

    Status close() override {
        _closed = true;
        return Status::OK();
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        *bytes_read = 0;
        return Status::OK();
    }

private:
    io::Path _path;
    size_t _size;
    bool _closed {false};
};
} // anonymous namespace

TEST(FileMetaCacheTest, KeyGenerationFromParams) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 123456789;
    int64_t file_size = 987654321;

    std::string key1 = FileMetaCache::get_key(file_name, mtime, file_size);
    std::string key2 = FileMetaCache::get_key(file_name, mtime, file_size);
    EXPECT_EQ(key1, key2) << "Same parameters should produce same key";

    // Different mtime should produce different key
    std::string key3 = FileMetaCache::get_key(file_name, mtime + 1, file_size);
    EXPECT_NE(key1, key3);

    // Different file size should produce different key even when mtime is set.
    std::string key_with_different_file_size =
            FileMetaCache::get_key(file_name, mtime, file_size + 1);
    EXPECT_NE(key1, key_with_different_file_size);

    // mtime == 0 still includes file_size
    std::string key4 = FileMetaCache::get_key(file_name, 0, file_size);
    std::string key5 = FileMetaCache::get_key(file_name, 0, file_size);
    EXPECT_EQ(key4, key5);
    EXPECT_NE(key1, key4);

    // mtime == 0, different file_size
    std::string key6 = FileMetaCache::get_key(file_name, 0, file_size + 1);
    EXPECT_NE(key4, key6);
}

TEST(FileMetaCacheTest, KeyGenerationFromFileReader) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 123456789;
    int64_t file_size = 100;

    // file_description.file_size != -1, use it as file size
    io::FileDescription desc1;
    desc1.mtime = mtime;
    desc1.file_size = file_size;
    auto reader1 = std::make_shared<MockFileReader>(file_name, 200);

    std::string key1 = FileMetaCache::get_key(reader1, desc1);
    std::string expected_key1 = FileMetaCache::get_key(file_name, mtime, file_size);
    EXPECT_EQ(key1, expected_key1);

    // file_description.file_size == -1, use reader->size()
    io::FileDescription desc2;
    desc2.mtime = 0;
    desc2.file_size = -1;
    auto reader2 = std::make_shared<MockFileReader>(file_name, 300);

    std::string key2 = FileMetaCache::get_key(reader2, desc2);
    std::string expected_key2 = FileMetaCache::get_key(file_name, 0, 300);
    EXPECT_EQ(key2, expected_key2);
}

TEST(FileMetaCacheTest, KeyGenerationIncludesFileSystemName) {
    const std::string file_name = "/warehouse/default/table/data.orc";
    constexpr int64_t mtime = 123456789;
    constexpr int64_t file_size = 100;

    io::FileDescription desc1;
    desc1.mtime = mtime;
    desc1.file_size = file_size;
    desc1.fs_name = "hdfs://nameservice1";

    io::FileDescription desc2;
    desc2.mtime = mtime;
    desc2.file_size = file_size;
    desc2.fs_name = "hdfs://nameservice2";

    auto reader = std::make_shared<MockFileReader>(file_name, file_size);

    std::string key1 = FileMetaCache::get_key(reader, desc1);
    std::string key2 = FileMetaCache::get_key(reader, desc2);

    EXPECT_NE(key1, key2);
}

TEST(FileMetaCacheTest, KeyContentVerification) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 0x0102030405060708;
    int64_t file_size = 0x1112131415161718;

    std::string key_with_mtime = FileMetaCache::get_key(file_name, mtime, file_size);

    ASSERT_EQ(key_with_mtime.size(), sizeof(uint64_t) + file_name.size() + sizeof(int64_t) * 2);

    const auto* key_with_mtime_ptr = reinterpret_cast<const uint8_t*>(key_with_mtime.data());
    EXPECT_EQ(decode_fixed64_le(key_with_mtime_ptr), file_name.size());
    key_with_mtime_ptr += sizeof(uint64_t);

    EXPECT_EQ(memcmp(key_with_mtime_ptr, file_name.data(), file_name.size()), 0);
    key_with_mtime_ptr += file_name.size();

    EXPECT_EQ(static_cast<int64_t>(decode_fixed64_le(key_with_mtime_ptr)), mtime);
    key_with_mtime_ptr += sizeof(uint64_t);
    EXPECT_EQ(static_cast<int64_t>(decode_fixed64_le(key_with_mtime_ptr)), file_size);

    std::string key_with_filesize = FileMetaCache::get_key(file_name, 0, file_size);
    ASSERT_EQ(key_with_filesize.size(), sizeof(uint64_t) + file_name.size() + sizeof(int64_t) * 2);
    const auto* key_with_filesize_ptr = reinterpret_cast<const uint8_t*>(key_with_filesize.data());
    EXPECT_EQ(decode_fixed64_le(key_with_filesize_ptr), file_name.size());
    key_with_filesize_ptr += sizeof(uint64_t) + file_name.size();
    EXPECT_EQ(static_cast<int64_t>(decode_fixed64_le(key_with_filesize_ptr)), 0);
    key_with_filesize_ptr += sizeof(uint64_t);
    EXPECT_EQ(static_cast<int64_t>(decode_fixed64_le(key_with_filesize_ptr)), file_size);
}

TEST(FileMetaCacheTest, HdfsFileCacheIdentityUsesEffectiveFileSystemName) {
    io::FileSystemProperties properties;
    properties.system_type = TFileType::FILE_HDFS;
    properties.hdfs_params.__set_fs_name("hdfs://nameservice1");

    io::FileDescription default_fs_file;
    default_fs_file.path = "/warehouse/default/table/data.orc";
    EXPECT_EQ(FileFactory::get_file_cache_identity(properties, default_fs_file),
              "hdfs://nameservice1");

    io::FileDescription uri_file;
    uri_file.path = "hdfs://nameservice2/warehouse/default/table/data.orc";
    EXPECT_EQ(FileFactory::get_file_cache_identity(properties, uri_file), "hdfs://nameservice2");
}

TEST(FileMetaCacheTest, S3FileCacheIdentityIncludesEndpoint) {
    io::FileDescription file;
    file.path = "s3://bucket/table/data.parquet";

    io::FileSystemProperties properties1;
    properties1.system_type = TFileType::FILE_S3;
    properties1.properties["AWS_ENDPOINT"] = "http://minio-a:9000";
    properties1.properties["AWS_REGION"] = "us-east-1";

    io::FileSystemProperties properties2 = properties1;
    properties2.properties["AWS_ENDPOINT"] = "http://minio-b:9000";

    EXPECT_NE(FileFactory::get_file_cache_identity(properties1, file),
              FileFactory::get_file_cache_identity(properties2, file));
}

TEST(FileMetaCacheTest, InsertAndLookupWithIntValue) {
    FileMetaCache cache(1024 * 1024);

    int* value = new int(12345);
    ObjLRUCache::CacheHandle handle;

    cache.insert("key_int", value, &handle);
    ASSERT_NE(handle._cache, nullptr);

    const int* cached_val = handle.data<int>();
    ASSERT_NE(cached_val, nullptr);
    EXPECT_EQ(*cached_val, 12345);

    ObjLRUCache::CacheHandle handle2;
    cache.lookup("key_int", &handle2);

    ASSERT_NE(handle2._cache, nullptr);

    const int* cached_val2 = handle2.data<int>();
    ASSERT_NE(cached_val2, nullptr);
    EXPECT_EQ(*cached_val2, 12345);
}

TEST(FileMetaCacheTest, ExternalFileMetaDiskCacheSwitchIsStartupOnly) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};

    config::enable_external_file_meta_disk_cache = false;
    Status status = config::set_config("enable_external_file_meta_disk_cache", "true");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>() || status.is<ErrorCode::NOT_FOUND>())
            << status.to_string();
    EXPECT_FALSE(config::enable_external_file_meta_disk_cache);
}

TEST(FileMetaCacheTest, ExternalFileMetaDiskCacheIsEnabledByDefault) {
    EXPECT_TRUE(config::enable_external_file_meta_disk_cache);
}

TEST(FileMetaCacheTest, LookupUpdatesMemoryHitProfile) {
    FileMetaCache cache(config::max_external_file_meta_cache_num);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/memory.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456};
    auto cached_payload = std::make_unique<std::string>("serialized footer payload");
    ObjLRUCache::CacheHandle insert_handle;
    ASSERT_TRUE(cache.insert(meta_key, cached_payload, &insert_handle));

    int64_t hit_cache = 0;
    int64_t hit_memory_cache = 0;
    int64_t hit_disk_cache = 0;
    int64_t miss_disk_cache = 0;
    int64_t write_disk_cache = 0;
    int64_t read_disk_cache_time = 0;
    int64_t write_disk_cache_time = 0;
    FileMetaCacheProfile profile {.hit_cache = &hit_cache,
                                  .hit_memory_cache = &hit_memory_cache,
                                  .hit_disk_cache = &hit_disk_cache,
                                  .miss_disk_cache = &miss_disk_cache,
                                  .write_disk_cache = &write_disk_cache,
                                  .read_disk_cache_time = &read_disk_cache_time,
                                  .write_disk_cache_time = &write_disk_cache_time};

    ObjLRUCache::CacheHandle lookup_handle;
    std::string output;
    const auto lookup_result = cache.lookup(meta_context, &lookup_handle, &output, &profile);

    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::MEMORY_HIT);
    EXPECT_EQ(hit_cache, 1);
    EXPECT_EQ(hit_memory_cache, 1);
    EXPECT_EQ(hit_disk_cache, 0);
    EXPECT_EQ(miss_disk_cache, 0);
    EXPECT_EQ(write_disk_cache, 0);
    EXPECT_EQ(read_disk_cache_time, 0);
    EXPECT_EQ(write_disk_cache_time, 0);
}

TEST_F(FileMetaCacheDiskTest, DiskCacheWorksWhenMemoryCacheAdmissionIsDisabled) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_without_memory_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 16;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    for (int i = 0; i < 5000; ++i) {
        if (block_cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(block_cache.get_async_open_success());

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    const std::string meta_key =
            FileMetaCache::get_key("s3://bucket/without-memory.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456,
                                             .enable_memory_cache = false};
    const std::string payload = "serialized footer payload";
    auto cached_payload = std::make_unique<std::string>(payload);
    ObjLRUCache::CacheHandle cache_handle;

    const auto insert_result = cache.insert(meta_context, cached_payload, &cache_handle, payload);
    EXPECT_TRUE(insert_result.persisted_inserted);
    EXPECT_FALSE(insert_result.memory_inserted);

    ObjLRUCache::CacheHandle memory_lookup_handle;
    EXPECT_FALSE(cache.lookup(meta_key, &memory_lookup_handle));

    std::string output;
    ObjLRUCache::CacheHandle lookup_handle;
    const auto lookup_result = cache.lookup(meta_context, &lookup_handle, &output);
    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    EXPECT_EQ(output, payload);
    EXPECT_FALSE(lookup_handle.valid());
}

TEST_F(FileMetaCacheDiskTest, ReadReturnsPayloadWrittenThroughIndexQueue) {
    std::filesystem::path cache_dir = std::filesystem::current_path() / "file_meta_disk_cache_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 16;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    for (int i = 0; i < 5000; ++i) {
        if (block_cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(block_cache.get_async_open_success());

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/test.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456};
    const std::string payload = "serialized footer payload";
    auto cached_payload = std::make_unique<std::string>(payload);
    ObjLRUCache::CacheHandle cache_handle;
    int64_t write_disk_cache = 0;
    int64_t write_disk_cache_time = 0;
    FileMetaCacheProfile insert_profile {.write_disk_cache = &write_disk_cache,
                                         .write_disk_cache_time = &write_disk_cache_time};

    const auto insert_result = cache.insert(meta_context, cached_payload, &cache_handle,
                                            std::string_view(payload), &insert_profile);
    EXPECT_TRUE(insert_result.persisted_inserted);
    EXPECT_EQ(write_disk_cache, 1);

    FileMetaCache cache_after_l1_miss(config::max_external_file_meta_cache_num, &block_cache);
    std::string output;
    ObjLRUCache::CacheHandle lookup_handle;
    int64_t hit_cache = 0;
    int64_t hit_memory_cache = 0;
    int64_t hit_disk_cache = 0;
    int64_t miss_disk_cache = 0;
    int64_t read_disk_cache_time = 0;
    FileMetaCacheProfile lookup_profile {.hit_cache = &hit_cache,
                                         .hit_memory_cache = &hit_memory_cache,
                                         .hit_disk_cache = &hit_disk_cache,
                                         .miss_disk_cache = &miss_disk_cache,
                                         .read_disk_cache_time = &read_disk_cache_time};
    const auto lookup_result =
            cache_after_l1_miss.lookup(meta_context, &lookup_handle, &output, &lookup_profile);
    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    EXPECT_EQ(output, payload);
    EXPECT_EQ(hit_cache, 1);
    EXPECT_EQ(hit_memory_cache, 0);
    EXPECT_EQ(hit_disk_cache, 1);
    EXPECT_EQ(miss_disk_cache, 0);

    std::string stale_output;
    ObjLRUCache::CacheHandle stale_lookup_handle;
    const FileMetaCacheContext stale_meta_context {.format = FileMetaCacheFormat::PARQUET,
                                                   .key = meta_key,
                                                   .modification_time = 124,
                                                   .file_size = 456};
    int64_t stale_miss_disk_cache = 0;
    FileMetaCacheProfile stale_lookup_profile {.miss_disk_cache = &stale_miss_disk_cache};
    const auto stale_lookup_result = cache_after_l1_miss.lookup(
            stale_meta_context, &stale_lookup_handle, &stale_output, &stale_lookup_profile);
    EXPECT_EQ(stale_lookup_result.state, FileMetaCacheLookupState::MISS);
    EXPECT_EQ(stale_miss_disk_cache, 1);

    const std::string refreshed_payload = "refreshed serialized footer payload";
    auto refreshed_cached_payload = std::make_unique<std::string>(refreshed_payload);
    ObjLRUCache::CacheHandle refreshed_cache_handle;
    const auto refreshed_insert_result = cache_after_l1_miss.insert(
            stale_meta_context, refreshed_cached_payload, &refreshed_cache_handle,
            std::string_view(refreshed_payload), &insert_profile);
    EXPECT_TRUE(refreshed_insert_result.persisted_inserted);

    FileMetaCache cache_after_stale_l1_miss(config::max_external_file_meta_cache_num, &block_cache);
    ObjLRUCache::CacheHandle refreshed_lookup_handle;
    const auto refreshed_lookup_result = cache_after_stale_l1_miss.lookup(
            stale_meta_context, &refreshed_lookup_handle, &stale_output, &lookup_profile);
    EXPECT_EQ(refreshed_lookup_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    EXPECT_EQ(stale_output, refreshed_payload);
}

TEST_F(FileMetaCacheDiskTest, PersistentHitRefreshesIndexQueueLru) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_lru_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 96;
    settings.max_file_block_size = 64;
    settings.index_queue_size = 96;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    ASSERT_TRUE(wait_for_cache_async_open(&block_cache));

    const std::string first_key = FileMetaCache::get_key("s3://bucket/lru-first.parquet", 123, 456);
    const std::string second_key =
            FileMetaCache::get_key("s3://bucket/lru-second.parquet", 123, 456);
    const std::string third_key = FileMetaCache::get_key("s3://bucket/lru-third.parquet", 123, 456);
    const FileMetaCacheContext first_context {.format = FileMetaCacheFormat::PARQUET,
                                              .key = first_key,
                                              .modification_time = 123,
                                              .file_size = 456,
                                              .enable_memory_cache = false};
    const FileMetaCacheContext second_context {.format = FileMetaCacheFormat::PARQUET,
                                               .key = second_key,
                                               .modification_time = 123,
                                               .file_size = 456,
                                               .enable_memory_cache = false};
    const FileMetaCacheContext third_context {.format = FileMetaCacheFormat::PARQUET,
                                              .key = third_key,
                                              .modification_time = 123,
                                              .file_size = 456,
                                              .enable_memory_cache = false};

    auto insert_payload = [&](FileMetaCache& cache, const FileMetaCacheContext& context,
                              std::string_view payload) {
        auto cached_payload = std::make_unique<std::string>(payload);
        ObjLRUCache::CacheHandle cache_handle;
        const auto insert_result = cache.insert(context, cached_payload, &cache_handle, payload);
        ASSERT_TRUE(insert_result.persisted_inserted);
    };

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    insert_payload(cache, first_context, "payload1");
    insert_payload(cache, second_context, "payload2");

    FileMetaCache cache_after_l1_miss(config::max_external_file_meta_cache_num, &block_cache);
    std::string output;
    ObjLRUCache::CacheHandle lookup_handle;
    const auto first_lookup_result =
            cache_after_l1_miss.lookup(first_context, &lookup_handle, &output);
    ASSERT_EQ(first_lookup_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    ASSERT_EQ(output, "payload1");

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    insert_payload(cache, third_context, "payload3");

    FileMetaCache cache_after_eviction(config::max_external_file_meta_cache_num, &block_cache);
    ObjLRUCache::CacheHandle first_lookup_after_eviction_handle;
    const auto first_lookup_after_eviction_result = cache_after_eviction.lookup(
            first_context, &first_lookup_after_eviction_handle, &output);
    EXPECT_EQ(first_lookup_after_eviction_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    EXPECT_EQ(output, "payload1");

    std::string second_output;
    ObjLRUCache::CacheHandle second_lookup_after_eviction_handle;
    const auto second_lookup_after_eviction_result = cache_after_eviction.lookup(
            second_context, &second_lookup_after_eviction_handle, &second_output);
    EXPECT_EQ(second_lookup_after_eviction_result.state, FileMetaCacheLookupState::MISS);
    EXPECT_TRUE(second_output.empty());
}

TEST_F(FileMetaCacheDiskTest, InvalidEntryCanBeRefreshedAfterChecksumMismatch) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_invalid_entry_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 1024;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    for (int i = 0; i < 5000; ++i) {
        if (block_cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(block_cache.get_async_open_success());

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/corrupt.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456};
    const std::string payload = "serialized footer payload";
    auto cached_payload = std::make_unique<std::string>(payload);
    ObjLRUCache::CacheHandle cache_handle;
    const auto insert_result =
            cache.insert(meta_context, cached_payload, &cache_handle, std::string_view(payload));
    ASSERT_TRUE(insert_result.persisted_inserted);

    constexpr size_t payload_offset = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;
    std::vector<std::filesystem::path> cache_files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(cache_dir)) {
        if (entry.is_regular_file() &&
            entry.file_size() == payload_offset + static_cast<size_t>(payload.size())) {
            cache_files.emplace_back(entry.path());
        }
    }
    ASSERT_EQ(cache_files.size(), 1);

    std::fstream cache_stream(cache_files[0], std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(cache_stream.is_open());
    cache_stream.seekp(payload_offset);
    const char corrupted_byte = payload[0] == 'x' ? 'y' : 'x';
    cache_stream.write(&corrupted_byte, 1);
    ASSERT_TRUE(cache_stream.good());
    cache_stream.close();

    std::string output;
    FileMetaCache cache_after_l1_miss(config::max_external_file_meta_cache_num, &block_cache);
    ObjLRUCache::CacheHandle lookup_handle;
    const auto lookup_result = cache_after_l1_miss.lookup(meta_context, &lookup_handle, &output);
    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::MISS);
    EXPECT_TRUE(output.empty());

    const std::string refreshed_payload = "refreshed serialized footer payload";
    auto refreshed_cached_payload = std::make_unique<std::string>(refreshed_payload);
    ObjLRUCache::CacheHandle refreshed_cache_handle;
    const auto refreshed_insert_result = cache_after_l1_miss.insert(
            meta_context, refreshed_cached_payload, &refreshed_cache_handle,
            std::string_view(refreshed_payload));
    ASSERT_TRUE(refreshed_insert_result.persisted_inserted);

    FileMetaCache cache_after_refresh_l1_miss(config::max_external_file_meta_cache_num,
                                              &block_cache);
    ObjLRUCache::CacheHandle refreshed_lookup_handle;
    const auto refreshed_lookup_result =
            cache_after_refresh_l1_miss.lookup(meta_context, &refreshed_lookup_handle, &output);
    EXPECT_EQ(refreshed_lookup_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    EXPECT_EQ(output, refreshed_payload);
}

TEST_F(FileMetaCacheDiskTest, FailedMultiBlockInsertRemovesPartialEntry) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_partial_insert_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 64;
    settings.max_file_block_size = 64;
    settings.index_queue_size = 64;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    ASSERT_TRUE(wait_for_cache_async_open(&block_cache));

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/partial.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456,
                                             .enable_memory_cache = false};
    const std::string payload(128, 'x');
    auto cached_payload = std::make_unique<std::string>(payload);
    ObjLRUCache::CacheHandle cache_handle;

    const auto insert_result =
            cache.insert(meta_context, cached_payload, &cache_handle, std::string_view(payload));
    ASSERT_FALSE(insert_result.persisted_inserted);

    const auto hash = io::BlockFileCache::hash(
            FileMetaCache::get_persistent_cache_key(FileMetaCacheFormat::PARQUET, meta_key));
    EXPECT_TRUE(block_cache.get_blocks_by_key(hash).empty());
}

TEST_F(FileMetaCacheDiskTest, BlockFileCacheSetWritesCompleteValue) {
    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    settings.max_file_block_size = 8;
    settings.max_query_cache_size = 1024 * 1024;
    settings.storage = "memory";
    io::BlockFileCache block_cache("file_meta_block_cache_set_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    io::ReadStatistics stats;
    io::CacheContext cache_context;
    cache_context.cache_type = io::FileCacheType::INDEX;
    cache_context.query_id = TUniqueId();
    cache_context.expiration_time = 0;
    cache_context.is_cold_data = false;
    cache_context.is_warmup = false;
    cache_context.stats = &stats;

    const std::string disk_cache_key = "file_meta_cache:set-interface";
    const auto hash = io::BlockFileCache::hash(disk_cache_key);
    const std::string value = "serialized file meta cache value across blocks";

    Status status = block_cache.set(hash, value, cache_context);
    ASSERT_TRUE(status.ok()) << status;

    std::string cached_value;
    status = read_block_cache_value_for_test(&block_cache, hash, &cached_value);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(cached_value, value);
}

TEST_F(FileMetaCacheDiskTest, LookupRemovesPartialEntryAfterPayloadReadFailure) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_partial_lookup_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 64;
    settings.max_file_block_size = 64;
    settings.index_queue_size = 64;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    ASSERT_TRUE(wait_for_cache_async_open(&block_cache));

    const std::string meta_key =
            FileMetaCache::get_key("s3://bucket/partial-lookup.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456,
                                             .enable_memory_cache = false};
    const std::string payload(128, 'x');
    const std::string value = build_disk_cache_value_for_test(FileMetaCacheFormat::PARQUET,
                                                              meta_context.modification_time,
                                                              meta_context.file_size, payload);
    const auto hash = io::BlockFileCache::hash(
            FileMetaCache::get_persistent_cache_key(FileMetaCacheFormat::PARQUET, meta_key));

    {
        io::ReadStatistics stats;
        io::CacheContext cache_context;
        cache_context.cache_type = io::FileCacheType::INDEX;
        cache_context.query_id = TUniqueId();
        cache_context.expiration_time = 0;
        cache_context.is_cold_data = false;
        cache_context.is_warmup = false;
        cache_context.stats = &stats;
        auto holder = block_cache.get_or_set(hash, 0, value.size(), cache_context);

        bool downloaded_prefix = false;
        bool saw_uncached_suffix = false;
        for (const auto& block : holder.file_blocks) {
            const auto state = block->state();
            if (state == io::FileBlock::State::SKIP_CACHE) {
                saw_uncached_suffix = true;
                continue;
            }
            ASSERT_EQ(state, io::FileBlock::State::EMPTY);
            ASSERT_FALSE(downloaded_prefix);
            ASSERT_EQ(block->range().left, 0);
            ASSERT_EQ(block->get_or_set_downloader(), io::FileBlock::get_caller_id());
            const auto& range = block->range();
            Status status = block->append(Slice(value.data() + range.left, range.size()));
            ASSERT_TRUE(status.ok()) << status;
            status = block->finalize();
            ASSERT_TRUE(status.ok()) << status;
            downloaded_prefix = true;
        }
        ASSERT_TRUE(downloaded_prefix);
        ASSERT_TRUE(saw_uncached_suffix);
    }
    ASSERT_EQ(block_cache.get_blocks_by_key(hash).size(), 1);

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    std::string output;
    ObjLRUCache::CacheHandle lookup_handle;
    const auto lookup_result = cache.lookup(meta_context, &lookup_handle, &output);
    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::MISS);
    EXPECT_TRUE(output.empty());
    EXPECT_TRUE(block_cache.get_blocks_by_key(hash).empty());
}

TEST_F(FileMetaCacheDiskTest, NegativeMaxEntryBytesDisablesPersistentCache) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_negative_max_entry_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
        std::filesystem::remove_all(cache_dir);
    }};
    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = -1;

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 1024;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    for (int i = 0; i < 5000; ++i) {
        if (block_cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(block_cache.get_async_open_success());

    FileMetaCache cache(config::max_external_file_meta_cache_num, &block_cache);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/negative.parquet", 123, 456);
    const FileMetaCacheContext meta_context {.format = FileMetaCacheFormat::PARQUET,
                                             .key = meta_key,
                                             .modification_time = 123,
                                             .file_size = 456};
    const std::string payload = "serialized footer payload";
    auto cached_payload = std::make_unique<std::string>(payload);
    ObjLRUCache::CacheHandle cache_handle;
    int64_t write_disk_cache = 0;
    FileMetaCacheProfile insert_profile {.write_disk_cache = &write_disk_cache};

    const auto insert_result = cache.insert(meta_context, cached_payload, &cache_handle,
                                            std::string_view(payload), &insert_profile);
    EXPECT_FALSE(insert_result.persisted_inserted);
    EXPECT_EQ(write_disk_cache, 0);

    FileMetaCache cache_after_l1_miss(config::max_external_file_meta_cache_num, &block_cache);
    std::string output;
    ObjLRUCache::CacheHandle lookup_handle;
    const auto lookup_result = cache_after_l1_miss.lookup(meta_context, &lookup_handle, &output);
    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::MISS);
    EXPECT_TRUE(output.empty());
}

} // namespace doris
