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

#include <filesystem>
#include <fstream>
#include <vector>

#include "common/config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/file_reader.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {
namespace {

class ScopedFileCacheDiskResourceLimitConfig {
public:
    ScopedFileCacheDiskResourceLimitConfig()
            : _old_enter_percent(config::file_cache_enter_disk_resource_limit_mode_percent),
              _old_exit_percent(config::file_cache_exit_disk_resource_limit_mode_percent),
              _old_ttl_gc_interval_ms(config::file_cache_background_ttl_gc_interval_ms),
              _old_ttl_info_update_interval_ms(
                      config::file_cache_background_ttl_info_update_interval_ms),
              _old_leak_scan_interval_seconds(config::file_cache_leak_scan_interval_seconds) {
        config::file_cache_enter_disk_resource_limit_mode_percent = 101;
        config::file_cache_exit_disk_resource_limit_mode_percent = 100;
        config::file_cache_background_ttl_gc_interval_ms = 1;
        config::file_cache_background_ttl_info_update_interval_ms = 1;
        config::file_cache_leak_scan_interval_seconds = 0;
    }

    ~ScopedFileCacheDiskResourceLimitConfig() {
        config::file_cache_enter_disk_resource_limit_mode_percent = _old_enter_percent;
        config::file_cache_exit_disk_resource_limit_mode_percent = _old_exit_percent;
        config::file_cache_background_ttl_gc_interval_ms = _old_ttl_gc_interval_ms;
        config::file_cache_background_ttl_info_update_interval_ms =
                _old_ttl_info_update_interval_ms;
        config::file_cache_leak_scan_interval_seconds = _old_leak_scan_interval_seconds;
    }

private:
    int32_t _old_enter_percent;
    int32_t _old_exit_percent;
    int64_t _old_ttl_gc_interval_ms;
    int64_t _old_ttl_info_update_interval_ms;
    int64_t _old_leak_scan_interval_seconds;
};

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
    MockFileReader(const std::string& file_name, size_t size)
            : _file_name(file_name), _size(size) {}
    ~MockFileReader() override = default;

    const io::Path& path() const override {
        static io::Path p(_file_name);
        return p;
    }

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
    std::string _file_name;
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

    // mtime == 0, use file_size
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
TEST(FileMetaCacheTest, KeyContentVerification) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 0x0102030405060708;
    int64_t file_size = 0x1112131415161718;

    std::string key_with_mtime = FileMetaCache::get_key(file_name, mtime, file_size);

    ASSERT_EQ(key_with_mtime.size(), file_name.size() + sizeof(int64_t));

    EXPECT_EQ(memcmp(key_with_mtime.data(), file_name.data(), file_name.size()), 0);

    int64_t extracted_mtime = 0;
    memcpy(&extracted_mtime, key_with_mtime.data() + file_name.size(), sizeof(int64_t));
    EXPECT_EQ(extracted_mtime, mtime);

    std::string key_with_filesize = FileMetaCache::get_key(file_name, 0, file_size);
    ASSERT_EQ(key_with_filesize.size(), file_name.size() + sizeof(int64_t));
    EXPECT_EQ(memcmp(key_with_filesize.data(), file_name.data(), file_name.size()), 0);
    int64_t extracted_filesize = 0;
    memcpy(&extracted_filesize, key_with_filesize.data() + file_name.size(), sizeof(int64_t));
    EXPECT_EQ(extracted_filesize, file_size);
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

} // namespace doris
