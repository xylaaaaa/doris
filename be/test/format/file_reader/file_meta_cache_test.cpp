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

#include "common/config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/file_meta_disk_cache.h"
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

class FileMetaDiskCacheTest : public testing::Test {
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

TEST_F(FileMetaDiskCacheTest, ReadReturnsPayloadWrittenThroughMetaQueue) {
    std::filesystem::path cache_dir = std::filesystem::current_path() / "file_meta_disk_cache_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    Defer defer {[&] { std::filesystem::remove_all(cache_dir); }};

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 16;
    settings.meta_queue_size = 1024 * 1024;
    settings.meta_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    for (int i = 0; i < 5000; ++i) {
        if (block_cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(block_cache.get_async_open_success());

    FileMetaDiskCache disk_cache(&block_cache);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/test.parquet", 123, 456);
    const std::string payload = "serialized footer payload";

    ASSERT_TRUE(disk_cache
                        .write(FileMetaDiskCacheFormat::PARQUET, meta_key, 123, 456,
                               std::string_view(payload))
                        .ok());

    std::string output;
    ASSERT_TRUE(
            disk_cache.read(FileMetaDiskCacheFormat::PARQUET, meta_key, 123, 456, &output).ok());
    EXPECT_EQ(output, payload);

    std::string stale_output;
    Status stale_status =
            disk_cache.read(FileMetaDiskCacheFormat::PARQUET, meta_key, 124, 456, &stale_output);
    EXPECT_TRUE(stale_status.is<ErrorCode::NOT_FOUND>());

    const std::string refreshed_payload = "refreshed serialized footer payload";
    ASSERT_TRUE(disk_cache
                        .write(FileMetaDiskCacheFormat::PARQUET, meta_key, 124, 456,
                               std::string_view(refreshed_payload))
                        .ok());
    ASSERT_TRUE(disk_cache.read(FileMetaDiskCacheFormat::PARQUET, meta_key, 124, 456, &stale_output)
                        .ok());
    EXPECT_EQ(stale_output, refreshed_payload);
}

TEST_F(FileMetaDiskCacheTest, InvalidEntryCanBeRefreshedAfterChecksumMismatch) {
    std::filesystem::path cache_dir =
            std::filesystem::current_path() / "file_meta_disk_cache_invalid_entry_test";
    if (std::filesystem::exists(cache_dir)) {
        std::filesystem::remove_all(cache_dir);
    }
    std::filesystem::create_directories(cache_dir);
    ScopedFileCacheDiskResourceLimitConfig disk_resource_limit_config;
    Defer defer {[&] { std::filesystem::remove_all(cache_dir); }};

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 1024;
    settings.meta_queue_size = 1024 * 1024;
    settings.meta_queue_elements = 1024;
    io::BlockFileCache block_cache(cache_dir.string(), settings);
    ASSERT_TRUE(block_cache.initialize().ok());
    for (int i = 0; i < 5000; ++i) {
        if (block_cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(block_cache.get_async_open_success());

    FileMetaDiskCache disk_cache(&block_cache);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/corrupt.parquet", 123, 456);
    const std::string payload = "serialized footer payload";
    ASSERT_TRUE(disk_cache
                        .write(FileMetaDiskCacheFormat::PARQUET, meta_key, 123, 456,
                               std::string_view(payload))
                        .ok());

    const auto hash = io::BlockFileCache::hash(
            FileMetaDiskCache::get_key(FileMetaDiskCacheFormat::PARQUET, meta_key));
    auto blocks = block_cache.get_blocks_by_key(hash);
    ASSERT_EQ(blocks.size(), 1);
    const std::string cache_file = blocks.begin()->second->get_cache_file();
    blocks.clear();

    constexpr size_t payload_offset = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;
    std::fstream cache_stream(cache_file, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(cache_stream.is_open());
    cache_stream.seekp(payload_offset);
    const char corrupted_byte = payload[0] == 'x' ? 'y' : 'x';
    cache_stream.write(&corrupted_byte, 1);
    ASSERT_TRUE(cache_stream.good());
    cache_stream.close();

    std::string output;
    Status status = disk_cache.read(FileMetaDiskCacheFormat::PARQUET, meta_key, 123, 456, &output);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_TRUE(output.empty());

    const std::string refreshed_payload = "refreshed serialized footer payload";
    ASSERT_TRUE(disk_cache
                        .write(FileMetaDiskCacheFormat::PARQUET, meta_key, 123, 456,
                               std::string_view(refreshed_payload))
                        .ok());
    ASSERT_TRUE(
            disk_cache.read(FileMetaDiskCacheFormat::PARQUET, meta_key, 123, 456, &output).ok());
    EXPECT_EQ(output, refreshed_payload);
}

} // namespace doris
