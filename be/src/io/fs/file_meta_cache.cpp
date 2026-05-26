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
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <cstring>

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"

namespace doris {
namespace {

constexpr std::string_view FILE_META_CACHE_DISK_MAGIC = "DFMC";
constexpr uint8_t FILE_META_CACHE_DISK_VERSION = 1;
constexpr size_t FILE_META_CACHE_DISK_HEADER_SIZE = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;

std::string_view format_name(FileMetaCacheFormat format) {
    switch (format) {
    case FileMetaCacheFormat::PARQUET:
        return "parquet";
    case FileMetaCacheFormat::ORC:
        return "orc";
    }
    DCHECK(false) << "unknown file meta cache format";
    return "unknown";
}

struct FileMetaCacheDiskHeader {
    FileMetaCacheFormat format;
    int64_t modification_time = 0;
    int64_t file_size = 0;
    uint64_t payload_size = 0;
    uint32_t checksum = 0;
};

Status parse_disk_cache_header(std::string_view header, FileMetaCacheDiskHeader* parsed) {
    DCHECK(header.size() == FILE_META_CACHE_DISK_HEADER_SIZE);
    if (std::memcmp(header.data(), FILE_META_CACHE_DISK_MAGIC.data(),
                    FILE_META_CACHE_DISK_MAGIC.size()) != 0) {
        return Status::NotFound("file meta disk cache magic mismatch");
    }

    const auto* ptr =
            reinterpret_cast<const uint8_t*>(header.data() + FILE_META_CACHE_DISK_MAGIC.size());
    const uint8_t version = *ptr++;
    if (version != FILE_META_CACHE_DISK_VERSION) {
        return Status::NotFound("file meta disk cache version mismatch");
    }

    parsed->format = static_cast<FileMetaCacheFormat>(*ptr++);
    ptr += 2;
    parsed->file_size = static_cast<int64_t>(decode_fixed64_le(ptr));
    ptr += sizeof(uint64_t);
    parsed->modification_time = static_cast<int64_t>(decode_fixed64_le(ptr));
    ptr += sizeof(uint64_t);
    parsed->payload_size = decode_fixed64_le(ptr);
    ptr += sizeof(uint64_t);
    parsed->checksum = decode_fixed32_le(ptr);
    return Status::OK();
}

std::string build_disk_cache_value(FileMetaCacheFormat format, int64_t modification_time,
                                   int64_t file_size, std::string_view payload) {
    std::string value;
    value.reserve(FILE_META_CACHE_DISK_HEADER_SIZE + payload.size());
    value.append(FILE_META_CACHE_DISK_MAGIC.data(), FILE_META_CACHE_DISK_MAGIC.size());
    value.push_back(static_cast<char>(FILE_META_CACHE_DISK_VERSION));
    value.push_back(static_cast<char>(format));
    value.push_back(0);
    value.push_back(0);
    put_fixed64_le(&value, static_cast<uint64_t>(file_size));
    put_fixed64_le(&value, static_cast<uint64_t>(modification_time));
    put_fixed64_le(&value, payload.size());
    put_fixed32_le(&value, crc32c::Crc32c(payload.data(), payload.size()));
    value.append(payload.data(), payload.size());
    return value;
}

io::CacheContext build_meta_cache_context() {
    io::CacheContext context;
    context.cache_type = io::FileCacheType::INDEX;
    context.query_id = TUniqueId();
    context.expiration_time = 0;
    context.is_cold_data = false;
    context.is_warmup = false;
    return context;
}

void update_profile_counter(int64_t* counter, int64_t value = 1) {
    if (counter != nullptr) {
        *counter += value;
    }
}

Status read_cached_file_cache(io::BlockFileCache* cache, const io::UInt128Wrapper& hash,
                              size_t offset, Slice buffer) {
    if (buffer.size == 0) {
        return Status::OK();
    }

    auto blocks = cache->get_blocks_by_key(hash);
    Defer reset_owned_by_cached_reader {[&] {
        for (auto& [_, block] : blocks) {
            block->_owned_by_cached_reader = false;
        }
    }};
    if (blocks.empty()) {
        return Status::NotFound("file cache block not found, hash={}", hash.to_string());
    }

    const size_t right = offset + buffer.size - 1;
    size_t current_pos = offset;
    size_t written_size = 0;
    auto it = blocks.upper_bound(offset);
    if (it != blocks.begin()) {
        --it;
    }
    for (; it != blocks.end() && current_pos <= right; ++it) {
        const auto& block = it->second;
        const auto& range = block->range();
        if (range.right < current_pos) {
            continue;
        }
        if (range.left > current_pos) {
            return Status::NotFound("file cache block range has holes, hash={}", hash.to_string());
        }

        const size_t read_size = std::min(range.right, right) - current_pos + 1;
        const size_t read_offset = current_pos - range.left;
        RETURN_IF_ERROR(block->read(Slice(buffer.data + written_size, read_size), read_offset));
        written_size += read_size;
        current_pos += read_size;
    }
    if (current_pos <= right) {
        return Status::NotFound("file cache block range has holes, hash={}", hash.to_string());
    }
    return Status::OK();
}

} // namespace

FileMetaCache::FileMetaCache(int64_t capacity, io::BlockFileCache* block_file_cache)
        : _cache(capacity), _block_file_cache(block_file_cache) {}

std::string FileMetaCache::get_key(const std::string file_name, int64_t modification_time,
                                   int64_t file_size) {
    std::string meta_cache_key;
    meta_cache_key.resize(file_name.size() + sizeof(int64_t));

    memcpy(meta_cache_key.data(), file_name.data(), file_name.size());
    if (modification_time != 0) {
        memcpy(meta_cache_key.data() + file_name.size(), &modification_time, sizeof(int64_t));
    } else {
        memcpy(meta_cache_key.data() + file_name.size(), &file_size, sizeof(int64_t));
    }
    return meta_cache_key;
}

std::string FileMetaCache::get_key(io::FileReaderSPtr file_reader,
                                   const io::FileDescription& _file_description) {
    return FileMetaCache::get_key(
            file_reader->path().native(), _file_description.mtime,
            _file_description.file_size == -1 ? file_reader->size() : _file_description.file_size);
}

std::string FileMetaCache::get_persistent_cache_key(FileMetaCacheFormat format,
                                                    std::string_view file_meta_cache_key) {
    std::string key;
    key.reserve(32 + file_meta_cache_key.size());
    key.append("file_meta_cache:v1:");
    key.append(format_name(format));
    key.push_back(':');
    key.append(file_meta_cache_key.data(), file_meta_cache_key.size());
    return key;
}

FileMetaCacheLookupResult FileMetaCache::lookup(const FileMetaCacheContext& context,
                                                ObjLRUCache::CacheHandle* handle,
                                                std::string* serialized_meta,
                                                FileMetaCacheProfile* profile) {
    DCHECK(handle != nullptr);
    DCHECK(serialized_meta != nullptr);
    if (lookup(context.key, handle)) {
        serialized_meta->clear();
        if (profile != nullptr) {
            update_profile_counter(profile->hit_cache);
            update_profile_counter(profile->hit_memory_cache);
        }
        return {.state = FileMetaCacheLookupState::MEMORY_HIT};
    }

    FileMetaCacheLookupResult result;
    int64_t persisted_read_time = 0;
    if (lookup_persistent_cache(context, serialized_meta, &persisted_read_time)) {
        result.state = FileMetaCacheLookupState::PERSISTED_HIT;
        if (profile != nullptr) {
            update_profile_counter(profile->hit_cache);
            update_profile_counter(profile->hit_disk_cache);
            update_profile_counter(profile->read_disk_cache_time, persisted_read_time);
        }
    } else if (config::enable_external_file_meta_disk_cache && profile != nullptr) {
        update_profile_counter(profile->miss_disk_cache);
    }
    return result;
}

bool FileMetaCache::lookup_persistent_cache(const FileMetaCacheContext& context,
                                            std::string* payload, int64_t* read_time) {
    DCHECK(payload != nullptr);
    DCHECK(read_time != nullptr);
    payload->clear();
    *read_time = 0;
    if (!config::enable_external_file_meta_disk_cache) {
        return false;
    }

    MonotonicStopWatch watch;
    watch.start();
    auto stop_watch = [&]() { *read_time = watch.elapsed_time(); };

    const std::string disk_cache_key = get_persistent_cache_key(context.format, context.key);
    const auto hash = io::BlockFileCache::hash(disk_cache_key);
    io::BlockFileCache* cache = get_block_file_cache(hash);
    if (cache == nullptr) {
        stop_watch();
        return false;
    }

    auto invalidate_entry = [&](const Status& status) {
        payload->clear();
        cache->remove_if_cached(hash);
        VLOG_DEBUG << "lookup file meta disk cache failed: " << status;
        stop_watch();
        return false;
    };

    std::string header(FILE_META_CACHE_DISK_HEADER_SIZE, '\0');
    Status status = read_cached_file_cache(cache, hash, 0, Slice(header.data(), header.size()));
    if (!status.ok()) {
        VLOG_DEBUG << "lookup file meta disk cache failed: " << status;
        stop_watch();
        return false;
    }

    FileMetaCacheDiskHeader parsed;
    status = parse_disk_cache_header(header, &parsed);
    if (!status.ok()) {
        return invalidate_entry(status);
    }
    const auto max_entry_bytes =
            static_cast<uint64_t>(config::external_file_meta_disk_cache_max_entry_bytes);
    if (parsed.format != context.format || parsed.modification_time != context.modification_time ||
        parsed.file_size != context.file_size || parsed.payload_size > max_entry_bytes) {
        return invalidate_entry(Status::NotFound("file meta disk cache header mismatch"));
    }

    payload->resize(parsed.payload_size);
    if (parsed.payload_size > 0) {
        status = read_cached_file_cache(cache, hash, FILE_META_CACHE_DISK_HEADER_SIZE,
                                        Slice(payload->data(), payload->size()));
        if (!status.ok()) {
            payload->clear();
            VLOG_DEBUG << "lookup file meta disk cache failed: " << status;
            stop_watch();
            return false;
        }
    }
    const uint32_t checksum = crc32c::Crc32c(payload->data(), payload->size());
    if (checksum != parsed.checksum) {
        return invalidate_entry(Status::NotFound("file meta disk cache checksum mismatch"));
    }

    stop_watch();
    return true;
}

bool FileMetaCache::insert_persistent_cache(const FileMetaCacheContext& context,
                                            std::string_view payload, int64_t* write_time) {
    DCHECK(write_time != nullptr);
    *write_time = 0;
    if (!config::enable_external_file_meta_disk_cache ||
        payload.size() >
                static_cast<size_t>(config::external_file_meta_disk_cache_max_entry_bytes)) {
        return false;
    }

    MonotonicStopWatch watch;
    watch.start();
    auto stop_watch = [&]() { *write_time = watch.elapsed_time(); };

    const std::string disk_cache_key = get_persistent_cache_key(context.format, context.key);
    const auto hash = io::BlockFileCache::hash(disk_cache_key);
    io::BlockFileCache* cache = get_block_file_cache(hash);
    if (cache == nullptr) {
        stop_watch();
        return false;
    }

    const std::string value = build_disk_cache_value(context.format, context.modification_time,
                                                     context.file_size, payload);
    io::ReadStatistics stats;
    io::CacheContext cache_context = build_meta_cache_context();
    cache_context.stats = &stats;
    auto holder = cache->get_or_set(hash, 0, value.size(), cache_context);
    for (const auto& block : holder.file_blocks) {
        auto state = block->state();
        if (state == io::FileBlock::State::DOWNLOADING && !block->is_downloader()) {
            state = block->wait();
        }
        if (state == io::FileBlock::State::DOWNLOADED) {
            continue;
        }
        if (state != io::FileBlock::State::EMPTY) {
            VLOG_DEBUG << "insert file meta disk cache failed: file block is not writable";
            stop_watch();
            return false;
        }

        if (block->get_or_set_downloader() != io::FileBlock::get_caller_id()) {
            VLOG_DEBUG << "insert file meta disk cache failed: file block has another downloader";
            stop_watch();
            return false;
        }
        const auto& range = block->range();
        DCHECK_LT(range.right, value.size());
        Status status = block->append(Slice(value.data() + range.left, range.size()));
        if (!status.ok()) {
            VLOG_DEBUG << "insert file meta disk cache failed: " << status;
            stop_watch();
            return false;
        }
        status = block->finalize();
        if (!status.ok()) {
            VLOG_DEBUG << "insert file meta disk cache failed: " << status;
            stop_watch();
            return false;
        }
    }
    stop_watch();
    return true;
}

io::BlockFileCache* FileMetaCache::get_block_file_cache(const io::UInt128Wrapper& hash) const {
    if (_block_file_cache != nullptr) {
        return _block_file_cache;
    }
    io::FileCacheFactory* factory = io::FileCacheFactory::instance();
    if (factory == nullptr || factory->get_cache_instance_size() == 0) {
        return nullptr;
    }
    return factory->get_by_path(hash);
}

} // namespace doris
