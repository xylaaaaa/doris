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
#include <gen_cpp/file_cache.pb.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

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

constexpr uint8_t FILE_META_CACHE_DISK_VERSION = 1;
constexpr size_t FILE_META_CACHE_DISK_ENVELOPE_HEADER_SIZE = 1 + sizeof(uint64_t);
constexpr size_t FILE_META_CACHE_DISK_CHECKSUM_SIZE = sizeof(uint32_t);
constexpr uint64_t FILE_META_CACHE_DISK_PROTO_OVERHEAD_LIMIT = 128;

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

Status parse_disk_cache_envelope_header(std::string_view header, uint64_t* proto_size) {
    DCHECK(header.size() == FILE_META_CACHE_DISK_ENVELOPE_HEADER_SIZE);
    const auto* ptr = reinterpret_cast<const uint8_t*>(header.data());
    const uint8_t version = *ptr++;
    if (version != FILE_META_CACHE_DISK_VERSION) {
        return Status::NotFound("file meta disk cache version mismatch");
    }

    *proto_size = decode_fixed64_le(ptr);
    return Status::OK();
}

Status parse_disk_cache_format(uint32_t format, FileMetaCacheFormat* parsed) {
    if (format == static_cast<uint32_t>(FileMetaCacheFormat::PARQUET)) {
        *parsed = FileMetaCacheFormat::PARQUET;
        return Status::OK();
    }
    if (format == static_cast<uint32_t>(FileMetaCacheFormat::ORC)) {
        *parsed = FileMetaCacheFormat::ORC;
        return Status::OK();
    }
    return Status::NotFound("file meta disk cache format mismatch");
}

bool is_persistent_cache_proto_size_allowed(uint64_t proto_size) {
    const int64_t max_entry_bytes = config::external_file_meta_disk_cache_max_entry_bytes;
    return max_entry_bytes > 0 && proto_size > 0 &&
           proto_size <= static_cast<uint64_t>(std::numeric_limits<int>::max()) &&
           std::cmp_less_equal(proto_size, static_cast<uint64_t>(max_entry_bytes) +
                                                   FILE_META_CACHE_DISK_PROTO_OVERHEAD_LIMIT);
}

Status build_disk_cache_value(FileMetaCacheFormat format, int64_t modification_time,
                              int64_t file_size, std::string_view payload, std::string* value) {
    io::cache::FileMetaCacheDiskEntryPb entry;
    entry.set_format(static_cast<uint32_t>(format));
    entry.set_modification_time(modification_time);
    entry.set_file_size(file_size);
    entry.set_payload(payload.data(), payload.size());

    std::string serialized_entry;
    if (!entry.SerializeToString(&serialized_entry)) {
        return Status::InternalError("failed to serialize file meta disk cache entry");
    }

    value->clear();
    value->reserve(FILE_META_CACHE_DISK_ENVELOPE_HEADER_SIZE + serialized_entry.size() +
                   FILE_META_CACHE_DISK_CHECKSUM_SIZE);
    value->push_back(static_cast<char>(FILE_META_CACHE_DISK_VERSION));
    put_fixed64_le(value, serialized_entry.size());
    value->append(serialized_entry);
    put_fixed32_le(value, crc32c::Crc32c(serialized_entry.data(), serialized_entry.size()));
    return Status::OK();
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
                              size_t offset, Slice buffer,
                              std::vector<io::FileBlockSPtr>* read_blocks = nullptr) {
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
        if (read_blocks != nullptr) {
            read_blocks->push_back(block);
        }
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

std::string FileMetaCache::get_key(const std::string& file_name, int64_t modification_time,
                                   int64_t file_size) {
    std::string meta_cache_key;
    meta_cache_key.reserve(sizeof(uint64_t) + file_name.size() + sizeof(int64_t) * 2);
    put_fixed64_le(&meta_cache_key, file_name.size());
    meta_cache_key.append(file_name);
    put_fixed64_le(&meta_cache_key, static_cast<uint64_t>(modification_time));
    put_fixed64_le(&meta_cache_key, static_cast<uint64_t>(file_size));
    return meta_cache_key;
}

std::string FileMetaCache::get_key(io::FileReaderSPtr file_reader,
                                   const io::FileDescription& _file_description) {
    const std::string& file_path = file_reader->path().native();
    std::string file_identity;
    if (_file_description.fs_name.empty()) {
        file_identity = file_path;
    } else {
        file_identity.reserve(_file_description.fs_name.size() + 1 + file_path.size());
        file_identity.append(_file_description.fs_name);
        file_identity.push_back('\0');
        file_identity.append(file_path);
    }
    return FileMetaCache::get_key(
            file_identity, _file_description.mtime,
            _file_description.file_size == -1 ? file_reader->size() : _file_description.file_size);
}

bool FileMetaCache::is_persistent_cache_enabled() {
    const int64_t max_entry_bytes = config::external_file_meta_disk_cache_max_entry_bytes;
    return config::enable_external_file_meta_disk_cache && max_entry_bytes > 0;
}

bool FileMetaCache::is_persistent_cache_payload_size_allowed(uint64_t payload_size) {
    const int64_t max_entry_bytes = config::external_file_meta_disk_cache_max_entry_bytes;
    return config::enable_external_file_meta_disk_cache && max_entry_bytes > 0 &&
           std::cmp_less_equal(payload_size, max_entry_bytes);
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
    if (context.enable_memory_cache && lookup(context.key, handle)) {
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
    } else if (is_persistent_cache_enabled() && profile != nullptr) {
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
    if (!is_persistent_cache_enabled()) {
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

    std::vector<io::FileBlockSPtr> read_blocks;
    auto invalidate_entry = [&](const Status& status) {
        payload->clear();
        read_blocks.clear();
        cache->remove_if_cached(hash);
        VLOG_DEBUG << "lookup file meta disk cache failed: " << status;
        stop_watch();
        return false;
    };

    std::string envelope_header(FILE_META_CACHE_DISK_ENVELOPE_HEADER_SIZE, '\0');
    Status status = read_cached_file_cache(
            cache, hash, 0, Slice(envelope_header.data(), envelope_header.size()), &read_blocks);
    if (!status.ok()) {
        VLOG_DEBUG << "lookup file meta disk cache failed: " << status;
        stop_watch();
        return false;
    }

    uint64_t proto_size = 0;
    status = parse_disk_cache_envelope_header(envelope_header, &proto_size);
    if (!status.ok()) {
        return invalidate_entry(status);
    }
    if (!is_persistent_cache_proto_size_allowed(proto_size)) {
        return invalidate_entry(Status::NotFound("file meta disk cache proto size is invalid"));
    }

    std::string proto_and_checksum(proto_size + FILE_META_CACHE_DISK_CHECKSUM_SIZE, '\0');
    status = read_cached_file_cache(cache, hash, FILE_META_CACHE_DISK_ENVELOPE_HEADER_SIZE,
                                    Slice(proto_and_checksum.data(), proto_and_checksum.size()),
                                    &read_blocks);
    if (!status.ok()) {
        return invalidate_entry(status);
    }

    const auto* checksum_ptr =
            reinterpret_cast<const uint8_t*>(proto_and_checksum.data() + proto_size);
    const uint32_t checksum = decode_fixed32_le(checksum_ptr);
    if (checksum != crc32c::Crc32c(proto_and_checksum.data(), proto_size)) {
        return invalidate_entry(Status::NotFound("file meta disk cache checksum mismatch"));
    }

    io::cache::FileMetaCacheDiskEntryPb entry;
    if (!entry.ParseFromArray(proto_and_checksum.data(), static_cast<int>(proto_size))) {
        return invalidate_entry(Status::NotFound("file meta disk cache protobuf parse failed"));
    }
    FileMetaCacheFormat parsed_format;
    status = parse_disk_cache_format(entry.format(), &parsed_format);
    if (!status.ok()) {
        return invalidate_entry(status);
    }
    if (!entry.has_format() || !entry.has_modification_time() || !entry.has_file_size() ||
        !entry.has_payload() || parsed_format != context.format ||
        entry.modification_time() != context.modification_time ||
        entry.file_size() != context.file_size ||
        !is_persistent_cache_payload_size_allowed(entry.payload().size())) {
        return invalidate_entry(Status::NotFound("file meta disk cache entry mismatch"));
    }
    payload->assign(entry.payload());

    for (auto& block : read_blocks) {
        cache->add_need_update_lru_block(std::move(block));
    }
    stop_watch();
    return true;
}

bool FileMetaCache::insert_persistent_cache(const FileMetaCacheContext& context,
                                            std::string_view payload, int64_t* write_time) {
    DCHECK(write_time != nullptr);
    *write_time = 0;
    if (!is_persistent_cache_payload_size_allowed(static_cast<uint64_t>(payload.size()))) {
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

    std::string value;
    Status status = build_disk_cache_value(context.format, context.modification_time,
                                           context.file_size, payload, &value);
    if (!status.ok()) {
        VLOG_DEBUG << "insert file meta disk cache failed: " << status;
        stop_watch();
        return false;
    }
    io::ReadStatistics stats;
    io::CacheContext cache_context = build_meta_cache_context();
    cache_context.stats = &stats;
    status = cache->set(hash, value, cache_context);
    if (!status.ok()) {
        VLOG_DEBUG << "insert file meta disk cache failed: " << status;
        stop_watch();
        return false;
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
