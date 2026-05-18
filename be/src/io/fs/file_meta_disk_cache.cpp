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

#include "io/fs/file_meta_disk_cache.h"

#include <crc32c/crc32c.h>
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <cstring>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris {
namespace {

constexpr std::string_view FILE_META_DISK_CACHE_MAGIC = "DFMC";
constexpr uint8_t FILE_META_DISK_CACHE_VERSION = 1;
constexpr size_t FILE_META_DISK_CACHE_HEADER_SIZE = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;

std::string_view format_name(FileMetaDiskCacheFormat format) {
    switch (format) {
    case FileMetaDiskCacheFormat::PARQUET:
        return "parquet";
    case FileMetaDiskCacheFormat::ORC:
        return "orc";
    }
    DCHECK(false) << "unknown file meta disk cache format";
    return "unknown";
}

struct FileMetaDiskCacheHeader {
    FileMetaDiskCacheFormat format;
    int64_t modification_time = 0;
    int64_t file_size = 0;
    uint64_t payload_size = 0;
    uint32_t checksum = 0;
};

Status parse_header(std::string_view header, FileMetaDiskCacheHeader* parsed) {
    DCHECK(header.size() == FILE_META_DISK_CACHE_HEADER_SIZE);
    if (std::memcmp(header.data(), FILE_META_DISK_CACHE_MAGIC.data(),
                    FILE_META_DISK_CACHE_MAGIC.size()) != 0) {
        return Status::NotFound("file meta disk cache magic mismatch");
    }

    const uint8_t* ptr =
            reinterpret_cast<const uint8_t*>(header.data() + FILE_META_DISK_CACHE_MAGIC.size());
    const uint8_t version = *ptr++;
    if (version != FILE_META_DISK_CACHE_VERSION) {
        return Status::NotFound("file meta disk cache version mismatch");
    }

    parsed->format = static_cast<FileMetaDiskCacheFormat>(*ptr++);
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

std::string build_cache_value(FileMetaDiskCacheFormat format, int64_t modification_time,
                              int64_t file_size, std::string_view payload) {
    std::string value;
    value.reserve(FILE_META_DISK_CACHE_HEADER_SIZE + payload.size());
    value.append(FILE_META_DISK_CACHE_MAGIC.data(), FILE_META_DISK_CACHE_MAGIC.size());
    value.push_back(static_cast<char>(FILE_META_DISK_CACHE_VERSION));
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
    context.cache_type = io::FileCacheType::META;
    context.query_id = TUniqueId();
    context.expiration_time = 0;
    context.is_cold_data = false;
    context.is_warmup = false;
    return context;
}

} // namespace

std::string FileMetaDiskCache::get_key(FileMetaDiskCacheFormat format,
                                       std::string_view file_meta_cache_key) {
    std::string key;
    key.reserve(32 + file_meta_cache_key.size());
    key.append("file_meta_cache:v1:");
    key.append(format_name(format));
    key.push_back(':');
    key.append(file_meta_cache_key.data(), file_meta_cache_key.size());
    return key;
}

Status FileMetaDiskCache::read(FileMetaDiskCacheFormat format,
                               const std::string& file_meta_cache_key, int64_t modification_time,
                               int64_t file_size, std::string* payload) const {
    payload->clear();
    const std::string cache_key = get_key(format, file_meta_cache_key);
    const auto hash = io::BlockFileCache::hash(cache_key);
    io::BlockFileCache* cache = get_cache(hash);
    if (cache == nullptr) {
        return Status::NotFound("file meta disk cache is not available");
    }

    io::ReadStatistics stats;
    io::CacheContext context = build_meta_cache_context();
    context.stats = &stats;

    std::string header(FILE_META_DISK_CACHE_HEADER_SIZE, '\0');
    RETURN_IF_ERROR(cache->read_if_cached(hash, 0, Slice(header.data(), header.size()), context));

    FileMetaDiskCacheHeader parsed;
    RETURN_IF_ERROR(parse_header(header, &parsed));
    if (parsed.format != format || parsed.modification_time != modification_time ||
        parsed.file_size != file_size ||
        parsed.payload_size >
                static_cast<uint64_t>(config::external_file_meta_disk_cache_max_entry_bytes)) {
        return Status::NotFound("file meta disk cache header mismatch");
    }

    payload->resize(parsed.payload_size);
    if (parsed.payload_size > 0) {
        RETURN_IF_ERROR(cache->read_if_cached(hash, FILE_META_DISK_CACHE_HEADER_SIZE,
                                              Slice(payload->data(), payload->size()), context));
    }
    const uint32_t checksum = crc32c::Crc32c(payload->data(), payload->size());
    if (checksum != parsed.checksum) {
        payload->clear();
        return Status::NotFound("file meta disk cache checksum mismatch");
    }
    return Status::OK();
}

Status FileMetaDiskCache::write(FileMetaDiskCacheFormat format,
                                const std::string& file_meta_cache_key, int64_t modification_time,
                                int64_t file_size, std::string_view payload) const {
    if (payload.size() >
        static_cast<size_t>(config::external_file_meta_disk_cache_max_entry_bytes)) {
        return Status::NotFound("file meta disk cache payload is too large");
    }

    const std::string cache_key = get_key(format, file_meta_cache_key);
    const auto hash = io::BlockFileCache::hash(cache_key);
    io::BlockFileCache* cache = get_cache(hash);
    if (cache == nullptr) {
        return Status::NotFound("file meta disk cache is not available");
    }

    const std::string value = build_cache_value(format, modification_time, file_size, payload);
    io::ReadStatistics stats;
    io::CacheContext context = build_meta_cache_context();
    context.stats = &stats;
    auto holder = cache->get_or_set(hash, 0, value.size(), context);
    for (const auto& block : holder.file_blocks) {
        auto state = block->state();
        if (state == io::FileBlock::State::DOWNLOADING && !block->is_downloader()) {
            state = block->wait();
        }
        if (state == io::FileBlock::State::DOWNLOADED) {
            continue;
        }
        if (state != io::FileBlock::State::EMPTY) {
            return Status::NotFound("file meta disk cache block is not writable");
        }

        if (block->get_or_set_downloader() != io::FileBlock::get_caller_id()) {
            return Status::NotFound("file meta disk cache block has another downloader");
        }
        const auto& range = block->range();
        DCHECK_LT(range.right, value.size());
        RETURN_IF_ERROR(block->append(Slice(value.data() + range.left, range.size())));
        RETURN_IF_ERROR(block->finalize());
    }
    return Status::OK();
}

io::BlockFileCache* FileMetaDiskCache::get_cache(const io::UInt128Wrapper& hash) const {
    if (_cache != nullptr) {
        return _cache;
    }
    io::FileCacheFactory* factory = io::FileCacheFactory::instance();
    if (factory == nullptr || factory->get_cache_instance_size() == 0) {
        return nullptr;
    }
    return factory->get_by_path(hash);
}

} // namespace doris
