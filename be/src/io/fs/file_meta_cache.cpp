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

#include "common/config.h"
#include "common/logging.h"

namespace doris {

FileMetaCache::FileMetaCache(int64_t capacity) : _cache(capacity) {}

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

bool FileMetaCache::lookup_disk_cache(FileMetaDiskCacheFormat format, const std::string& key,
                                      int64_t modification_time, int64_t file_size,
                                      std::string* payload) {
    FileMetaDiskCache* cache = disk_cache();
    if (cache == nullptr) {
        return false;
    }
    Status st = cache->read(format, key, modification_time, file_size, payload);
    if (!st.ok()) {
        VLOG_DEBUG << "lookup file meta disk cache failed: " << st;
        return false;
    }
    return true;
}

bool FileMetaCache::insert_disk_cache(FileMetaDiskCacheFormat format, const std::string& key,
                                      int64_t modification_time, int64_t file_size,
                                      std::string_view payload) {
    FileMetaDiskCache* cache = disk_cache();
    if (cache == nullptr) {
        return false;
    }
    Status st = cache->write(format, key, modification_time, file_size, payload);
    if (!st.ok()) {
        VLOG_DEBUG << "insert file meta disk cache failed: " << st;
        return false;
    }
    return true;
}

FileMetaDiskCache* FileMetaCache::disk_cache() {
    if (!config::enable_external_file_meta_disk_cache) {
        return nullptr;
    }
    std::lock_guard lock(_disk_cache_mutex);
    if (_disk_cache == nullptr) {
        _disk_cache = std::make_unique<FileMetaDiskCache>();
    }
    return _disk_cache.get();
}

} // namespace doris
