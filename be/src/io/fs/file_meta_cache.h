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

#pragma once

#include <memory>
#include <mutex>

#include "io/file_factory.h"
#include "io/fs/file_meta_disk_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/obj_lru_cache.h"

namespace doris {

// A file meta cache depends on a LRU cache.
// Such as parsed parquet footer.
// The capacity will limit the number of cache entries in cache.
class FileMetaCache {
public:
    FileMetaCache(int64_t capacity);

    FileMetaCache(const FileMetaCache&) = delete;
    const FileMetaCache& operator=(const FileMetaCache&) = delete;

    ObjLRUCache& cache() { return _cache; }

    static std::string get_key(const std::string file_name, int64_t modification_time,
                               int64_t file_size);

    static std::string get_key(io::FileReaderSPtr file_reader,
                               const io::FileDescription& _file_description);

    bool lookup(const std::string& key, ObjLRUCache::CacheHandle* handle) {
        return _cache.lookup({key}, handle);
    }

    template <typename T>
    void insert(const std::string& key, T* value, ObjLRUCache::CacheHandle* handle) {
        _cache.insert({key}, value, handle);
    }

    bool enabled() const { return _cache.enabled(); }

    bool should_enable_memory_cache(int64_t num_scan_ranges) const;
    bool should_enable_for_reader(int64_t num_scan_ranges) const;

    bool lookup_disk_cache(FileMetaDiskCacheFormat format, const std::string& key,
                           int64_t modification_time, int64_t file_size, std::string* payload);

    bool insert_disk_cache(FileMetaDiskCacheFormat format, const std::string& key,
                           int64_t modification_time, int64_t file_size, std::string_view payload);

private:
    FileMetaDiskCache* disk_cache();

    ObjLRUCache _cache;
    std::mutex _disk_cache_mutex;
    std::unique_ptr<FileMetaDiskCache> _disk_cache;
};

} // namespace doris
