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

package org.apache.doris.connector.delta;

import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;

import java.util.List;

/** A consistent Delta table snapshot and its active data files. */
public class DeltaKernelSnapshot {
    private final String tablePath;
    private final long version;
    private final StructType schema;
    private final List<FileStatus> activeFiles;

    public DeltaKernelSnapshot(String tablePath, long version, StructType schema, List<FileStatus> activeFiles) {
        this.tablePath = tablePath;
        this.version = version;
        this.schema = schema;
        this.activeFiles = List.copyOf(activeFiles);
    }

    public String getTablePath() {
        return tablePath;
    }

    public long getVersion() {
        return version;
    }

    public StructType getSchema() {
        return schema;
    }

    public List<FileStatus> getActiveFiles() {
        return activeFiles;
    }
}
