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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** One full-file Parquet split from a pinned Delta snapshot. */
public final class DeltaScanRange implements ConnectorScanRange {
    private static final long serialVersionUID = 1L;

    private final String path;
    private final long size;
    private final long modificationTime;
    private final Map<String, String> partitionValues;

    public DeltaScanRange(DeltaScanFile file) {
        this.path = file.getPath();
        this.size = file.getSize();
        this.modificationTime = file.getModificationTime();
        this.partitionValues = file.getPartitionValues();
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of(path);
    }

    @Override
    public long getLength() {
        return size;
    }

    @Override
    public long getFileSize() {
        return size;
    }

    @Override
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public String getFileFormat() {
        return "parquet";
    }

    @Override
    public String getTableFormatType() {
        return "delta";
    }

    @Override
    public List<String> getHosts() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc,
            TFileRangeDesc rangeDesc) {
        rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        if (!partitionValues.isEmpty()) {
            List<String> keys = new ArrayList<>(partitionValues.size());
            List<String> values = new ArrayList<>(partitionValues.size());
            List<Boolean> isNull = new ArrayList<>(partitionValues.size());
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                keys.add(entry.getKey());
                values.add(entry.getValue() == null ? "\\N" : entry.getValue());
                isNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(keys);
            rangeDesc.setColumnsFromPath(values);
            rangeDesc.setColumnsFromPathIsNull(isNull);
        }
    }
}
