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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Plans native Parquet file ranges from a pinned Delta snapshot. */
public final class DeltaScanPlanProvider implements ConnectorScanPlanProvider {
    private final DeltaCatalogAdapter catalogAdapter;
    private final Map<String, String> properties;

    public DeltaScanPlanProvider(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties) {
        this.catalogAdapter = catalogAdapter;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot((DeltaTableHandle) handle);
        List<ConnectorScanRange> ranges = new ArrayList<>(snapshot.getActiveFiles().size());
        for (DeltaScanFile file : snapshot.getActiveFiles()) {
            ranges.add(new DeltaScanRange(file));
        }
        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot((DeltaTableHandle) handle);
        Map<String, String> scanProperties = new LinkedHashMap<>();
        scanProperties.put("file_format_type", "parquet");
        if (!snapshot.getPartitionColumnNames().isEmpty()) {
            scanProperties.put("path_partition_keys",
                    String.join(",", snapshot.getPartitionColumnNames()));
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (isBackendStorageProperty(entry.getKey())) {
                scanProperties.put("location." + entry.getKey(), entry.getValue());
            }
        }
        return scanProperties;
    }

    private static boolean isBackendStorageProperty(String key) {
        return key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")
                || key.startsWith("hive.") || key.startsWith("s3.") || key.startsWith("s3a.")
                || key.startsWith("cos.") || key.startsWith("oss.") || key.startsWith("obs.")
                || key.startsWith("azure.") || key.startsWith("adls.")
                || key.startsWith("gcs.") || key.startsWith("google.") || key.equals("uri");
    }
}
