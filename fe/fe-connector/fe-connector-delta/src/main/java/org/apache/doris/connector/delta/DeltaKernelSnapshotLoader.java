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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Loads a filesystem-managed Delta table snapshot through Delta Kernel. */
public class DeltaKernelSnapshotLoader {
    private static final String COLUMN_MAPPING_MODE = "delta.columnMapping.mode";
    private static final String CATALOG_MANAGED_FEATURE = "catalogManaged";
    private static final Set<String> UNSUPPORTED_DIRECT_PARQUET_READER_FEATURES = Set.of(
            "columnMapping",
            "deletionVectors",
            "geospatial",
            "typeWidening",
            "typeWidening-preview",
            "variantShredding",
            "variantShredding-preview",
            "variantType",
            "variantType-preview");

    private final Engine engine;

    public DeltaKernelSnapshotLoader(Engine engine) {
        this.engine = engine;
    }

    public DeltaKernelSnapshot load(String tablePath) throws IOException {
        Snapshot snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine);
        return loadSnapshot(snapshot, false);
    }

    public DeltaKernelSnapshot loadLatest(String tablePath) throws IOException {
        return load(tablePath);
    }

    public DeltaKernelSnapshot load(String tablePath, long version) throws IOException {
        Snapshot snapshot = Table.forPath(engine, tablePath).getSnapshotAsOfVersion(engine, version);
        return loadSnapshot(snapshot, false);
    }

    public DeltaKernelSnapshot loadVersion(String tablePath, long version) throws IOException {
        return load(tablePath, version);
    }

    DeltaKernelSnapshot loadCatalogManagedSnapshot(Snapshot snapshot) throws IOException {
        return loadSnapshot(snapshot, true);
    }

    private DeltaKernelSnapshot loadSnapshot(
            Snapshot snapshot, boolean catalogManagedRead) throws IOException {
        validateSupportedTableFeatures(snapshot, catalogManagedRead);
        Protocol protocol = ((SnapshotImpl) snapshot).getProtocol();
        Scan scan = snapshot.getScanBuilder().build();
        List<DeltaScanFile> activeFiles = new ArrayList<>();
        List<String> partitionColumns = snapshot.getPartitionColumnNames();

        try (CloseableIterator<FilteredColumnarBatch> batches = scan.getScanFiles(engine)) {
            while (batches.hasNext()) {
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        // Kernel's public Scan API returns nested scan rows. Keep this
                        // version-specific extraction in the adapter so the rest of the
                        // connector does not depend on Kernel's internal package.
                        DeletionVectorDescriptor deletionVector =
                                InternalScanFileUtils.getDeletionVectorDescriptorFromRow(row);
                        if (deletionVector != null) {
                            throw new UnsupportedOperationException(
                                    "Delta deletion vectors are not supported by the initial Doris connector");
                        }

                        FileStatus file = InternalScanFileUtils.getAddFileStatus(row);
                        if (file.getSize() <= 0) {
                            throw new IllegalArgumentException(
                                    "Delta data file has an invalid size: " + file.getPath());
                        }
                        activeFiles.add(new DeltaScanFile(
                                file.getPath(), file.getSize(), file.getModificationTime(),
                                orderedPartitionValues(row, partitionColumns)));
                    }
                }
            }
        }
        activeFiles.sort(Comparator.comparing(DeltaScanFile::getPath));
        return new DeltaKernelSnapshot(snapshot.getPath(), snapshot.getVersion(),
                snapshot.getSchema(), partitionColumns, snapshot.getTableProperties(),
                protocol.getMinWriterVersion(), protocol.getWriterFeatures(), activeFiles);
    }

    private static Map<String, String> orderedPartitionValues(Row row,
            List<String> partitionColumns) {
        Map<String, String> values = InternalScanFileUtils.getPartitionValues(row);
        Map<String, String> orderedValues = new LinkedHashMap<>();
        for (String column : partitionColumns) {
            if (!values.containsKey(column)) {
                throw new IllegalArgumentException(
                        "Delta add-file is missing partition value for column " + column);
            }
            orderedValues.put(column, values.get(column));
        }
        return orderedValues;
    }

    private static void validateSupportedTableFeatures(
            Snapshot snapshot, boolean catalogManagedRead) {
        Set<String> readerFeatures = ((SnapshotImpl) snapshot).getProtocol().getReaderFeatures();
        if (readerFeatures.contains(CATALOG_MANAGED_FEATURE) && !catalogManagedRead) {
            throw new UnsupportedOperationException(
                    "Catalog-managed Delta tables must be loaded through a catalog-aware adapter");
        }

        Set<String> unsupportedFeatures = new TreeSet<>(readerFeatures);
        unsupportedFeatures.retainAll(UNSUPPORTED_DIRECT_PARQUET_READER_FEATURES);
        if (!unsupportedFeatures.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Delta reader features require physical-row transforms that are not supported "
                            + "by the Doris native Parquet scan: " + unsupportedFeatures);
        }

        String mappingMode = snapshot.getTableProperties().getOrDefault(COLUMN_MAPPING_MODE, "none");
        if (!"none".equalsIgnoreCase(mappingMode)) {
            throw new UnsupportedOperationException(
                    "Delta column mapping mode is not supported by the initial Doris connector: "
                            + mappingMode);
        }
    }
}
