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

import org.apache.doris.connector.api.ConnectorTableSnapshot;
import org.apache.doris.connector.api.DorisConnectorException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Path-only catalog adapter for a single configured Delta table.
 *
 * <p>This adapter deliberately has no table discovery service.  It exposes the
 * configured database/table pair and uses the Delta log at the configured path
 * as the source of snapshot metadata.  A Unity Catalog adapter can implement
 * the same {@link DeltaCatalogAdapter} contract later.</p>
 */
public class DeltaPathCatalogAdapter implements DeltaCatalogAdapter {

    private final String databaseName;
    private final String tableName;
    private final String tablePath;
    private final DeltaKernelSnapshotLoader snapshotLoader;

    public DeltaPathCatalogAdapter(String databaseName, String tableName,
            String tablePath, DeltaKernelSnapshotLoader snapshotLoader) {
        this.databaseName = requireNonBlank(databaseName, "databaseName");
        this.tableName = requireNonBlank(tableName, "tableName");
        this.tablePath = requireNonBlank(tablePath, "tablePath");
        this.snapshotLoader = snapshotLoader;
    }

    @Override
    public List<String> listDatabaseNames() {
        return Collections.singletonList(databaseName);
    }

    @Override
    public boolean databaseExists(String requestedDatabaseName) {
        return databaseName.equals(requestedDatabaseName);
    }

    @Override
    public List<String> listTableNames(String requestedDatabaseName) {
        if (!databaseExists(requestedDatabaseName)) {
            return Collections.emptyList();
        }
        return Collections.singletonList(tableName);
    }

    @Override
    public Optional<DeltaTableHandle> getTableHandle(
            String requestedDatabaseName, String requestedTableName) {
        if (!databaseName.equals(requestedDatabaseName)
                || !tableName.equals(requestedTableName)) {
            return Optional.empty();
        }
        DeltaKernelSnapshot snapshot = loadLatestSnapshot();
        return Optional.of(new DeltaTableHandle(databaseName, tableName, tablePath,
                snapshot.getVersion()).withPinnedSnapshot(snapshot));
    }

    @Override
    public DeltaKernelSnapshot loadSnapshot(DeltaTableHandle tableHandle) {
        validateHandle(tableHandle);
        if (tableHandle.getPinnedSnapshot() != null) {
            return tableHandle.getPinnedSnapshot();
        }
        try {
            return snapshotLoader.loadVersion(tablePath, tableHandle.getSnapshotVersion());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load Delta snapshot version " + tableHandle.getSnapshotVersion()
                            + " at '" + tablePath + "'", e);
        }
    }

    @Override
    public DeltaTableHandle applyTableSnapshot(
            DeltaTableHandle tableHandle, ConnectorTableSnapshot snapshot) {
        validateHandle(tableHandle);
        DeltaKernelSnapshot requested;
        try {
            requested = snapshot.getType() == ConnectorTableSnapshot.Type.VERSION
                    ? snapshotLoader.loadVersion(tablePath, snapshot.getValue())
                    : snapshotLoader.loadTimestamp(tablePath, snapshot.getValue());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load requested Delta snapshot at '" + tablePath + "'", e);
        }
        DeltaCatalogAdapter.requireCompatibleSchema(loadSnapshot(tableHandle), requested);
        return tableHandle.withSnapshotVersion(requested.getVersion())
                .withPinnedSnapshot(requested);
    }

    /** Loads the current latest snapshot for connectivity checks and handle creation. */
    public DeltaKernelSnapshot loadLatestSnapshot() {
        try {
            return snapshotLoader.loadLatest(tablePath);
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load latest Delta snapshot at '" + tablePath + "'", e);
        }
    }

    private void validateHandle(DeltaTableHandle tableHandle) {
        if (!databaseName.equals(tableHandle.getDatabaseName())
                || !tableName.equals(tableHandle.getTableName())
                || !tablePath.equals(tableHandle.getTablePath())) {
            throw new IllegalArgumentException(
                    "Table handle does not belong to this Delta path adapter");
        }
    }

    @Override
    public String testConnection() {
        DeltaKernelSnapshot snapshot = loadLatestSnapshot();
        return "Delta snapshot version " + snapshot.getVersion() + " is readable";
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
