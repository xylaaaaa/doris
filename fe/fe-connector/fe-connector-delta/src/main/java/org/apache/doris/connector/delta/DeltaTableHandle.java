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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Objects;

/** Serializable coordinates for a Delta table pinned to one snapshot version. */
public final class DeltaTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final String tableName;
    private final String tablePath;
    private final long snapshotVersion;

    public DeltaTableHandle(String databaseName, String tableName,
            String tablePath, long snapshotVersion) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath");
        if (snapshotVersion < 0) {
            throw new IllegalArgumentException("Delta snapshot version must be non-negative");
        }
        this.snapshotVersion = snapshotVersion;
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

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DeltaTableHandle)) {
            return false;
        }
        DeltaTableHandle that = (DeltaTableHandle) other;
        return snapshotVersion == that.snapshotVersion
                && databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName)
                && tablePath.equals(that.tablePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tablePath, snapshotVersion);
    }

    @Override
    public String toString() {
        return "DeltaTableHandle{" + databaseName + "." + tableName
                + ", path=" + tablePath + ", version=" + snapshotVersion + "}";
    }
}
