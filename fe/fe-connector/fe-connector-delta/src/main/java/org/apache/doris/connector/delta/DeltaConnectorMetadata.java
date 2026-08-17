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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.NamedColumnHandle;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Metadata facade backed by a single path-based Delta catalog adapter. */
public final class DeltaConnectorMetadata implements ConnectorMetadata {

    private final DeltaCatalogAdapter catalogAdapter;
    private final Map<String, String> properties;

    public DeltaConnectorMetadata(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties) {
        this.catalogAdapter = catalogAdapter;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return catalogAdapter.listDatabaseNames();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        return catalogAdapter.databaseExists(dbName);
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return catalogAdapter.listTableNames(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        return catalogAdapter.getTableHandle(dbName, tableName)
                .map(handle -> (ConnectorTableHandle) handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        List<ConnectorColumn> columns = toColumns(snapshot.getSchema());
        Map<String, String> tableProperties = new LinkedHashMap<>();
        tableProperties.put("location", snapshot.getTablePath());
        tableProperties.put("delta.snapshot.version", String.valueOf(snapshot.getVersion()));
        return new ConnectorTableSchema(deltaHandle.getTableName(), columns,
                "DELTA", tableProperties);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        StructType schema = catalogAdapter.loadSnapshot(deltaHandle).getSchema();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
        for (StructField field : schema.fields()) {
            handles.put(field.getName(), new NamedColumnHandle(field.getName()));
        }
        return handles;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    private static List<ConnectorColumn> toColumns(StructType schema) {
        List<ConnectorColumn> columns = new ArrayList<>(schema.length());
        for (StructField field : schema.fields()) {
            columns.add(new ConnectorColumn(field.getName(),
                    DeltaTypeMapping.fromDeltaType(field.getDataType()), "",
                    field.isNullable(), null));
        }
        return columns;
    }
}
