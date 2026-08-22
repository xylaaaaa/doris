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
import org.apache.doris.connector.api.ConnectorTableCreateRequest;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableSnapshot;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.NamedColumnHandle;
import org.apache.doris.connector.api.write.ConnectorFileCommitInfo;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Metadata facade backed by a Delta catalog adapter. */
public final class DeltaConnectorMetadata implements ConnectorMetadata {

    private static final String IN_COMMIT_TIMESTAMPS = "delta.enableInCommitTimestamps";

    private final DeltaCatalogAdapter catalogAdapter;
    private final Map<String, String> properties;
    private final DeltaKernelWriter writer;
    private final boolean writeEnabled;

    public DeltaConnectorMetadata(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties) {
        this(catalogAdapter, properties, null);
    }

    public DeltaConnectorMetadata(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties, DeltaKernelWriter writer) {
        this.catalogAdapter = catalogAdapter;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.writer = writer;
        this.writeEnabled = Boolean.parseBoolean(
                properties.getOrDefault(DeltaConnectorProperties.WRITE_ENABLED, "false"));
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
    public ConnectorTableHandle applyTableSnapshot(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorTableSnapshot snapshot) {
        return catalogAdapter.applyTableSnapshot((DeltaTableHandle) handle, snapshot);
    }

    @Override
    public boolean createTable(
            ConnectorSession session, ConnectorTableCreateRequest request) {
        requireWriteEnabled();
        if (!catalogAdapter.supportsCreateTable()) {
            throw new UnsupportedOperationException(
                    "This native Delta catalog adapter does not support CREATE TABLE");
        }
        for (ConnectorColumn column : request.getTableSchema().getColumns()) {
            if (column.getDefaultValue() != null) {
                throw new UnsupportedOperationException(
                        "Native Delta CREATE TABLE does not support column defaults");
            }
        }
        return catalogAdapter.createTable(request);
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

    @Override
    public boolean supportsInsert() {
        return writeEnabled && (writer != null || catalogAdapter.supportsInsert());
    }

    @Override
    public boolean supportsInsertOverwrite() {
        return writeEnabled && (writer != null || catalogAdapter.supportsOverwrite());
    }

    @Override
    public boolean supportsDelete() {
        return supportsInsertOverwrite();
    }

    @Override
    public boolean supportsUpdate() {
        return supportsInsertOverwrite();
    }

    @Override
    public ConnectorWriteConfig getWriteConfig(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        if (!deltaHandle.isExternalTable() && !deltaHandle.isCatalogManaged()) {
            throw new UnsupportedOperationException(
                    "Ordinary Unity managed Delta writes are not supported; "
                            + "the table must enable the catalogManaged feature");
        }
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        if (deltaHandle.isCatalogManaged()) {
            if (!Boolean.parseBoolean(snapshot.getTableProperties().get(IN_COMMIT_TIMESTAMPS))) {
                throw new UnsupportedOperationException(
                        "Catalog-managed Delta writes require " + IN_COMMIT_TIMESTAMPS + "=true");
            }
            Set<String> supportedCatalogManagedWriterFeatures =
                    Set.of("catalogManaged", "vacuumProtocolCheck");
            if (snapshot.getMinWriterVersion() != 7
                    || !supportedCatalogManagedWriterFeatures.containsAll(
                    snapshot.getWriterFeatures())) {
                throw new UnsupportedOperationException(
                        "The initial catalog-managed Delta writer supports only catalogManaged and "
                                + "vacuumProtocolCheck "
                                + "writer protocol; table requires minWriterVersion="
                                + snapshot.getMinWriterVersion() + ", writerFeatures="
                                + snapshot.getWriterFeatures());
            }
        } else if (snapshot.getMinWriterVersion() > 2 || !snapshot.getWriterFeatures().isEmpty()) {
            throw new UnsupportedOperationException(
                    "The initial native Delta writer supports only baseline writer protocol; "
                            + "table requires minWriterVersion=" + snapshot.getMinWriterVersion()
                            + ", writerFeatures=" + snapshot.getWriterFeatures());
        }
        List<ConnectorColumn> tableColumns = toColumns(snapshot.getSchema());
        if (!hasSameWriteSchema(tableColumns, columns)) {
            throw new UnsupportedOperationException(
                    "The initial native Delta writer requires every table column in schema order; "
                            + "expected " + tableColumns + " but received " + columns);
        }
        Map<String, String> storageProperties = getBackendStoragePropertiesForWrite(deltaHandle);
        DeltaVendedCredentialLifetime.validateWrite(session, storageProperties,
                DeltaConnectorProperties.positiveLongProperty(properties,
                        DeltaConnectorProperties.UNITY_CREDENTIAL_MIN_LIFETIME_MS,
                        DeltaConnectorProperties.DEFAULT_UNITY_CREDENTIAL_MIN_LIFETIME_MS));
        return ConnectorWriteConfig.builder(ConnectorWriteType.FILE_WRITE)
                .fileFormat("parquet")
                .compression("snappy")
                .writeLocation(deltaHandle.getTablePath())
                .partitionColumns(snapshot.getPartitionColumnNames())
                .properties(storageProperties)
                .build();
    }

    @Override
    public ConnectorInsertHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        String applicationId = session == null ? null : session.getQueryId();
        if (writer != null) {
            return writer.beginInsert(deltaHandle, applicationId);
        }
        return catalogAdapter.beginInsert(deltaHandle, applicationId);
    }

    @Override
    public ConnectorInsertHandle beginInsertOverwrite(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        requireWriteEnabled();
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        String applicationId = session == null ? null : session.getQueryId();
        if (writer != null) {
            return writer.beginOverwrite(deltaHandle, snapshot, applicationId);
        }
        return catalogAdapter.beginOverwrite(deltaHandle, applicationId);
    }

    @Override
    public void finishFileInsert(ConnectorSession session, ConnectorInsertHandle handle,
            Collection<ConnectorFileCommitInfo> files) {
        requireWriteEnabled();
        DeltaInsertHandle deltaHandle = (DeltaInsertHandle) handle;
        deltaHandle.getWriter().finishInsert(deltaHandle, files);
    }

    @Override
    public void abortInsert(ConnectorSession session, ConnectorInsertHandle handle) {
        if (handle instanceof DeltaInsertHandle) {
            ((DeltaInsertHandle) handle).getWriter().abortInsert((DeltaInsertHandle) handle);
        }
    }

    private void requireWriteEnabled() {
        if (!supportsInsert()) {
            throw new UnsupportedOperationException(
                    "Native Delta INSERT requires delta.write.enabled=true and a supported "
                            + "external or catalog-managed table");
        }
    }

    private Map<String, String> getBackendStoragePropertiesForWrite(
            DeltaTableHandle tableHandle) {
        Map<String, String> storageProperties = new LinkedHashMap<>();
        storageProperties.putAll(DeltaStorageProperties.toBackendProperties(properties));
        storageProperties.putAll(catalogAdapter.getBackendStoragePropertiesForWrite(tableHandle));
        return storageProperties;
    }

    private static boolean hasSameWriteSchema(List<ConnectorColumn> tableColumns,
            List<ConnectorColumn> insertColumns) {
        if (tableColumns.size() != insertColumns.size()) {
            return false;
        }
        for (int i = 0; i < tableColumns.size(); i++) {
            ConnectorColumn expected = tableColumns.get(i);
            ConnectorColumn actual = insertColumns.get(i);
            if (!expected.getName().equals(actual.getName())
                    || !hasCompatibleWriteType(expected.getType(), actual.getType())
                    || expected.isNullable() != actual.isNullable()) {
                return false;
            }
        }
        return true;
    }

    static boolean hasCompatibleWriteType(ConnectorType expected, ConnectorType actual) {
        if (expected.equals(actual)) {
            return true;
        }
        if (isDecimalV3(expected.getTypeName()) && isDecimalV3(actual.getTypeName())) {
            return decimalPrecisionMatchesStorageWidth(expected.getPrecision(), actual)
                    && parameterMatches(expected.getPrecision(), actual.getPrecision())
                    && parameterMatches(expected.getScale(), actual.getScale());
        }
        if (!expected.getTypeName().equals(actual.getTypeName())
                || !parameterMatches(expected.getPrecision(), actual.getPrecision())
                || !parameterMatches(expected.getScale(), actual.getScale())
                || !expected.getFieldNames().equals(actual.getFieldNames())
                || expected.getChildren().size() != actual.getChildren().size()) {
            return false;
        }
        for (int i = 0; i < expected.getChildren().size(); i++) {
            if (!hasCompatibleWriteType(expected.getChildren().get(i), actual.getChildren().get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDecimalV3(String typeName) {
        return "DECIMALV3".equals(typeName) || "DECIMAL32".equals(typeName)
                || "DECIMAL64".equals(typeName) || "DECIMAL128".equals(typeName)
                || "DECIMAL256".equals(typeName);
    }

    private static boolean decimalPrecisionMatchesStorageWidth(
            int expectedPrecision, ConnectorType actual) {
        if (expectedPrecision < 0 || actual.getPrecision() >= 0
                || "DECIMALV3".equals(actual.getTypeName())) {
            return true;
        }
        switch (actual.getTypeName()) {
            case "DECIMAL32":
                return expectedPrecision <= 9;
            case "DECIMAL64":
                return expectedPrecision >= 10 && expectedPrecision <= 18;
            case "DECIMAL128":
                return expectedPrecision >= 19 && expectedPrecision <= 38;
            case "DECIMAL256":
                return expectedPrecision >= 39 && expectedPrecision <= 76;
            default:
                return false;
        }
    }

    private static boolean parameterMatches(int expected, int actual) {
        return expected < 0 || actual < 0 || expected == actual;
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
