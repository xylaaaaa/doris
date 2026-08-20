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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaTableMetadata;
import io.unitycatalog.client.delta.model.DeltaTableType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Unity Catalog adapter for Delta reads and supported append writes. */
final class UnityDeltaCatalogAdapter implements DeltaCatalogAdapter {
    private static final String CATALOG_MANAGED_PROPERTY = "delta.feature.catalogManaged";

    private final String catalogName;
    private final UnityDeltaClient client;
    private final Configuration baseConfiguration;
    private final Map<String, String> catalogProperties;

    UnityDeltaCatalogAdapter(String catalogName, UnityDeltaClient client,
            Configuration baseConfiguration, Map<String, String> catalogProperties) {
        this.catalogName = requireNonBlank(catalogName, "catalogName");
        this.client = client;
        this.baseConfiguration = new Configuration(baseConfiguration);
        this.catalogProperties = Collections.unmodifiableMap(
                new LinkedHashMap<>(catalogProperties));
        DeltaStorageProperties.configureHadoop(this.baseConfiguration, this.catalogProperties);
    }

    @Override
    public List<String> listDatabaseNames() {
        return client.listSchemas(catalogName);
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return listDatabaseNames().contains(databaseName);
    }

    @Override
    public List<String> listTableNames(String databaseName) {
        if (!databaseExists(databaseName)) {
            return List.of();
        }
        return client.listDeltaTables(catalogName, databaseName);
    }

    @Override
    public Optional<DeltaTableHandle> getTableHandle(String databaseName, String tableName) {
        // Unity only vends external-engine credentials for tables advertising this capability.
        // Apply the same gate here as in listTableNames so a direct name lookup cannot bypass it.
        if (!client.listDeltaTables(catalogName, databaseName).contains(tableName)) {
            return Optional.empty();
        }
        Optional<DeltaLoadTableResponse> response = client.loadTable(
                catalogName, databaseName, tableName);
        if (response.isEmpty()) {
            return Optional.empty();
        }
        DeltaLoadTableResponse loadResponse = response.get();
        DeltaTableMetadata metadata = validatedMetadata(loadResponse, databaseName, tableName);
        boolean catalogManaged = isCatalogManaged(loadResponse);
        String tableId = metadata.getTableUuid() == null
                ? null : metadata.getTableUuid().toString();
        if (catalogManaged && tableId == null) {
            throw new DorisConnectorException(
                    "Catalog-managed Unity Delta table has no table UUID: "
                            + catalogName + "." + databaseName + "." + tableName);
        }
        DeltaKernelSnapshot snapshot = loadInitialSnapshot(
                databaseName, tableName, tableId, metadata.getLocation(), loadResponse,
                catalogManaged);
        Long catalogVersion = response.get().getLatestTableVersion();
        if (catalogVersion != null && catalogVersion != snapshot.getVersion()) {
            throw new DorisConnectorException(
                    "Unity Catalog reports Delta version " + catalogVersion
                            + " but the resolved snapshot is version " + snapshot.getVersion()
                            + "; refusing to plan an inconsistent table state");
        }
        return Optional.of(new DeltaTableHandle(databaseName, tableName,
                metadata.getLocation(), snapshot.getVersion(), tableId, catalogManaged,
                metadata.getTableType() == DeltaTableType.EXTERNAL));
    }

    @Override
    public DeltaKernelSnapshot loadSnapshot(DeltaTableHandle tableHandle) {
        DeltaTableMetadata metadata = resolveExistingTable(tableHandle);
        Configuration configuration = client.buildReadHadoopConfiguration(
                catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName(),
                metadata.getLocation(), baseConfiguration);
        Engine engine = DefaultEngine.create(configuration);
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(engine);
        try {
            if (tableHandle.isCatalogManaged()) {
                Snapshot snapshot = client.loadCatalogManagedSnapshot(
                        engine, tableHandle.getCatalogTableId(), metadata.getLocation(),
                        catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName(),
                        Optional.of(tableHandle.getSnapshotVersion()));
                return loader.loadCatalogManagedSnapshot(snapshot);
            }
            return loader.loadVersion(
                    metadata.getLocation(), tableHandle.getSnapshotVersion());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load Unity Delta snapshot version "
                            + tableHandle.getSnapshotVersion() + " for '" + catalogName + "."
                            + tableHandle.getDatabaseName() + "." + tableHandle.getTableName() + "'", e);
        }
    }

    @Override
    public Map<String, String> getBackendStorageProperties(DeltaTableHandle tableHandle) {
        DeltaTableMetadata metadata = resolveExistingTable(tableHandle);
        if ("file".equalsIgnoreCase(URI.create(metadata.getLocation()).getScheme())) {
            return Map.of();
        }
        return UnityDeltaStorageProperties.toBackendProperties(
                metadata.getLocation(), client.getReadCredentials(
                        catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName()),
                catalogProperties, DeltaCredentialOperation.READ);
    }

    @Override
    public Map<String, String> getBackendStoragePropertiesForWrite(
            DeltaTableHandle tableHandle) {
        DeltaTableMetadata metadata = resolveExistingTable(tableHandle);
        if ("file".equalsIgnoreCase(URI.create(metadata.getLocation()).getScheme())) {
            return Map.of();
        }
        return UnityDeltaStorageProperties.toBackendProperties(
                metadata.getLocation(), client.getWriteCredentials(
                        catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName()),
                catalogProperties, DeltaCredentialOperation.READ_WRITE);
    }

    @Override
    public ConnectorInsertHandle beginInsert(DeltaTableHandle tableHandle) {
        return beginInsert(tableHandle, null);
    }

    @Override
    public ConnectorInsertHandle beginInsert(DeltaTableHandle tableHandle,
            String applicationId) {
        if (!tableHandle.isCatalogManaged() && tableHandle.isExternalTable()) {
            DeltaTableMetadata metadata = resolveExistingTable(tableHandle);
            Configuration configuration = client.buildWriteHadoopConfiguration(
                    catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName(),
                    metadata.getLocation(), baseConfiguration);
            return new DeltaKernelWriter(
                    io.delta.kernel.defaults.engine.DefaultEngine.create(configuration))
                    .beginInsert(tableHandle, applicationId);
        }
        if (!tableHandle.isCatalogManaged()) {
            throw new UnsupportedOperationException(
                    "Ordinary Unity managed Delta INSERT is not supported; "
                            + "enable the catalogManaged table feature");
        }
        DeltaTableMetadata metadata = resolveExistingTable(tableHandle);
        Configuration configuration = client.buildWriteHadoopConfiguration(
                catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName(),
                metadata.getLocation(), baseConfiguration);
        Engine engine = io.delta.kernel.defaults.engine.DefaultEngine.create(configuration);
        UnityDeltaClient.CatalogManagedSnapshot openSnapshot;
        try {
            openSnapshot = client.openCatalogManagedSnapshot(
                    engine, tableHandle.getCatalogTableId(), metadata.getLocation(),
                    catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName(),
                    Optional.empty());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to open Unity catalog-managed Delta transaction for '" + catalogName
                            + "." + tableHandle.getDatabaseName() + "."
                            + tableHandle.getTableName() + "'", e);
        }
        return new DeltaKernelWriter(engine).beginCatalogManagedInsert(
                tableHandle, openSnapshot.getSnapshot(), applicationId, openSnapshot);
    }

    @Override
    public boolean supportsInsert() {
        return Boolean.parseBoolean(catalogProperties.getOrDefault(
                DeltaConnectorProperties.WRITE_ENABLED, "false"));
    }

    @Override
    public String testConnection() {
        int schemaCount = listDatabaseNames().size();
        return "Unity Catalog '" + catalogName + "' is readable; discovered "
                + schemaCount + " schema(s)";
    }

    private DeltaKernelSnapshot loadInitialSnapshot(String databaseName, String tableName,
            String tableId, String location, DeltaLoadTableResponse response,
            boolean catalogManaged) {
        Configuration configuration = client.buildReadHadoopConfiguration(
                catalogName, databaseName, tableName, location, baseConfiguration);
        Engine engine = DefaultEngine.create(configuration);
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(engine);
        try {
            if (catalogManaged) {
                Long latestVersion = response.getLatestTableVersion();
                if (latestVersion == null) {
                    throw new DorisConnectorException(
                            "Catalog-managed Unity Delta response has no latest table version for '"
                                    + catalogName + "." + databaseName + "." + tableName + "'");
                }
                Snapshot snapshot = client.loadCatalogManagedSnapshot(
                        engine, tableId, location, catalogName, databaseName, tableName,
                        Optional.of(latestVersion));
                return loader.loadCatalogManagedSnapshot(snapshot);
            }
            return loader.loadLatest(location);
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to load latest Unity Delta snapshot for '" + catalogName + "."
                            + databaseName + "." + tableName + "'", e);
        }
    }

    private DeltaTableMetadata resolveExistingTable(DeltaTableHandle tableHandle) {
        DeltaLoadTableResponse response = client.loadTable(
                catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName())
                .orElseThrow(() -> new DorisConnectorException(
                        "Unity Delta table no longer exists: " + catalogName + "."
                                + tableHandle.getDatabaseName() + "." + tableHandle.getTableName()));
        DeltaTableMetadata metadata = validatedMetadata(
                response, tableHandle.getDatabaseName(), tableHandle.getTableName());
        if (!tableHandle.getTablePath().equals(metadata.getLocation())) {
            throw new DorisConnectorException(
                    "Unity Delta table location changed while planning '" + catalogName + "."
                            + tableHandle.getDatabaseName() + "." + tableHandle.getTableName() + "'");
        }
        String currentTableId = metadata.getTableUuid() == null
                ? null : metadata.getTableUuid().toString();
        if (tableHandle.getCatalogTableId() != null
                && !tableHandle.getCatalogTableId().equals(currentTableId)) {
            throw new DorisConnectorException(
                    "Unity Delta table identity changed while planning '" + catalogName + "."
                            + tableHandle.getDatabaseName() + "." + tableHandle.getTableName() + "'");
        }
        if (tableHandle.isCatalogManaged() != isCatalogManaged(response)
                || tableHandle.isExternalTable() != (metadata.getTableType() == DeltaTableType.EXTERNAL)) {
            throw new DorisConnectorException(
                    "Unity Delta table management mode changed while planning '" + catalogName + "."
                            + tableHandle.getDatabaseName() + "." + tableHandle.getTableName() + "'");
        }
        return metadata;
    }

    private static DeltaTableMetadata validatedMetadata(DeltaLoadTableResponse response,
            String databaseName, String tableName) {
        DeltaTableMetadata metadata = response.getMetadata();
        if (metadata == null) {
            throw new DorisConnectorException(
                    "Unity Delta load response has no metadata for "
                            + databaseName + "." + tableName);
        }
        String location = metadata.getLocation();
        if (location == null || !URI.create(location).isAbsolute()) {
            throw new DorisConnectorException(
                    "Unity Delta load response has no absolute storage location for "
                            + databaseName + "." + tableName);
        }
        if (metadata.getTableType() != DeltaTableType.MANAGED
                && metadata.getTableType() != DeltaTableType.EXTERNAL) {
            throw new DorisConnectorException(
                    "Unity Delta load response has no supported table type for "
                            + databaseName + "." + tableName);
        }
        return metadata;
    }

    private static boolean isCatalogManaged(DeltaLoadTableResponse response) {
        Map<String, String> properties = response.getMetadata().getProperties();
        String catalogManaged = properties == null ? null : properties.get(CATALOG_MANAGED_PROPERTY);
        return "supported".equalsIgnoreCase(catalogManaged)
                || (response.getCommits() != null && !response.getCommits().isEmpty());
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
