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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaTableMetadata;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Read-only Unity Catalog adapter for managed and external Delta tables. */
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
        UnityDeltaStorageProperties.configureS3HadoopProperties(
                this.baseConfiguration, this.catalogProperties);
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
        Optional<DeltaLoadTableResponse> response = client.loadTable(
                catalogName, databaseName, tableName);
        if (response.isEmpty()) {
            return Optional.empty();
        }
        DeltaTableMetadata metadata = validatedMetadata(response.get(), databaseName, tableName);
        DeltaKernelSnapshot snapshot = loadLatestSnapshot(
                databaseName, tableName, metadata.getLocation());
        Long catalogVersion = response.get().getLatestTableVersion();
        if (catalogVersion != null && catalogVersion != snapshot.getVersion()) {
            throw new UnsupportedOperationException(
                    "Unity Catalog reports Delta version " + catalogVersion
                            + " but the storage log exposes version " + snapshot.getVersion()
                            + "; catalog-held log tails are not supported yet");
        }
        return Optional.of(new DeltaTableHandle(databaseName, tableName,
                metadata.getLocation(), snapshot.getVersion()));
    }

    @Override
    public DeltaKernelSnapshot loadSnapshot(DeltaTableHandle tableHandle) {
        DeltaTableMetadata metadata = resolveExistingTable(tableHandle);
        Configuration configuration = client.buildReadHadoopConfiguration(
                catalogName, tableHandle.getDatabaseName(), tableHandle.getTableName(),
                metadata.getLocation(), baseConfiguration);
        try {
            return new DeltaKernelSnapshotLoader(DefaultEngine.create(configuration))
                    .loadVersion(metadata.getLocation(), tableHandle.getSnapshotVersion());
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
                catalogProperties);
    }

    @Override
    public String testConnection() {
        int schemaCount = listDatabaseNames().size();
        return "Unity Catalog '" + catalogName + "' is readable; discovered "
                + schemaCount + " schema(s)";
    }

    private DeltaKernelSnapshot loadLatestSnapshot(
            String databaseName, String tableName, String location) {
        Configuration configuration = client.buildReadHadoopConfiguration(
                catalogName, databaseName, tableName, location, baseConfiguration);
        try {
            return new DeltaKernelSnapshotLoader(DefaultEngine.create(configuration))
                    .loadLatest(location);
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
        String catalogManaged = metadata.getProperties().get(CATALOG_MANAGED_PROPERTY);
        if ("supported".equalsIgnoreCase(catalogManaged)
                || (response.getCommits() != null && !response.getCommits().isEmpty())) {
            throw new UnsupportedOperationException(
                    "Catalog-managed Unity Delta tables require the UC catalog log tail; "
                            + "path-only snapshot loading is intentionally disabled");
        }
        return metadata;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
