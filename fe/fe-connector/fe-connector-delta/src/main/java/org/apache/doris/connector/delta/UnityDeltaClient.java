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

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Thin wrapper around the official Unity Catalog Java client. */
final class UnityDeltaClient {
    private static final int PAGE_SIZE = 1000;

    private final String workspaceUri;
    private final TokenProvider tokenProvider;
    private final ApiClient apiClient;
    private final SchemasApi schemasApi;
    private final TablesApi tablesApi;
    private final DeltaTablesApi deltaTablesApi;
    private final DeltaTemporaryCredentialsApi credentialsApi;

    static UnityDeltaClient create(String workspaceUri, String token) {
        String normalizedUri = stripTrailingSlash(workspaceUri);
        TokenProvider tokenProvider = TokenProvider.create(
                Map.of("type", "static", "token", token));
        ApiClient apiClient = ApiClientBuilder.create()
                .uri(normalizedUri)
                .tokenProvider(tokenProvider)
                .addAppVersion("Apache-Doris", "native-delta")
                .build();
        return new UnityDeltaClient(normalizedUri, tokenProvider, apiClient);
    }

    UnityDeltaClient(String workspaceUri, TokenProvider tokenProvider, ApiClient apiClient) {
        this.workspaceUri = workspaceUri;
        this.tokenProvider = tokenProvider;
        this.apiClient = apiClient;
        this.schemasApi = new SchemasApi(apiClient);
        this.tablesApi = new TablesApi(apiClient);
        this.deltaTablesApi = new DeltaTablesApi(apiClient);
        this.credentialsApi = new DeltaTemporaryCredentialsApi(apiClient);
    }

    List<String> listSchemas(String catalogName) {
        List<String> schemaNames = new ArrayList<>();
        String pageToken = null;
        do {
            try {
                ListSchemasResponse response = schemasApi.listSchemas(
                        catalogName, PAGE_SIZE, pageToken);
                for (SchemaInfo schema : response.getSchemas()) {
                    schemaNames.add(schema.getName());
                }
                pageToken = response.getNextPageToken();
            } catch (ApiException e) {
                throw requestFailure("list schemas in catalog '" + catalogName + "'", e);
            }
        } while (pageToken != null && !pageToken.isEmpty());
        return schemaNames;
    }

    List<String> listDeltaTables(String catalogName, String schemaName) {
        List<String> tableNames = new ArrayList<>();
        String pageToken = null;
        do {
            try {
                ListTablesResponse response = tablesApi.listTables(
                        catalogName, schemaName, PAGE_SIZE, pageToken);
                for (TableInfo table : response.getTables()) {
                    if (isReadableDeltaTable(table)) {
                        tableNames.add(table.getName());
                    }
                }
                pageToken = response.getNextPageToken();
            } catch (ApiException e) {
                throw requestFailure("list tables in '" + catalogName + "." + schemaName + "'", e);
            }
        } while (pageToken != null && !pageToken.isEmpty());
        return tableNames;
    }

    Optional<DeltaLoadTableResponse> loadTable(
            String catalogName, String schemaName, String tableName) {
        try {
            return Optional.of(deltaTablesApi.loadTable(catalogName, schemaName, tableName));
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return Optional.empty();
            }
            throw requestFailure(
                    "load Delta table '" + catalogName + "." + schemaName + "." + tableName + "'", e);
        }
    }

    Configuration buildReadHadoopConfiguration(String catalogName, String schemaName,
            String tableName, String location, Configuration baseConfiguration) {
        Configuration configuration = new Configuration(baseConfiguration);
        String scheme = storageScheme(location);
        if ("file".equals(scheme)) {
            return configuration;
        }
        try {
            Map<String, String> credentialProperties = UCCredentialHadoopConfs
                    .builder(workspaceUri, scheme)
                    .tokenProvider(tokenProvider)
                    .apiClient(apiClient)
                    .enableCredentialRenewal(true)
                    .enableCredentialScopedFs(true)
                    .hadoopConf(configuration)
                    .addAppVersions(Map.of("Apache-Doris", "native-delta"))
                    .buildForTable(catalogName, schemaName, tableName,
                            UCCredentialHadoopConfs.TableOperation.READ, location);
            credentialProperties.forEach(configuration::set);
            return configuration;
        } catch (ApiException e) {
            throw requestFailure(
                    "vend read credentials for Delta table '" + catalogName + "."
                            + schemaName + "." + tableName + "'", e);
        }
    }

    DeltaCredentialsResponse getReadCredentials(
            String catalogName, String schemaName, String tableName) {
        try {
            return credentialsApi.getTableCredentials(
                    DeltaCredentialOperation.READ, catalogName, schemaName, tableName);
        } catch (ApiException e) {
            throw requestFailure(
                    "vend backend read credentials for Delta table '" + catalogName + "."
                            + schemaName + "." + tableName + "'", e);
        }
    }

    private static boolean isReadableDeltaTable(TableInfo table) {
        return table.getDataSourceFormat() == DataSourceFormat.DELTA
                && (table.getTableType() == TableType.MANAGED
                || table.getTableType() == TableType.EXTERNAL);
    }

    private static String storageScheme(String location) {
        String scheme = URI.create(location).getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a Delta location without a URI scheme");
        }
        if ("s3a".equalsIgnoreCase(scheme)) {
            return "s3";
        }
        switch (scheme.toLowerCase(java.util.Locale.ROOT)) {
            case "file":
            case "s3":
            case "gs":
            case "abfs":
            case "abfss":
                return scheme.toLowerCase(java.util.Locale.ROOT);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Unity Delta storage scheme: " + scheme);
        }
    }

    private static DorisConnectorException requestFailure(String operation, ApiException cause) {
        return new DorisConnectorException(
                "Unity Catalog request failed while attempting to " + operation
                        + " (HTTP " + cause.getCode() + ")", cause);
    }

    private static String stripTrailingSlash(String value) {
        String normalized = value;
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
