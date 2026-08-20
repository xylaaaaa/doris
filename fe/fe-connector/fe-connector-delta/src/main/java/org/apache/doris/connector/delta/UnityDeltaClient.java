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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.delta.kernel.Snapshot;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedCommitter;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/** Thin wrapper around the official Unity Catalog Java client. */
final class UnityDeltaClient {
    private static final int PAGE_SIZE = 1000;
    private static final String APP_NAME = "Apache-Doris";
    private static final String APP_VERSION = "native-delta";
    private static final Map<String, String> APP_VERSIONS =
            Map.of(APP_NAME, APP_VERSION);

    private final String workspaceUri;
    private final TokenProvider tokenProvider;
    private final ApiClient apiClient;
    private final SchemasApi schemasApi;
    private final DeltaTablesApi deltaTablesApi;
    private final DeltaTemporaryCredentialsApi credentialsApi;

    static UnityDeltaClient create(String workspaceUri, String token) {
        return create(workspaceUri, Map.of("type", "static", "token", token));
    }

    static UnityDeltaClient create(Map<String, String> properties) {
        String workspaceUri = requireProperty(properties, DeltaConnectorProperties.UNITY_URI);
        long connectTimeoutMs = DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_CONNECT_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_CONNECT_TIMEOUT_MS);
        long readTimeoutMs = DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_READ_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_READ_TIMEOUT_MS);
        String authType = properties.getOrDefault(
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "pat").trim().toLowerCase(
                        java.util.Locale.ROOT);
        Map<String, String> authProperties;
        if ("pat".equals(authType)) {
            authProperties = Map.of("type", "static",
                    "token", requireProperty(properties, DeltaConnectorProperties.UNITY_TOKEN));
        } else if ("oauth".equals(authType)) {
            authProperties = Map.of("type", "oauth",
                    "oauth.uri", requireProperty(properties, DeltaConnectorProperties.UNITY_OAUTH_URI),
                    "oauth.clientId", requireProperty(
                            properties, DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID),
                    "oauth.clientSecret", requireProperty(
                            properties, DeltaConnectorProperties.UNITY_OAUTH_CLIENT_SECRET));
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Unity authentication type '" + authType + "'");
        }
        return create(workspaceUri, authProperties, connectTimeoutMs, readTimeoutMs);
    }

    private static UnityDeltaClient create(String workspaceUri,
            Map<String, String> authProperties) {
        return create(workspaceUri, authProperties,
                DeltaConnectorProperties.DEFAULT_UNITY_CONNECT_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_READ_TIMEOUT_MS);
    }

    private static UnityDeltaClient create(String workspaceUri,
            Map<String, String> authProperties, long connectTimeoutMs, long readTimeoutMs) {
        String normalizedUri = stripTrailingSlash(workspaceUri);
        TokenProvider tokenProvider = TokenProvider.create(authProperties);
        ApiClient apiClient = ApiClientBuilder.create()
                .uri(normalizedUri)
                .tokenProvider(tokenProvider)
                .addAppVersion(APP_NAME, APP_VERSION)
                .build();
        apiClient.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        apiClient.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return new UnityDeltaClient(normalizedUri, tokenProvider, apiClient);
    }

    private static String requireProperty(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required Unity property '" + key + "'");
        }
        return value;
    }

    UnityDeltaClient(String workspaceUri, TokenProvider tokenProvider, ApiClient apiClient) {
        this.workspaceUri = workspaceUri;
        this.tokenProvider = tokenProvider;
        this.apiClient = apiClient;
        registerCredentialConfigDeserializer(apiClient);
        this.schemasApi = new SchemasApi(apiClient);
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
                JsonNode response = listTablesWithManifestCapabilities(
                        catalogName, schemaName, pageToken);
                JsonNode tables = response.get("tables");
                if (tables == null || !tables.isArray()) {
                    throw new ApiException("Unity Catalog list tables response has no tables array");
                }
                for (JsonNode table : tables) {
                    if (isReadableDeltaTable(table)) {
                        tableNames.add(table.get("name").asText());
                    }
                }
                JsonNode nextPageToken = response.get("next_page_token");
                pageToken = nextPageToken == null || nextPageToken.isNull()
                        ? null : nextPageToken.asText();
            } catch (ApiException e) {
                throw requestFailure("list tables in '" + catalogName + "." + schemaName + "'", e);
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Unity Catalog request failed while attempting to list tables in '"
                                + catalogName + "." + schemaName + "'", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DorisConnectorException(
                        "Unity Catalog request was interrupted while attempting to list tables in '"
                                + catalogName + "." + schemaName + "'", e);
            }
        } while (pageToken != null && !pageToken.isEmpty());
        return tableNames;
    }

    private JsonNode listTablesWithManifestCapabilities(
            String catalogName, String schemaName, String pageToken)
            throws IOException, InterruptedException, ApiException {
        StringBuilder uri = new StringBuilder(apiClient.getBaseUri()).append("/tables?")
                .append("catalog_name=").append(ApiClient.urlEncode(catalogName))
                .append("&schema_name=").append(ApiClient.urlEncode(schemaName))
                .append("&max_results=").append(PAGE_SIZE)
                .append("&include_manifest_capabilities=true");
        if (pageToken != null && !pageToken.isEmpty()) {
            uri.append("&page_token=").append(ApiClient.urlEncode(pageToken));
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(uri.toString()))
                .header("Accept", "application/json");
        if (apiClient.getReadTimeout() != null) {
            request.timeout(apiClient.getReadTimeout());
        }
        Consumer<HttpRequest.Builder> interceptor = apiClient.getRequestInterceptor();
        if (interceptor != null) {
            interceptor.accept(request);
        }
        HttpResponse<String> response = apiClient.getHttpClient().send(
                request.GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new ApiException(response.statusCode(), response.body());
        }
        return apiClient.getObjectMapper().readTree(response.body());
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
        return buildHadoopConfiguration(catalogName, schemaName, tableName, location,
                baseConfiguration, UCCredentialHadoopConfs.TableOperation.READ);
    }

    Configuration buildWriteHadoopConfiguration(String catalogName, String schemaName,
            String tableName, String location, Configuration baseConfiguration) {
        return buildHadoopConfiguration(catalogName, schemaName, tableName, location,
                baseConfiguration, UCCredentialHadoopConfs.TableOperation.READ_WRITE);
    }

    private Configuration buildHadoopConfiguration(String catalogName, String schemaName,
            String tableName, String location, Configuration baseConfiguration,
            UCCredentialHadoopConfs.TableOperation operation) {
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
                    .addAppVersions(APP_VERSIONS)
                    .buildForTable(catalogName, schemaName, tableName,
                            operation, location);
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
        return getCredentials(catalogName, schemaName, tableName, DeltaCredentialOperation.READ);
    }

    DeltaCredentialsResponse getWriteCredentials(
            String catalogName, String schemaName, String tableName) {
        return getCredentials(catalogName, schemaName, tableName,
                DeltaCredentialOperation.READ_WRITE);
    }

    private DeltaCredentialsResponse getCredentials(
            String catalogName, String schemaName, String tableName,
            DeltaCredentialOperation operation) {
        try {
            return credentialsApi.getTableCredentials(
                    operation, catalogName, schemaName, tableName);
        } catch (ApiException e) {
            throw requestFailure(
                    "vend backend " + operation.getValue() + " credentials for Delta table '"
                            + catalogName + "."
                            + schemaName + "." + tableName + "'", e);
        }
    }

    Snapshot loadCatalogManagedSnapshot(Engine engine, String tableId, String tablePath,
            String catalogName, String schemaName, String tableName,
            Optional<Long> version) throws IOException {
        UCTableIdentifier tableIdentifier =
                new UCTableIdentifier(catalogName, schemaName, tableName);
        try (UCDeltaTokenBasedRestClient catalogClient =
                new UCDeltaTokenBasedRestClient(
                        workspaceUri, tokenProvider, APP_VERSIONS)) {
            return new NamedUCCatalogManagedClient(catalogClient, tableIdentifier).loadSnapshot(
                    engine, tableId, tablePath, tableIdentifier,
                    version, Optional.empty());
        }
    }

    CatalogManagedSnapshot openCatalogManagedSnapshot(Engine engine, String tableId,
            String tablePath, String catalogName, String schemaName, String tableName,
            Optional<Long> version) throws IOException {
        UCDeltaTokenBasedRestClient catalogClient = new UCDeltaTokenBasedRestClient(
                workspaceUri, tokenProvider, APP_VERSIONS);
        UCTableIdentifier tableIdentifier =
                new UCTableIdentifier(catalogName, schemaName, tableName);
        try {
            Snapshot snapshot = new NamedUCCatalogManagedClient(
                    catalogClient, tableIdentifier).loadSnapshot(
                    engine, tableId, tablePath, tableIdentifier, version, Optional.empty());
            return new CatalogManagedSnapshot(snapshot, catalogClient);
        } catch (RuntimeException e) {
            try {
                catalogClient.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    static final class CatalogManagedSnapshot implements AutoCloseable {
        private final Snapshot snapshot;
        private final UCDeltaTokenBasedRestClient catalogClient;

        private CatalogManagedSnapshot(Snapshot snapshot,
                UCDeltaTokenBasedRestClient catalogClient) {
            this.snapshot = snapshot;
            this.catalogClient = catalogClient;
        }

        Snapshot getSnapshot() {
            return snapshot;
        }

        @Override
        public void close() throws IOException {
            catalogClient.close();
        }
    }

    /** Keeps the table identifier on the committer used by name-based Unity REST updates. */
    private static final class NamedUCCatalogManagedClient extends UCCatalogManagedClient {
        private final UCTableIdentifier tableIdentifier;

        private NamedUCCatalogManagedClient(
                UCClient catalogClient, UCTableIdentifier tableIdentifier) {
            super(catalogClient);
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        protected Committer createUCCommitter(
                UCClient catalogClient, String tableId, String tablePath) {
            return new UCCatalogManagedCommitter(
                    catalogClient, tableId, tablePath, tableIdentifier);
        }
    }

    private static boolean isReadableDeltaTable(JsonNode table) {
        String format = textValue(table, "data_source_format");
        String tableType = textValue(table, "table_type");
        if (!"DELTA".equalsIgnoreCase(format)
                || (!"MANAGED".equalsIgnoreCase(tableType)
                && !"EXTERNAL".equalsIgnoreCase(tableType))) {
            return false;
        }
        JsonNode capabilities = table.get("manifest_capabilities");
        if (capabilities == null) {
            capabilities = table.get("manifest-capabilities");
        }
        Set<String> capabilityNames = capabilityNames(capabilities);
        return capabilityNames.isEmpty()
                || capabilityNames.contains("HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT");
    }

    private static String textValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static Set<String> capabilityNames(JsonNode capabilities) {
        if (capabilities == null || capabilities.isNull()) {
            return Set.of();
        }
        Set<String> names = new HashSet<>();
        if (capabilities.isArray()) {
            for (JsonNode capability : capabilities) {
                if (capability.isTextual()) {
                    names.add(capability.asText());
                }
            }
        } else if (capabilities.isObject()) {
            capabilities.fields().forEachRemaining(entry -> {
                if (entry.getValue().asBoolean(false)) {
                    names.add(entry.getKey());
                }
            });
        }
        return names;
    }

    static String credentialRegion(DeltaStorageCredentialConfig config) {
        if (config instanceof DeltaStorageCredentialConfigWithRegion) {
            return ((DeltaStorageCredentialConfigWithRegion) config).getRegion();
        }
        return null;
    }

    private static void registerCredentialConfigDeserializer(ApiClient apiClient) {
        ObjectMapper mapper = apiClient.getObjectMapper();
        SimpleModule module = new SimpleModule("doris-delta-credential-config");
        module.addDeserializer(DeltaStorageCredentialConfig.class,
                new DeltaStorageCredentialConfigDeserializer());
        mapper.registerModule(module);
        apiClient.setObjectMapper(mapper);
    }

    private static final class DeltaStorageCredentialConfigDeserializer
            extends StdDeserializer<DeltaStorageCredentialConfig> {
        private DeltaStorageCredentialConfigDeserializer() {
            super(DeltaStorageCredentialConfig.class);
        }

        @Override
        public DeltaStorageCredentialConfig deserialize(JsonParser parser,
                com.fasterxml.jackson.databind.DeserializationContext context) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            DeltaStorageCredentialConfig config = new DeltaStorageCredentialConfig()
                    .s3AccessKeyId(text(node, "s3.access-key-id"))
                    .s3SecretAccessKey(text(node, "s3.secret-access-key"))
                    .s3SessionToken(text(node, "s3.session-token"))
                    .azureSasToken(text(node, "azure.sas-token"))
                    .gcsOauthToken(text(node, "gcs.oauth-token"));
            String region = firstNonBlank(node, "client.region", "s3.region", "aws.region");
            if (region == null) {
                return config;
            }
            return new DeltaStorageCredentialConfigWithRegion(config, region);
        }

        private static String text(JsonNode node, String key) {
            JsonNode value = node.get(key);
            return value == null || value.isNull() ? null : value.asText();
        }

        private static String firstNonBlank(JsonNode node, String... keys) {
            for (String key : keys) {
                String value = text(node, key);
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
            return null;
        }
    }

    private static final class DeltaStorageCredentialConfigWithRegion
            extends DeltaStorageCredentialConfig {
        private final String region;

        private DeltaStorageCredentialConfigWithRegion(
                DeltaStorageCredentialConfig config, String region) {
            s3AccessKeyId(config.getS3AccessKeyId());
            s3SecretAccessKey(config.getS3SecretAccessKey());
            s3SessionToken(config.getS3SessionToken());
            azureSasToken(config.getAzureSasToken());
            gcsOauthToken(config.getGcsOauthToken());
            this.region = region;
        }

        private String getRegion() {
            return region;
        }
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
