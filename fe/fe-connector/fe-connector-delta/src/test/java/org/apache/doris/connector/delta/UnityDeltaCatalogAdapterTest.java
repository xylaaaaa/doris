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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorFileCommitInfo;
import org.apache.doris.connector.spi.ConnectorContext;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UnityDeltaCatalogAdapterTest {
    private static final String TEST_TOKEN = "test-unity-token-never-log";

    private HttpServer server;
    private String workspaceUri;
    private String tableLocation;
    private String catalogManagedLocation;
    private String catalogManagedTableId;
    private final List<String> requestPaths = new ArrayList<>();
    private final List<String> requestQueries = new ArrayList<>();
    private final List<String> updateRequestPaths = new ArrayList<>();

    @TempDir
    private Path tempDirectory;

    @BeforeEach
    public void startServer() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/path_table"));
        tableLocation = Paths.get(fixture.toURI()).toUri().toString();
        URL catalogManagedFixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/catalog_managed_table"));
        catalogManagedLocation = Paths.get(catalogManagedFixture.toURI()).toUri().toString();
        catalogManagedTableId = "c79de738-d13c-44a5-8e75-8435123d60c7";
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handleRequest);
        server.start();
        workspaceUri = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void testUnityCatalogDiscoveryAndPinnedPathSnapshot() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN);

        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        provider.validateProperties(properties);
        Connector connector = provider.create(properties, connectorContext());

        Assertions.assertTrue(connector.testConnection(null).isSuccess());
        Assertions.assertTrue(connector.defaultTestConnection());
        Assertions.assertTrue(connector.getCapabilities().contains(
                ConnectorCapability.SUPPORTS_VENDED_CREDENTIALS));
        Assertions.assertEquals(List.of("default"),
                connector.getMetadata(null).listDatabaseNames(null));
        Assertions.assertEquals(List.of("events", "catalog_managed", "propertyless",
                        "missing_type"),
                connector.getMetadata(null).listTableNames(null, "default"));
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "blocked").isEmpty());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();
        Assertions.assertEquals(1, ((DeltaTableHandle) handle).getSnapshotVersion());
        Assertions.assertEquals(2, connector.getScanPlanProvider()
                .planScan(null, handle, List.of(), java.util.Optional.empty()).size());
        Map<String, String> scanProperties = connector.getScanPlanProvider()
                .getScanNodeProperties(null, handle, List.of(), java.util.Optional.empty());
        Assertions.assertEquals("parquet", scanProperties.get("file_format_type"));
        Assertions.assertFalse(scanProperties.containsValue(TEST_TOKEN));
        Assertions.assertTrue(requestPaths.stream().anyMatch(
                path -> path.endsWith("/delta/v1/catalogs/main/schemas/default/tables/events")));
        Assertions.assertTrue(requestQueries.stream().anyMatch(query -> query != null
                && query.contains("include_manifest_capabilities=true")));
        Assertions.assertTrue(requestPaths.stream().noneMatch(path -> path.endsWith("/credentials")));
    }

    @Test
    public void testUnityOAuthClientCredentialsAuthentication() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "oauth",
                DeltaConnectorProperties.UNITY_OAUTH_URI, workspaceUri + "/oauth/token",
                DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID, "client-id",
                DeltaConnectorProperties.UNITY_OAUTH_CLIENT_SECRET, "client-secret",
                DeltaConnectorProperties.UNITY_CATALOG, "main");

        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        provider.validateProperties(properties);
        Connector connector = provider.create(properties, connectorContext());

        Assertions.assertTrue(connector.testConnection(null).isSuccess());
        Assertions.assertEquals(List.of("default"),
                connector.getMetadata(null).listDatabaseNames(null));
        Assertions.assertTrue(requestPaths.stream().anyMatch(
                path -> path.equals("/oauth/token")));
    }

    @Test
    public void testUnityOAuthPropertiesRequireCompleteCredentials() {
        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        Map<String, String> properties = new java.util.HashMap<>(Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "oauth",
                DeltaConnectorProperties.UNITY_OAUTH_URI, workspaceUri + "/oauth/token",
                DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID, "client-id",
                DeltaConnectorProperties.UNITY_CATALOG, "main"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(properties));
    }

    @Test
    public void testOfficialDeltaCredentialEndpointAndBackendMappings() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        DeltaCredentialsResponse response = client.getReadCredentials("main", "default", "events");

        Map<String, String> aws = UnityDeltaStorageProperties.toBackendProperties(
                "s3://delta-bucket/tables/events", response, Map.of("s3.region", "us-west-2"));
        Assertions.assertEquals("temporary-ak", aws.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("temporary-sk", aws.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("temporary-session", aws.get("AWS_TOKEN"));
        long awsExpiration = Long.parseLong(aws.get("AWS_TOKEN_EXPIRATION_TIME_MS"));
        Assertions.assertTrue(awsExpiration > System.currentTimeMillis());
        Assertions.assertTrue(awsExpiration <= System.currentTimeMillis() + 3600000L);
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", aws.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", aws.get("AWS_REGION"));
        Assertions.assertFalse(aws.containsValue(TEST_TOKEN));
        Map<String, String> responseRegion = UnityDeltaStorageProperties.toBackendProperties(
                "s3://delta-bucket/tables/events", response, Map.of());
        Assertions.assertEquals("us-east-2", responseRegion.get("AWS_REGION"));
        Assertions.assertEquals("s3.us-east-2.amazonaws.com", responseRegion.get("AWS_ENDPOINT"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response, Map.of(),
                        DeltaCredentialOperation.READ_WRITE));
        Assertions.assertTrue(requestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/tables/events/credentials")));

        DeltaCredentialsResponse writeResponse = client.getWriteCredentials(
                "main", "default", "events");
        Map<String, String> writeProperties = UnityDeltaStorageProperties.toBackendProperties(
                "s3://delta-bucket/tables/events", writeResponse,
                Map.of("s3.region", "us-west-2"), DeltaCredentialOperation.READ_WRITE);
        Assertions.assertEquals("temporary-ak", writeProperties.get("AWS_ACCESS_KEY"));
        Assertions.assertTrue(requestQueries.stream().anyMatch(
                query -> query != null && query.contains("operation=READ_WRITE")));

        DeltaCredentialsResponse azureResponse = credentials(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                new DeltaStorageCredentialConfig().azureSasToken("azure-sas"));
        Map<String, String> azure = UnityDeltaStorageProperties.toBackendProperties(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                azureResponse, Map.of());
        Assertions.assertEquals("AZURE", azure.get("provider"));
        Assertions.assertEquals("account.blob.core.windows.net", azure.get("AWS_ENDPOINT"));
        Assertions.assertEquals("azure", azure.get("AWS_REGION"));
        Assertions.assertEquals("azure-sas", azure.get("AWS_TOKEN"));
        long azureExpiration = Long.parseLong(azure.get("AWS_TOKEN_EXPIRATION_TIME_MS"));
        Assertions.assertTrue(azureExpiration > System.currentTimeMillis());
        Assertions.assertTrue(azureExpiration <= System.currentTimeMillis() + 3600000L);
        Assertions.assertFalse(azure.containsKey("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));

        DeltaCredentialsResponse gcsResponse = credentials(
                "gs://delta-bucket/tables/events",
                new DeltaStorageCredentialConfig().gcsOauthToken("gcs-oauth"));
        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "gs://delta-bucket/tables/events", gcsResponse, Map.of()));
        Assertions.assertTrue(exception.getMessage().contains("GCS OAuth"));
    }

    @Test
    public void testRejectIncompleteVendedCredentials() {
        DeltaCredentialsResponse incompleteAws = credentials(
                "s3://delta-bucket/tables/events",
                new DeltaStorageCredentialConfig()
                        .s3AccessKeyId("temporary-ak")
                        .s3SecretAccessKey("temporary-sk"));
        IllegalArgumentException awsException = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", incompleteAws, Map.of()));
        Assertions.assertTrue(awsException.getMessage().contains("session token"));

        DeltaCredentialsResponse incompleteAzure = credentials(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                new DeltaStorageCredentialConfig().azureSasToken(""));
        IllegalArgumentException azureException = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "abfss://container@account.dfs.core.windows.net/tables/events",
                        incompleteAzure, Map.of()));
        Assertions.assertTrue(azureException.getMessage().contains("Azure SAS token"));
    }

    @Test
    public void testRejectMissingVendedCredentials() {
        DeltaCredentialsResponse empty = new DeltaCredentialsResponse();
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", empty, Map.of()));
        Assertions.assertTrue(exception.getMessage().contains("no storage credentials"));

        DeltaCredentialsResponse nullList = new DeltaCredentialsResponse()
                .storageCredentials(null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", nullList, Map.of()));
    }

    @Test
    public void testRejectVendedCredentialWithoutOperation() {
        DeltaCredentialsResponse response = new DeltaCredentialsResponse()
                .addStorageCredentialsItem(new DeltaStorageCredential()
                        .prefix("s3://delta-bucket/tables/events")
                        .config(new DeltaStorageCredentialConfig()
                                .s3AccessKeyId("temporary-ak")
                                .s3SecretAccessKey("temporary-sk")
                                .s3SessionToken("temporary-session")));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response, Map.of()));
        Assertions.assertTrue(exception.getMessage().contains("without an operation"));
    }

    @Test
    public void testRejectCredentialsThatAreAboutToExpire() {
        DeltaCredentialsResponse response = new DeltaCredentialsResponse()
                .addStorageCredentialsItem(new DeltaStorageCredential()
                        .prefix("s3://delta-bucket/tables/events")
                        .operation(DeltaCredentialOperation.READ)
                        .config(new DeltaStorageCredentialConfig()
                                .s3AccessKeyId("temporary-ak")
                                .s3SecretAccessKey("temporary-sk")
                                .s3SessionToken("temporary-session"))
                        .expirationTimeMs(System.currentTimeMillis() + 1000));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response,
                        Map.of("client.region", "us-east-1")));
        Assertions.assertTrue(exception.getMessage().contains("less than"));
    }

    @Test
    public void testRejectCredentialsWithoutExpiration() {
        DeltaCredentialsResponse response = new DeltaCredentialsResponse()
                .addStorageCredentialsItem(new DeltaStorageCredential()
                        .prefix("s3://delta-bucket/tables/events")
                        .operation(DeltaCredentialOperation.READ)
                        .config(new DeltaStorageCredentialConfig()
                                .s3AccessKeyId("temporary-ak")
                                .s3SecretAccessKey("temporary-sk")
                                .s3SessionToken("temporary-session")));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response,
                        Map.of("client.region", "us-east-1")));
        Assertions.assertTrue(exception.getMessage().contains("without an expiration time"));
    }

    @Test
    public void testUnityTableMetadataWithoutPropertiesIsOrdinaryExternalTable() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DeltaTableHandle handle = adapter.getTableHandle("default", "propertyless")
                .orElseThrow();

        Assertions.assertFalse(handle.isCatalogManaged());
        Assertions.assertTrue(handle.isExternalTable());
    }

    @Test
    public void testUnityTableMetadataWithoutTableTypeFailsClosed() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> adapter.getTableHandle("default", "missing_type"));

        Assertions.assertTrue(exception.getMessage().contains("supported table type"));
    }

    @Test
    public void testCatalogManagedSnapshotIncludesRatifiedLogTail() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();
        DeltaKernelSnapshot snapshot = adapter.loadSnapshot(handle);

        Assertions.assertTrue(handle.isCatalogManaged());
        Assertions.assertEquals("c79de738-d13c-44a5-8e75-8435123d60c7",
                handle.getCatalogTableId());
        Assertions.assertEquals(2, handle.getSnapshotVersion());
        Assertions.assertEquals(2, snapshot.getVersion());
        Assertions.assertEquals(
                List.of("part-00001.parquet", "part-00002.parquet"),
                snapshot.getActiveFiles().stream()
                        .map(file -> Paths.get(URI.create(file.getPath()))
                                .getFileName().toString())
                        .sorted()
                .collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testCatalogManagedInsertUsesUnityCatalogCommitter() throws Exception {
        catalogManagedLocation = copyCatalogManagedFixture().toUri().toString();
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of(
                        DeltaConnectorProperties.WRITE_ENABLED, "true"));
        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();

        Assertions.assertFalse(handle.isExternalTable());
        ConnectorInsertHandle insert = adapter.beginInsert(handle, "doris-catalog-managed-query");
        Path dataFile = Paths.get(URI.create(catalogManagedLocation)).resolve("part-doris.parquet");
        Files.write(dataFile, new byte[] {1, 2, 3, 4});

        DeltaInsertHandle deltaInsert = (DeltaInsertHandle) insert;
        deltaInsert.getWriter().finishInsert(deltaInsert, List.of(new ConnectorFileCommitInfo(
                dataFile.toUri().toString(), 2, Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis(), Map.of())));

        Assertions.assertTrue(updateRequestPaths.stream().anyMatch(
                path -> path.endsWith("/tables/catalog_managed")));
        try (java.util.stream.Stream<Path> stagedCommits = Files.list(
                Paths.get(URI.create(catalogManagedLocation)).resolve("_delta_log/_staged_commits"))) {
            Assertions.assertTrue(stagedCommits.anyMatch(
                    path -> path.getFileName().toString().startsWith("00000000000000000003.")));
        }
    }

    @Test
    public void testRejectRecreatedUnityTableWhilePlanningPinnedHandle() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());
        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();

        catalogManagedTableId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class, () -> adapter.loadSnapshot(handle));
        Assertions.assertTrue(exception.getMessage().contains("identity changed"));
    }

    @Test
    public void testUnityCatalogPropertiesRejectNonWorkspaceUris() {
        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        Map<String, String> base = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN);

        for (String invalidUri : List.of(
                "ftp://workspace.example.test",
                "https:missing-host",
                "https://workspace.example.test/api/2.1/unity-catalog",
                "https://user@workspace.example.test",
                "https://workspace.example.test?token=unsafe",
                "https://workspace.example.test#fragment")) {
            Map<String, String> properties = new java.util.HashMap<>(base);
            properties.put(DeltaConnectorProperties.UNITY_URI, invalidUri);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(properties), invalidUri);
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        if (exchange.getRequestURI().getPath().equals("/oauth/token")) {
            requestPaths.add(exchange.getRequestURI().getPath());
            requestQueries.add(exchange.getRequestURI().getRawQuery());
            respond(exchange, 200,
                    "{\"access_token\":\"" + TEST_TOKEN + "\",\"expires_in\":3600}");
            return;
        }
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (!("Bearer " + TEST_TOKEN).equals(authorization)) {
            respond(exchange, 401, "{\"error_code\":\"UNAUTHENTICATED\"}");
            return;
        }
        String path = exchange.getRequestURI().getPath();
        requestPaths.add(path);
        requestQueries.add(exchange.getRequestURI().getRawQuery());
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            updateRequestPaths.add(path);
        }
        if (path.equals("/api/2.1/unity-catalog/schemas")) {
            respond(exchange, 200,
                    "{\"schemas\":[{\"name\":\"default\",\"catalog_name\":\"main\"}]}");
            return;
        }
        if (path.equals("/api/2.1/unity-catalog/tables")) {
            respond(exchange, 200, "{\"tables\":["
                    + "{\"name\":\"events\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"catalog_managed\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"MANAGED\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"blocked\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"MANAGED\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"OTHER_CAPABILITY\"]},"
                    + "{\"name\":\"empty_capabilities\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[]},"
                    + "{\"name\":\"propertyless\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"missing_type\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"raw\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"PARQUET\"}]}");
            return;
        }
        if (path.endsWith("/tables/catalog_managed")
                && !"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getRequestBody().readAllBytes();
            respond(exchange, 200, catalogManagedLoadTableResponse());
            return;
        }
        if (path.endsWith("/tables/catalog_managed")) {
            respond(exchange, 200, catalogManagedLoadTableResponse());
            return;
        }
        if (path.endsWith("/tables/events/credentials")) {
            String operation = exchange.getRequestURI().getRawQuery() != null
                    && exchange.getRequestURI().getRawQuery().contains("READ_WRITE")
                    ? "READ_WRITE" : "READ";
            respond(exchange, 200, "{\"storage-credentials\":[{"
                    + "\"prefix\":\"s3://delta-bucket/tables/events\","
                    + "\"operation\":\"" + operation + "\",\"config\":{"
                    + "\"s3.access-key-id\":\"temporary-ak\","
                    + "\"s3.secret-access-key\":\"temporary-sk\","
                    + "\"s3.session-token\":\"temporary-session\","
                    + "\"client.region\":\"us-east-2\"},"
                    + "\"expiration-time-ms\":" + (System.currentTimeMillis() + 3600000) + "}]}");
            return;
        }
        if (path.endsWith("/tables/events")) {
            respond(exchange, 200, loadTableResponse(Map.of()));
            return;
        }
        if (path.endsWith("/tables/propertyless")) {
            respond(exchange, 200, loadTableResponseWithoutProperties(true));
            return;
        }
        if (path.endsWith("/tables/missing_type")) {
            respond(exchange, 200, loadTableResponseWithoutProperties(false));
            return;
        }
        respond(exchange, 404, "{\"error_code\":\"NOT_FOUND\"}");
    }

    private String loadTableResponse(Map<String, String> tableProperties) {
        String propertiesJson = tableProperties.isEmpty()
                ? "{}"
                : "{\"delta.feature.catalogManaged\":\"supported\"}";
        return "{\"metadata\":{\"etag\":\"test-etag\","
                + "\"table-type\":\"EXTERNAL\","
                + "\"table-uuid\":\"2ae93418-45d7-4f06-a899-d0379b3067d6\","
                + "\"location\":\"" + tableLocation + "\","
                + "\"partition-columns\":[],\"properties\":" + propertiesJson + ","
                + "\"last-commit-version\":1},\"commits\":[],\"latest-table-version\":1}";
    }

    private String loadTableResponseWithoutProperties(boolean includeTableType) {
        String tableType = includeTableType ? "\"table-type\":\"EXTERNAL\"," : "";
        return "{\"metadata\":{\"etag\":\"test-etag\"," + tableType
                + "\"table-uuid\":\"2ae93418-45d7-4f06-a899-d0379b3067d6\","
                + "\"location\":\"" + tableLocation + "\","
                + "\"partition-columns\":[],\"last-commit-version\":1},"
                + "\"commits\":[],\"latest-table-version\":1}";
    }

    private String catalogManagedLoadTableResponse() throws IOException {
        String firstCommit =
                "00000000000000000001.11111111-1111-1111-1111-111111111111.json";
        String secondCommit =
                "00000000000000000002.22222222-2222-2222-2222-222222222222.json";
        Path commitDirectory = Paths.get(URI.create(catalogManagedLocation))
                .resolve("_delta_log/_staged_commits");
        return "{\"metadata\":{\"etag\":\"catalog-managed-etag\","
                + "\"table-type\":\"MANAGED\","
                + "\"table-uuid\":\"" + catalogManagedTableId + "\","
                + "\"location\":\"" + catalogManagedLocation + "\","
                + "\"partition-columns\":[],\"properties\":{"
                + "\"delta.feature.catalogManaged\":\"supported\","
                + "\"delta.enableInCommitTimestamps\":\"true\"},"
                + "\"last-commit-version\":0},\"commits\":["
                + commitJson(1, firstCommit, Files.size(commitDirectory.resolve(firstCommit))) + ","
                + commitJson(2, secondCommit, Files.size(commitDirectory.resolve(secondCommit)))
                + "],\"latest-table-version\":2}";
    }

    private Path copyCatalogManagedFixture() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/catalog_managed_table"));
        Path source = Paths.get(fixture.toURI());
        Path target = tempDirectory.resolve("catalog-managed-table");
        try (java.util.stream.Stream<Path> paths = Files.walk(source)) {
            paths.forEach(path -> {
                try {
                    Path relative = source.relativize(path);
                    Path destination = target.resolve(relative);
                    if (Files.isDirectory(path)) {
                        Files.createDirectories(destination);
                    } else {
                        Files.createDirectories(destination.getParent());
                        Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }
        return target;
    }

    private static String commitJson(long version, String fileName, long fileSize) {
        return "{\"version\":" + version + ",\"timestamp\":"
                + (1700000000000L + version) + ",\"file-name\":\"" + fileName + "\","
                + "\"file-size\":" + fileSize + ",\"file-modification-timestamp\":"
                + (1700000000000L + version) + "}";
    }

    private static DeltaCredentialsResponse credentials(
            String prefix, DeltaStorageCredentialConfig config) {
        return new DeltaCredentialsResponse().addStorageCredentialsItem(
                new DeltaStorageCredential()
                        .prefix(prefix)
                        .operation(DeltaCredentialOperation.READ)
                        .config(config)
                        .expirationTimeMs(System.currentTimeMillis() + 3600000));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static ConnectorContext connectorContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "unity_delta_test";
            }

            @Override
            public long getCatalogId() {
                return 2;
            }
        };
    }
}
