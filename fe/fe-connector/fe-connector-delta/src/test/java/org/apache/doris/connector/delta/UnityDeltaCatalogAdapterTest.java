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
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UnityDeltaCatalogAdapterTest {
    private static final String TEST_TOKEN = "test-unity-token-never-log";

    private HttpServer server;
    private String workspaceUri;
    private String tableLocation;
    private final List<String> requestPaths = new ArrayList<>();

    @BeforeEach
    public void startServer() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/path_table"));
        tableLocation = Paths.get(fixture.toURI()).toUri().toString();
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
        Assertions.assertEquals(List.of("default"),
                connector.getMetadata(null).listDatabaseNames(null));
        Assertions.assertEquals(List.of("events"),
                connector.getMetadata(null).listTableNames(null, "default"));
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
        Assertions.assertTrue(requestPaths.stream().noneMatch(path -> path.endsWith("/credentials")));
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
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", aws.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", aws.get("AWS_REGION"));
        Assertions.assertFalse(aws.containsValue(TEST_TOKEN));
        Assertions.assertTrue(requestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/tables/events/credentials")));

        DeltaCredentialsResponse azureResponse = credentials(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                new DeltaStorageCredentialConfig().azureSasToken("azure-sas"));
        Map<String, String> azure = UnityDeltaStorageProperties.toBackendProperties(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                azureResponse, Map.of());
        Assertions.assertEquals("SAS",
                azure.get("fs.azure.account.auth.type.account.dfs.core.windows.net"));
        Assertions.assertEquals("azure-sas",
                azure.get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));

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
    public void testCatalogManagedMetadataFailsClosedBeforePathRead() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> adapter.getTableHandle("default", "catalog_managed"));

        Assertions.assertTrue(exception.getMessage().contains("catalog log tail"));
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
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (!("Bearer " + TEST_TOKEN).equals(authorization)) {
            respond(exchange, 401, "{\"error_code\":\"UNAUTHENTICATED\"}");
            return;
        }
        String path = exchange.getRequestURI().getPath();
        requestPaths.add(path);
        if (path.equals("/api/2.1/unity-catalog/schemas")) {
            respond(exchange, 200,
                    "{\"schemas\":[{\"name\":\"default\",\"catalog_name\":\"main\"}]}");
            return;
        }
        if (path.equals("/api/2.1/unity-catalog/tables")) {
            respond(exchange, 200, "{\"tables\":["
                    + "{\"name\":\"events\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\"},"
                    + "{\"name\":\"raw\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"PARQUET\"}]}");
            return;
        }
        if (path.endsWith("/tables/catalog_managed")) {
            respond(exchange, 200, loadTableResponse(
                    Map.of("delta.feature.catalogManaged", "supported")));
            return;
        }
        if (path.endsWith("/tables/events/credentials")) {
            respond(exchange, 200, "{\"storage-credentials\":[{"
                    + "\"prefix\":\"s3://delta-bucket/tables/events\","
                    + "\"operation\":\"READ\",\"config\":{"
                    + "\"s3.access-key-id\":\"temporary-ak\","
                    + "\"s3.secret-access-key\":\"temporary-sk\","
                    + "\"s3.session-token\":\"temporary-session\"},"
                    + "\"expiration-time-ms\":" + (System.currentTimeMillis() + 3600000) + "}]}");
            return;
        }
        if (path.endsWith("/tables/events")) {
            respond(exchange, 200, loadTableResponse(Map.of()));
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
