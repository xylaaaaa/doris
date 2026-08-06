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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.BaseViewSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergRestConnectivityTesterTest {

    @Test
    public void testConnectionUsesFullCatalogInitialization() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8181");
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

        CapturingIcebergRestProperties restProperties = new CapturingIcebergRestProperties(props);
        restProperties.initNormalizeAndCheckProps();
        List<StorageProperties> storageProperties = List.of(Mockito.mock(StorageProperties.class));

        IcebergRestConnectivityTester tester =
                new IcebergRestConnectivityTester(restProperties, storageProperties);
        tester.testConnection();

        Assertions.assertSame(storageProperties, restProperties.initializedWithStorageProperties);
        Mockito.verify((SupportsNamespaces) restProperties.catalog).listNamespaces();
        Assertions.assertEquals("s3://bucket/warehouse", tester.getTestLocation());
        Assertions.assertTrue(restProperties.closed);
    }

    @Test
    public void testConnectionClosesCatalogWhenNamespaceListingFails() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.rest.uri", "http://localhost:8181");

        CapturingIcebergRestProperties restProperties = new CapturingIcebergRestProperties(props);
        restProperties.initNormalizeAndCheckProps();
        Mockito.doThrow(new RuntimeException("list failed"))
                .when((SupportsNamespaces) restProperties.catalog).listNamespaces();

        IcebergRestConnectivityTester tester =
                new IcebergRestConnectivityTester(restProperties, List.of());
        Assertions.assertThrows(RuntimeException.class, tester::testConnection);
        Assertions.assertTrue(restProperties.closed);
    }

    @Test
    public void testServerDefaultFileIOIsOverriddenByCatalogProperties() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/iceberg/v1/config", exchange -> respond(exchange,
                "{\"defaults\":{\"io-impl\":\"org.apache.iceberg.aliyun.oss.OSSFileIO\"}}"));
        server.createContext("/iceberg/v1/namespaces", exchange -> respond(exchange,
                "{\"namespaces\":[]}"));
        server.start();

        try {
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            props.put("iceberg.catalog.type", "rest");
            props.put("iceberg.rest.uri",
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/iceberg");
            props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
            props.put("s3.endpoint", "http://127.0.0.1:1");
            props.put("s3.region", "us-east-1");
            props.put("s3.access_key", "placeholder-access-key");
            props.put("s3.secret_key", "placeholder-secret-key");

            IcebergRestProperties restProperties =
                    (IcebergRestProperties) MetastoreProperties.create(props);
            List<StorageProperties> storageProperties = StorageProperties.createAll(props);

            IcebergRestConnectivityTester tester =
                    new IcebergRestConnectivityTester(restProperties, storageProperties);
            Assertions.assertDoesNotThrow(tester::testConnection);
            Assertions.assertNull(restProperties.getRestSessionCatalog());
        } finally {
            server.stop(0);
        }
    }

    private static void respond(HttpExchange exchange, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static class CapturingIcebergRestProperties extends IcebergRestProperties {
        private final Catalog catalog;
        private final BaseViewSessionCatalog sessionCatalog;
        private List<StorageProperties> initializedWithStorageProperties;
        private boolean closed;

        private CapturingIcebergRestProperties(Map<String, String> props) {
            super(props);
            this.catalog = Mockito.mock(Catalog.class,
                    Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
            this.sessionCatalog = Mockito.mock(BaseViewSessionCatalog.class);
            Mockito.when(sessionCatalog.properties()).thenReturn(
                    Map.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/warehouse"));
        }

        @Override
        protected Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
                List<StorageProperties> storagePropertiesList, SessionContext sessionContext) {
            this.initializedWithStorageProperties = storagePropertiesList;
            return catalog;
        }

        @Override
        public BaseViewSessionCatalog getRestSessionCatalog() {
            return sessionCatalog;
        }

        @Override
        public void closeRestSessionCatalog() {
            closed = true;
        }
    }
}
