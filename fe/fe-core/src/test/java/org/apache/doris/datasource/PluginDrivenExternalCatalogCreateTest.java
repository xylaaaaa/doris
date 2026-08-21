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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableCreateRequest;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PluginDrivenExternalCatalogCreateTest {

    @Test
    public void testDelegatesStructuredCreateRequest() throws Exception {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getCapabilities()).thenReturn(
                Set.of(ConnectorCapability.SUPPORTS_CREATE_TABLE));
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.createTable(
                Mockito.any(ConnectorSession.class), Mockito.any())).thenReturn(true);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.when(database.getRemoteName()).thenReturn("remote_db");
        TestablePluginCatalog catalog = new TestablePluginCatalog(connector, database);
        CreateTableInfo createInfo = Mockito.mock(CreateTableInfo.class);
        Mockito.when(createInfo.getDbName()).thenReturn("local_db");
        Mockito.when(createInfo.getTableName()).thenReturn("events");
        Mockito.when(createInfo.getColumns()).thenReturn(List.of(
                new Column("id", Type.BIGINT, false),
                new Column("payload", Type.STRING, true)));
        Mockito.when(createInfo.getProperties()).thenReturn(Map.of("owner", "doris"));
        Mockito.when(createInfo.getExtProperties()).thenReturn(Map.of("custom", "value"));
        Mockito.when(createInfo.getComment()).thenReturn("events table");
        Mockito.when(createInfo.isIfNotExists()).thenReturn(true);

        Assertions.assertTrue(catalog.createTable(createInfo));

        ArgumentCaptor<ConnectorTableCreateRequest> request =
                ArgumentCaptor.forClass(ConnectorTableCreateRequest.class);
        Mockito.verify(metadata).createTable(Mockito.any(ConnectorSession.class), request.capture());
        Assertions.assertEquals("remote_db", request.getValue().getDatabaseName());
        Assertions.assertEquals("events", request.getValue().getTableSchema().getTableName());
        Assertions.assertEquals(List.of("id", "payload"),
                request.getValue().getTableSchema().getColumns().stream()
                        .map(column -> column.getName()).toList());
        Assertions.assertEquals(Map.of("owner", "doris", "custom", "value"),
                request.getValue().getProperties());
        Assertions.assertEquals("events table", request.getValue().getComment());
        Assertions.assertTrue(request.getValue().isIfNotExists());
    }

    private static final class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector testConnector;
        private final ExternalDatabase<?> database;

        TestablePluginCatalog(Connector connector, ExternalDatabase<?> database) {
            super(1L, "delta_catalog", null, Map.of("type", "delta"), "", connector);
            this.testConnector = connector;
            this.database = database;
        }

        @Override
        protected Connector createConnectorFromProperties() {
            return testConnector;
        }

        @Override
        public ExternalDatabase<?> getDbNullable(String dbName) {
            return database;
        }

        @Override
        public String getType() {
            return "delta";
        }

        @Override
        protected List<String> listDatabaseNames() {
            return List.of("remote_db");
        }

        @Override
        protected List<String> listTableNamesFromRemote(
                SessionContext ctx, String dbName) {
            return List.of();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }
}
