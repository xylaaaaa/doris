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

import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.persist.TruncateTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PluginDrivenExternalCatalogTruncateTest {

    @Test
    public void testTruncateResolvesRemoteHandleAndInvalidatesLocalTable()
            throws Exception {
        Fixture fixture = fixture(Set.of(
                ConnectorCapability.SUPPORTS_TRUNCATE_TABLE), true);

        fixture.catalog.truncateTableFromConnector("local_db", "events");

        Mockito.verify(fixture.metadata).truncateTable(
                Mockito.any(ConnectorSession.class), Mockito.same(fixture.handle));
        Mockito.verify(fixture.database).unregisterTable("events");
    }

    @Test
    public void testTruncateRequiresConnectorCapability() {
        Fixture fixture = fixture(Set.of(), true);

        Assertions.assertThrows(DdlException.class,
                () -> fixture.catalog.truncateTableFromConnector(
                        "local_db", "events"));
    }

    @Test
    public void testTruncateRejectsMissingRemoteTable() {
        Fixture fixture = fixture(Set.of(
                ConnectorCapability.SUPPORTS_TRUNCATE_TABLE), false);

        Assertions.assertThrows(DdlException.class,
                () -> fixture.catalog.truncateTableFromConnector(
                        "local_db", "events"));
        Mockito.verify(fixture.database, Mockito.never()).unregisterTable("events");
    }

    @Test
    public void testPartitionTruncateIsRejectedBeforeConnectorCall() {
        Fixture fixture = fixture(Set.of(
                ConnectorCapability.SUPPORTS_TRUNCATE_TABLE), true);

        Assertions.assertThrows(DdlException.class,
                () -> fixture.catalog.truncateTable("local_db", "events",
                        Mockito.mock(PartitionNamesInfo.class), false, ""));
        Mockito.verifyNoInteractions(fixture.metadata);
    }

    @Test
    public void testReplayInvalidatesLocalTableWithoutRemoteCall() {
        Fixture fixture = fixture(Set.of(
                ConnectorCapability.SUPPORTS_TRUNCATE_TABLE), true);

        fixture.catalog.replayTruncateTable(new TruncateTableInfo(
                "delta_catalog", "local_db", "events", null, 1L));

        Mockito.verify(fixture.database).unregisterTable("events");
        Mockito.verifyNoInteractions(fixture.metadata);
    }

    private static Fixture fixture(
            Set<ConnectorCapability> capabilities, boolean remoteExists) {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(connector.getCapabilities()).thenReturn(capabilities);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getTableHandle(Mockito.any(ConnectorSession.class),
                Mockito.eq("remote_db"), Mockito.eq("events")))
                .thenReturn(remoteExists ? Optional.of(handle) : Optional.empty());

        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> database =
                Mockito.mock(ExternalDatabase.class);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(database.getTableNullable("events")).thenReturn(table);
        Mockito.when(table.getRemoteDbName()).thenReturn("remote_db");
        Mockito.when(table.getRemoteName()).thenReturn("events");
        TestablePluginCatalog catalog = new TestablePluginCatalog(connector, database);
        return new Fixture(catalog, metadata, handle, database);
    }

    private static final class Fixture {
        private final TestablePluginCatalog catalog;
        private final ConnectorMetadata metadata;
        private final ConnectorTableHandle handle;
        private final ExternalDatabase<PluginDrivenExternalTable> database;

        private Fixture(TestablePluginCatalog catalog, ConnectorMetadata metadata,
                ConnectorTableHandle handle,
                ExternalDatabase<PluginDrivenExternalTable> database) {
            this.catalog = catalog;
            this.metadata = metadata;
            this.handle = handle;
            this.database = database;
        }
    }

    private static final class TestablePluginCatalog
            extends PluginDrivenExternalCatalog {
        private final Connector testConnector;
        private final ExternalDatabase<PluginDrivenExternalTable> database;

        private TestablePluginCatalog(Connector connector,
                ExternalDatabase<PluginDrivenExternalTable> database) {
            super(1L, "delta_catalog", null,
                    Map.of("type", "delta"), "", connector);
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
        public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(
                String dbName) {
            return Optional.of(database);
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
            return List.of("events");
        }

        @Override
        public boolean tableExist(
                SessionContext ctx, String dbName, String tblName) {
            return true;
        }
    }
}
