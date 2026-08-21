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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSnapshot;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Set;

public class PluginDrivenScanNodeSnapshotTest {

    @Test
    public void testConvertsVersionAndTimeSnapshots() {
        ConnectorTableSnapshot version = PluginDrivenScanNode.toConnectorTableSnapshot(
                TableSnapshot.versionOf("17"));
        Assertions.assertEquals(ConnectorTableSnapshot.Type.VERSION, version.getType());
        Assertions.assertEquals(17L, version.getValue());

        ConnectorTableSnapshot timestamp = PluginDrivenScanNode.toConnectorTableSnapshot(
                TableSnapshot.timeOf("2026-08-21 12:34:56"));
        Assertions.assertEquals(
                ConnectorTableSnapshot.Type.TIMESTAMP_MILLIS, timestamp.getType());
        Assertions.assertTrue(timestamp.getValue() > 0);

        ConnectorTableSnapshot timestampWithMillis =
                PluginDrivenScanNode.toConnectorTableSnapshot(
                        TableSnapshot.timeOf("2026-08-21 12:34:56.123"));
        Assertions.assertEquals(123L, timestampWithMillis.getValue() % 1000);

        ConnectorTableSnapshot numericTimestamp =
                PluginDrivenScanNode.toConnectorTableSnapshot(
                        TableSnapshot.timeOf("1700000000123"));
        Assertions.assertEquals(1_700_000_000_123L, numericTimestamp.getValue());
    }

    @Test
    public void testRejectsInvalidSnapshotBoundaries() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PluginDrivenScanNode.toConnectorTableSnapshot(
                        TableSnapshot.versionOf("not-a-version")));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PluginDrivenScanNode.toConnectorTableSnapshot(
                        TableSnapshot.versionOf("-1")));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PluginDrivenScanNode.toConnectorTableSnapshot(
                        TableSnapshot.timeOf("not-a-time")));
    }

    @Test
    public void testSnapshotReplacesConnectorHandleBeforeScanInitialization() {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle latest = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle historical = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(connector.getCapabilities()).thenReturn(
                Set.of(ConnectorCapability.SUPPORTS_TIME_TRAVEL));
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(metadata.applyTableSnapshot(
                Mockito.eq(session), Mockito.eq(latest), Mockito.any()))
                .thenReturn(historical);
        PluginDrivenScanNode scanNode = new PluginDrivenScanNode(
                new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), false,
                new SessionVariable(), ScanContext.EMPTY, connector, session, latest);

        scanNode.setQueryTableSnapshot(TableSnapshot.versionOf("3"));

        ArgumentCaptor<ConnectorTableSnapshot> snapshot =
                ArgumentCaptor.forClass(ConnectorTableSnapshot.class);
        Mockito.verify(metadata).applyTableSnapshot(
                Mockito.eq(session), Mockito.eq(latest), snapshot.capture());
        Assertions.assertEquals(ConnectorTableSnapshot.Type.VERSION,
                snapshot.getValue().getType());
        Assertions.assertEquals(3L, snapshot.getValue().getValue());
        Assertions.assertEquals("3", scanNode.getQueryTableSnapshot().getValue());
    }
}
