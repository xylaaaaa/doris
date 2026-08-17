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

import io.delta.kernel.defaults.engine.DefaultEngine;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeltaKernelSnapshotLoaderTest {
    @Test
    public void testLoadLatestSnapshotAndActiveFiles() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/path_table"));
        String tablePath = Paths.get(fixture.toURI()).toUri().toString();
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        DeltaKernelSnapshot snapshot = loader.load(tablePath);

        Assertions.assertEquals(1, snapshot.getVersion());
        Assertions.assertEquals(
                Paths.get(fixture.toURI()).toAbsolutePath().normalize().toString(),
                Paths.get(java.net.URI.create(snapshot.getTablePath())).toAbsolutePath().normalize()
                        .toString());
        Assertions.assertEquals(List.of("id", "name"), snapshot.getSchema().fieldNames());
        Assertions.assertEquals(List.of(), snapshot.getPartitionColumnNames());
        Assertions.assertEquals(
                List.of("part-00001.parquet", "part-00002.parquet"),
                snapshot.getActiveFiles().stream()
                        .map(file -> Paths.get(java.net.URI.create(file.getPath())).getFileName().toString())
                        .collect(Collectors.toList()));

        DeltaKernelSnapshot versionZero = loader.loadVersion(tablePath, 0);
        Assertions.assertEquals(0, versionZero.getVersion());
        Assertions.assertEquals(
                List.of("part-00000.parquet", "part-00001.parquet"),
                versionZero.getActiveFiles().stream()
                        .map(file -> Paths.get(java.net.URI.create(file.getPath())).getFileName().toString())
                        .collect(Collectors.toList()));
    }

    @Test
    public void testRejectColumnMappingUntilPhysicalTransformIsSupported() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/column_mapping_table"));
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> loader.loadLatest(Paths.get(fixture.toURI()).toUri().toString()));

        Assertions.assertTrue(exception.getMessage().contains("column mapping"));
    }
}
