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
import org.apache.doris.connector.api.write.ConnectorFileCommitInfo;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DeltaKernelWriterTest {

    @TempDir
    Path tempDirectory;

    @Test
    public void testBlindAppendCommitsBackendDataFile() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Path dataFile = tableDirectory.resolve("part-doris.parquet");
        Files.write(dataFile, new byte[] {1, 2, 3, 4});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaInsertHandle insert = writer.beginInsert(new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), 1));

        writer.finishInsert(insert, List.of(new ConnectorFileCommitInfo(
                dataFile.toUri().toString(), 2, Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis(), Map.of())));

        DeltaKernelSnapshot snapshot = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(2, snapshot.getVersion());
        Assertions.assertTrue(snapshot.getActiveFiles().stream()
                .anyMatch(file -> file.getPath().endsWith("part-doris.parquet")));
        String commit = Files.readString(tableDirectory.resolve(
                "_delta_log/00000000000000000002.json"));
        Assertions.assertTrue(commit.contains("part-doris.parquet"));
        Assertions.assertTrue(commit.contains("Apache Doris native Delta connector"));
    }

    @Test
    public void testEmptyAppendDoesNotCreateDeltaVersion() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaInsertHandle insert = writer.beginInsert(new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), 1));

        writer.finishInsert(insert, List.of());

        Assertions.assertFalse(Files.exists(tableDirectory.resolve(
                "_delta_log/00000000000000000002.json")));
    }

    @Test
    public void testPartitionedAppendPreservesOrderAndNullValues() throws Exception {
        Path tableDirectory = copyFixture(
                "delta/partitioned_table/_delta_log",
                List.of("00000000000000000000.json"));
        Path regularFile = tableDirectory.resolve("p2=two/p1=one/part-regular.parquet");
        Path nullFile = tableDirectory.resolve(
                "p2=__HIVE_DEFAULT_PARTITION__/p1=__HIVE_DEFAULT_PARTITION__/part-null.parquet");
        Files.createDirectories(regularFile.getParent());
        Files.createDirectories(nullFile.getParent());
        Files.write(regularFile, new byte[] {1, 2, 3});
        Files.write(nullFile, new byte[] {4, 5, 6});
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);
        DeltaInsertHandle insert = writer.beginInsert(new DeltaTableHandle(
                "default", "events", tableDirectory.toUri().toString(), 0));

        writer.finishInsert(insert, List.of(
                commitInfo(regularFile, Map.of("p1", "one", "p2", "two"), Set.of()),
                commitInfo(nullFile,
                        Map.of("p1", "__HIVE_DEFAULT_PARTITION__",
                                "p2", "__HIVE_DEFAULT_PARTITION__"),
                        Set.of("p2"))));

        DeltaKernelSnapshot snapshot = new DeltaKernelSnapshotLoader(engine)
                .loadLatest(tableDirectory.toUri().toString());
        Assertions.assertEquals(1, snapshot.getVersion());
        Assertions.assertTrue(snapshot.getActiveFiles().stream()
                .anyMatch(file -> file.getPath().contains("p2=two/p1=one/part-regular.parquet")));
        String commit = Files.readString(tableDirectory.resolve(
                "_delta_log/00000000000000000001.json"));
        Assertions.assertTrue(commit.contains("p2=two/p1=one/part-regular.parquet"));
        Assertions.assertTrue(commit.contains("\"p1\":\"__HIVE_DEFAULT_PARTITION__\""));
        Assertions.assertTrue(commit.contains("\"p2\":null"));
    }

    @Test
    public void testBeginInsertRejectsChangedSnapshot() throws Exception {
        Path tableDirectory = copyPathTableFixture();
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaKernelWriter writer = new DeltaKernelWriter(engine);

        Assertions.assertThrows(DorisConnectorException.class,
                () -> writer.beginInsert(new DeltaTableHandle(
                        "default", "events", tableDirectory.toUri().toString(), 0)));
    }

    private Path copyPathTableFixture() throws Exception {
        return copyFixture("delta/path_table/_delta_log", List.of(
                "00000000000000000000.json", "00000000000000000001.json"));
    }

    private ConnectorFileCommitInfo commitInfo(Path file, Map<String, String> partitionValues,
            Set<String> nullPartitionColumns) throws Exception {
        return new ConnectorFileCommitInfo(file.toUri().toString(), 1, Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), partitionValues, nullPartitionColumns);
    }

    private Path copyFixture(String resource, List<String> fileNames) throws Exception {
        URL fixture = Objects.requireNonNull(getClass().getClassLoader().getResource(resource));
        Path sourceLog = Paths.get(fixture.toURI());
        Path targetLog = tempDirectory.resolve("table-" + System.nanoTime()).resolve("_delta_log");
        Files.createDirectories(targetLog);
        for (String fileName : fileNames) {
            Files.copy(sourceLog.resolve(fileName), targetLog.resolve(fileName),
                    StandardCopyOption.REPLACE_EXISTING);
        }
        return targetLog.getParent();
    }
}
