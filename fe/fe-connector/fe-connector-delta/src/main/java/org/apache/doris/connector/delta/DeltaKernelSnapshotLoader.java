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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Loads a filesystem-managed Delta table snapshot through Delta Kernel. */
public class DeltaKernelSnapshotLoader {
    private final Engine engine;

    public DeltaKernelSnapshotLoader(Engine engine) {
        this.engine = engine;
    }

    public DeltaKernelSnapshot load(String tablePath) throws IOException {
        Snapshot snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine);
        Scan scan = snapshot.getScanBuilder().build();
        List<FileStatus> activeFiles = new ArrayList<>();

        try (CloseableIterator<FilteredColumnarBatch> batches = scan.getScanFiles(engine)) {
            while (batches.hasNext()) {
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        // Kernel's public Scan API returns nested scan rows. Keep this
                        // version-specific extraction in the adapter so the rest of the
                        // connector does not depend on Kernel's internal package.
                        activeFiles.add(InternalScanFileUtils.getAddFileStatus(rows.next()));
                    }
                }
            }
        }
        activeFiles.sort(Comparator.comparing(FileStatus::getPath));
        return new DeltaKernelSnapshot(
                snapshot.getPath(), snapshot.getVersion(), snapshot.getSchema(), activeFiles);
    }
}
