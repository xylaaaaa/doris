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

import java.util.List;
import java.util.Optional;

/** Catalog-specific control-plane operations used by the Delta connector. */
public interface DeltaCatalogAdapter {

    /** Lists databases visible through this adapter. */
    List<String> listDatabaseNames();

    /** Checks whether a database is visible through this adapter. */
    boolean databaseExists(String databaseName);

    /** Lists tables visible in a database. */
    List<String> listTableNames(String databaseName);

    /** Resolves a table name to a handle pinned to a Delta snapshot version. */
    Optional<DeltaTableHandle> getTableHandle(String databaseName, String tableName);

    /** Loads the snapshot pinned by a table handle. */
    DeltaKernelSnapshot loadSnapshot(DeltaTableHandle tableHandle);
}
