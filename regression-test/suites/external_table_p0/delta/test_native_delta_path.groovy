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

suite("test_native_delta_path", "p0,external") {
    def dorisHome = System.getenv("DORIS_HOME") ?: System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.trim().isEmpty()) {
        return
    }

    def catalogName = "test_native_delta_path"
    def tablePath = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").toURI().toString()

    sql "DROP CATALOG IF EXISTS ${catalogName}"
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'customer',
            'delta.table.path' = '${tablePath}',
            'test_connection' = 'false'
        )
    """

    qt_schema "DESC ${catalogName}.`default`.customer"
    order_qt_count "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"
    order_qt_filtered """
        SELECT c_custkey, c_name
        FROM ${catalogName}.`default`.customer
        WHERE c_custkey < 100
        ORDER BY c_custkey
        LIMIT 3
    """

    def createdTablePath = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-create-").resolve("created_events")
    def createCatalogName = "test_native_delta_path_create"
    sql "DROP CATALOG IF EXISTS ${createCatalogName}"
    sql """
        CREATE CATALOG ${createCatalogName} PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'created_events',
            'delta.table.path' = '${createdTablePath.toUri()}',
            'delta.write.enabled' = 'true',
            'test_connection' = 'false'
        )
    """
    sql """
        CREATE TABLE ${createCatalogName}.`default`.created_events (
            id BIGINT NOT NULL,
            payload STRING NULL
        )
    """
    sql "INSERT INTO ${createCatalogName}.`default`.created_events VALUES (1, 'created')"
    sql """
        INSERT OVERWRITE TABLE ${createCatalogName}.`default`.created_events
        SELECT 2, 'overwritten'
    """
    order_qt_created_after_overwrite """
        SELECT id, payload
        FROM ${createCatalogName}.`default`.created_events
        ORDER BY id
    """

    def sourceTable = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").toPath()
    def writableTable = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-write-").resolve("customer")
    java.nio.file.Files.walk(sourceTable).withCloseable { paths ->
        paths.forEach { source ->
            def relative = sourceTable.relativize(source)
            def target = writableTable.resolve(relative)
            if (java.nio.file.Files.isDirectory(source)) {
                java.nio.file.Files.createDirectories(target)
            } else {
                java.nio.file.Files.copy(source, target)
            }
        }
    }

    def writeCatalogName = "test_native_delta_path_write"
    sql "DROP CATALOG IF EXISTS ${writeCatalogName}"
    sql """
        CREATE CATALOG ${writeCatalogName} PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'customer',
            'delta.table.path' = '${writableTable.toUri()}',
            'delta.write.enabled' = 'true',
            'test_connection' = 'false'
        )
    """

    sql """
        INSERT INTO ${writeCatalogName}.`default`.customer VALUES
        (200001, 'Customer#000200001', 'Doris native Delta', 1,
         '10-100-100-1000', 12.34, 'BUILDING', 'native write')
    """
    order_qt_count_after_insert "SELECT COUNT(*) FROM ${writeCatalogName}.`default`.customer"
    order_qt_inserted_row """
        SELECT c_custkey, c_name, c_acctbal
        FROM ${writeCatalogName}.`default`.customer
        WHERE c_custkey = 200001
    """
}
