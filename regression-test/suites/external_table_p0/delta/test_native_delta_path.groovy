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
}
