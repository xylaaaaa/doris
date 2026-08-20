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

suite("test_native_delta_unity", "p0,external") {
    def dorisHome = System.getenv("DORIS_HOME") ?: System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.trim().isEmpty()) {
        return
    }

    def tablePath = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").toURI().toString()
    def token = "native-delta-unity-test-token"
    def server = com.sun.net.httpserver.HttpServer.create(
            new java.net.InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/") { exchange ->
        def sendJson = { int status, String body ->
            def bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8)
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(status, bytes.length)
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        if (exchange.requestHeaders.getFirst("Authorization") != "Bearer ${token}") {
            sendJson(401, '{"error_code":"UNAUTHENTICATED"}')
            return
        }

        def path = exchange.requestURI.path
        if (path == "/api/2.1/unity-catalog/schemas") {
            sendJson(200, '{"schemas":[{"name":"default","catalog_name":"main"}]}')
        } else if (path == "/api/2.1/unity-catalog/tables") {
            sendJson(200, '{"tables":[{"name":"customer","catalog_name":"main",'
                    + '"schema_name":"default","table_type":"EXTERNAL",'
                    + '"data_source_format":"DELTA",'
                    + '"manifest_capabilities":["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]},'
                    + '{"name":"managed_customer","catalog_name":"main",'
                    + '"schema_name":"default","table_type":"MANAGED",'
                    + '"data_source_format":"DELTA",'
                    + '"manifest_capabilities":["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]},'
                    + '{"name":"blocked","catalog_name":"main",'
                    + '"schema_name":"default","table_type":"EXTERNAL",'
                    + '"data_source_format":"DELTA",'
                    + '"manifest_capabilities":["OTHER_CAPABILITY"]}]}')
        } else if (path.endsWith("/tables/customer")) {
            sendJson(200, '{"metadata":{"etag":"test-etag",'
                    + '"table-type":"EXTERNAL",'
                    + '"table-uuid":"421eb35b-e9ec-44ed-92fd-25e0fda91036",'
                    + '"location":"' + tablePath + '",'
                    + '"partition-columns":[],"last-commit-version":0},'
                    + '"commits":[],"latest-table-version":0}')
        } else if (path.endsWith("/tables/managed_customer")) {
            sendJson(200, '{"metadata":{"etag":"managed-etag",'
                    + '"table-type":"MANAGED",'
                    + '"table-uuid":"0e5e167d-b97c-4fe9-af31-4e06f57a82a4",'
                    + '"location":"' + tablePath + '",'
                    + '"partition-columns":[],"last-commit-version":0},'
                    + '"commits":[],"latest-table-version":0}')
        } else {
            sendJson(404, '{"error_code":"NOT_FOUND"}')
        }
    }
    server.start()

    try {
        def catalogName = "test_native_delta_unity"
        sql "DROP CATALOG IF EXISTS ${catalogName}"
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'delta',
                'delta.catalog.type' = 'unity',
                'unity.uri' = 'http://127.0.0.1:${server.address.port}',
                'unity.auth.type' = 'pat',
                'unity.token' = '${token}',
                'unity.catalog' = 'main',
                'test_connection' = 'false'
            )
        """

        order_qt_databases "SHOW DATABASES FROM ${catalogName}"
        order_qt_tables "SHOW TABLES FROM ${catalogName}.`default`"
        order_qt_count "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"
        order_qt_filtered """
            SELECT c_custkey, c_name
            FROM ${catalogName}.`default`.customer
            WHERE c_custkey <= 3
            ORDER BY c_custkey
        """
        order_qt_managed_count "SELECT COUNT(*) FROM ${catalogName}.`default`.managed_customer"
    } finally {
        server.stop(0)
    }
}
