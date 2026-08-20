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
    def catalogManagedDirectory = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-catalog-managed-")
    def unityExternalWriteDirectory = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-unity-write-")
    def sourceTable = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").toPath()
    java.nio.file.Files.walk(sourceTable).withCloseable { paths ->
        paths.forEach { source ->
            def relative = sourceTable.relativize(source)
            def destination = unityExternalWriteDirectory.resolve(relative)
            if (java.nio.file.Files.isDirectory(source)) {
                java.nio.file.Files.createDirectories(destination)
            } else {
                java.nio.file.Files.copy(source, destination,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }
    def catalogManagedSource = new File(dorisHome,
            "fe/fe-connector/fe-connector-delta/src/test/resources/delta/catalog_managed_table")
            .toPath()
    java.nio.file.Files.walk(catalogManagedSource).withCloseable { paths ->
        paths.forEach { source ->
            def relative = catalogManagedSource.relativize(source)
            def destination = catalogManagedDirectory.resolve(relative)
            if (java.nio.file.Files.isDirectory(source)) {
                java.nio.file.Files.createDirectories(destination)
            } else {
                java.nio.file.Files.copy(source, destination,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }
    def sourceParquetFiles = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").listFiles()
            .findAll { it.name.endsWith(".parquet") }.sort { it.name }
    def catalogManagedDataFiles = ["part-00001.parquet", "part-00002.parquet"]
    catalogManagedDataFiles.eachWithIndex { fileName, index ->
        java.nio.file.Files.copy(sourceParquetFiles[index].toPath(),
                catalogManagedDirectory.resolve(fileName),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    }
    def jsonSlurper = new groovy.json.JsonSlurper()
    def stagedCommitDirectory = catalogManagedDirectory.resolve("_delta_log/_staged_commits")
    java.nio.file.Files.list(stagedCommitDirectory).withCloseable { commits ->
        commits.forEach { commit ->
            def updatedLines = java.nio.file.Files.readAllLines(commit).collect { line ->
                def action = jsonSlurper.parseText(line)
                if (action.add != null) {
                    action.add.size = java.nio.file.Files.size(
                            catalogManagedDirectory.resolve(action.add.path))
                }
                groovy.json.JsonOutput.toJson(action)
            }
            java.nio.file.Files.write(commit, updatedLines)
        }
    }
    def catalogManagedPath = catalogManagedDirectory.toUri().toString()
    def catalogManagedTableId = "c79de738-d13c-44a5-8e75-8435123d60c7"
    def catalogManagedCommits = java.nio.file.Files.list(stagedCommitDirectory).withCloseable {
        commits -> commits.sorted().collect { commit ->
            def fileName = commit.fileName.toString()
            def version = Long.parseLong(fileName.substring(0, 20))
            [version: version, timestamp: 1700000000000L + version,
                    "file-name": fileName, "file-size": java.nio.file.Files.size(commit),
                    "file-modification-timestamp": 1700000000000L + version]
        }
    }
    def catalogManagedResponse = groovy.json.JsonOutput.toJson([
            metadata: [etag: "catalog-managed-etag", "table-type": "MANAGED",
                    "table-uuid": catalogManagedTableId, location: catalogManagedPath,
                    "partition-columns": [], properties: [
                            "delta.feature.catalogManaged": "supported",
                            "delta.enableInCommitTimestamps": "true"],
                    "last-commit-version": 0],
            commits: catalogManagedCommits, "latest-table-version": 2])
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
                    + '{"name":"catalog_managed","catalog_name":"main",'
                    + '"schema_name":"default","table_type":"MANAGED",'
                    + '"data_source_format":"DELTA",'
                    + '"manifest_capabilities":["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]},'
                    + '{"name":"managed_customer","catalog_name":"main",'
                    + '"schema_name":"default","table_type":"MANAGED",'
                    + '"data_source_format":"DELTA",'
                    + '"manifest_capabilities":["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]},'
                    + '{"name":"unity_external_write","catalog_name":"main",'
                    + '"schema_name":"default","table_type":"EXTERNAL",'
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
        } else if (path.endsWith("/tables/unity_external_write")) {
            def latestVersion = java.nio.file.Files.list(
                    unityExternalWriteDirectory.resolve("_delta_log")).withCloseable { files ->
                def versions = files.filter { file ->
                    file.fileName.toString() ==~ /[0-9]{20}\.json/
                }.collect { file ->
                    Long.parseLong(file.fileName.toString().substring(0, 20))
                }
                versions.isEmpty() ? 0L : versions.max()
            }
            sendJson(200, groovy.json.JsonOutput.toJson([
                    metadata: [etag: "unity-write-etag-${latestVersion}",
                            "table-type": "EXTERNAL",
                            "table-uuid": "7fbe2c6d-ec90-43be-9a66-38d31c7de20d",
                            location: unityExternalWriteDirectory.toUri().toString(),
                            "partition-columns": [], "last-commit-version": latestVersion],
                    commits: [], "latest-table-version": latestVersion]))
        } else if (path.endsWith("/tables/catalog_managed")) {
            sendJson(200, catalogManagedResponse)
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
                'delta.write.enabled' = 'true',
                'test_connection' = 'false'
            )
        """

        order_qt_databases "SHOW DATABASES FROM ${catalogName}"
        order_qt_tables "SHOW TABLES FROM ${catalogName}.`default`"
        test {
            sql "SELECT COUNT(*) FROM ${catalogName}.`default`.blocked"
            exception "Table [blocked] does not exist in database"
        }
        order_qt_count "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"
        order_qt_filtered """
            SELECT c_custkey, c_name
            FROM ${catalogName}.`default`.customer
            WHERE c_custkey <= 3
            ORDER BY c_custkey
        """
        order_qt_managed_count "SELECT COUNT(*) FROM ${catalogName}.`default`.managed_customer"
        order_qt_catalog_managed_count """
            SELECT COUNT(*) FROM ${catalogName}.`default`.catalog_managed
        """
        order_qt_unity_external_before "SELECT COUNT(*) FROM ${catalogName}.`default`.unity_external_write"
        sql """
            INSERT INTO ${catalogName}.`default`.unity_external_write VALUES
            (300001, 'Customer#000300001', 'Doris Unity external write', 1,
             '30-300-300-3000', 45.67, 'BUILDING', 'native Unity write')
        """
        order_qt_unity_external_after "SELECT COUNT(*) FROM ${catalogName}.`default`.unity_external_write"
        order_qt_unity_external_inserted """
            SELECT c_custkey, c_name, c_acctbal
            FROM ${catalogName}.`default`.unity_external_write
            WHERE c_custkey = 300001
        """
    } finally {
        server.stop(0)
        java.nio.file.Files.walk(catalogManagedDirectory).withCloseable { paths ->
            paths.sorted(java.util.Comparator.reverseOrder()).forEach {
                java.nio.file.Files.deleteIfExists(it)
            }
        }
        java.nio.file.Files.walk(unityExternalWriteDirectory).withCloseable { paths ->
            paths.sorted(java.util.Comparator.reverseOrder()).forEach {
                java.nio.file.Files.deleteIfExists(it)
            }
        }
    }
}
