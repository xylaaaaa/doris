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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

import io.delta.kernel.defaults.engine.DefaultEngine;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Path-based Delta connector implementation. */
public final class DeltaConnector implements Connector {

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private final DeltaPathCatalogAdapter catalogAdapter;
    private final DeltaConnectorMetadata metadata;
    private final DeltaScanPlanProvider scanPlanProvider;

    public DeltaConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = immutableCopy(properties);
        this.context = Objects.requireNonNull(context, "context");

        String databaseName = this.properties.get(DeltaConnectorProperties.DATABASE);
        String tableName = this.properties.get(DeltaConnectorProperties.TABLE);
        String tablePath = this.properties.get(DeltaConnectorProperties.TABLE_PATH);
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(buildHadoopConfiguration(this.properties)));
        this.catalogAdapter = new DeltaPathCatalogAdapter(
                databaseName, tableName, tablePath, loader);
        this.metadata = new DeltaConnectorMetadata(catalogAdapter, this.properties);
        this.scanPlanProvider = new DeltaScanPlanProvider(catalogAdapter, this.properties);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return metadata;
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return scanPlanProvider;
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            DeltaKernelSnapshot snapshot = catalogAdapter.loadLatestSnapshot();
            return ConnectorTestResult.success(
                    "Delta snapshot version " + snapshot.getVersion() + " is readable");
        } catch (RuntimeException e) {
            return ConnectorTestResult.failure(e.getMessage());
        }
    }

    /** Returns the adapter for the scan planner and future catalog integrations. */
    public DeltaPathCatalogAdapter getCatalogAdapter() {
        return catalogAdapter;
    }

    /** Returns immutable catalog properties for connector-owned planners. */
    public Map<String, String> getProperties() {
        return properties;
    }

    /** Returns the FE context associated with this connector. */
    public ConnectorContext getContext() {
        return context;
    }

    private static Configuration buildHadoopConfiguration(Map<String, String> properties) {
        Configuration configuration = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (isKernelConfigurationProperty(entry.getKey())) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        return configuration;
    }

    private static boolean isKernelConfigurationProperty(String key) {
        return key.startsWith("hadoop.") || key.startsWith("fs.")
                || key.startsWith("dfs.") || key.startsWith("hive.")
                || key.startsWith("s3.") || key.startsWith("s3a.")
                || key.startsWith("azure.") || key.startsWith("adls.")
                || key.startsWith("gcs.") || key.startsWith("google.")
                || key.startsWith("cos.") || key.startsWith("oss.")
                || key.startsWith("obs.");
    }

    private static Map<String, String> immutableCopy(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public void close() throws IOException {
        // Delta Kernel's DefaultEngine has no close contract.  The connector owns no other
        // closeable resources; the method is retained for Connector lifecycle symmetry.
    }
}
