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

package org.apache.doris.connector.api;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Complete connector-neutral request for creating an external table. */
public final class ConnectorTableCreateRequest {

    private final String databaseName;
    private final ConnectorTableSchema tableSchema;
    private final List<String> partitionColumns;
    private final Map<String, String> properties;
    private final String comment;
    private final boolean ifNotExists;

    public ConnectorTableCreateRequest(String databaseName,
            ConnectorTableSchema tableSchema, List<String> partitionColumns,
            Map<String, String> properties, String comment, boolean ifNotExists) {
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
        this.partitionColumns = List.copyOf(partitionColumns);
        this.properties = Map.copyOf(properties);
        this.comment = comment;
        this.ifNotExists = ifNotExists;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public ConnectorTableSchema getTableSchema() {
        return tableSchema;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }
}
