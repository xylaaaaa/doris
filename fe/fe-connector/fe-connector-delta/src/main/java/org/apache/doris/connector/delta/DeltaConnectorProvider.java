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
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.net.URI;
import java.util.Map;

/** ServiceLoader entry point for the path-based Delta connector. */
public class DeltaConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return DeltaConnectorProperties.TYPE;
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        validateProperties(properties);
        return new DeltaConnector(properties, context);
    }

    @Override
    public void validateProperties(Map<String, String> properties) {
        requireNonBlank(properties, DeltaConnectorProperties.TABLE_PATH);
        requireNonBlank(properties, DeltaConnectorProperties.DATABASE);
        requireNonBlank(properties, DeltaConnectorProperties.TABLE);
        URI tablePath = URI.create(properties.get(DeltaConnectorProperties.TABLE_PATH));
        if (!tablePath.isAbsolute()) {
            throw new IllegalArgumentException("Delta table path must be an absolute URI");
        }
    }

    private static void requireNonBlank(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required Delta catalog property '" + key + "'");
        }
    }
}
