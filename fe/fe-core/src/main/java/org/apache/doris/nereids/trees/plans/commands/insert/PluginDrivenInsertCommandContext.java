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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Insert command context for plugin-driven connector catalogs.
 * Overwrite is inherited from {@link BaseExternalTableInsertCommandContext}.
 * Connector plugins provide write config through the ConnectorWriteOps SPI.
 */
public class PluginDrivenInsertCommandContext extends BaseExternalTableInsertCommandContext {
    private Optional<ConnectorTableHandle> overwriteBaseHandle = Optional.empty();
    private boolean reportRemovedRows;
    private OptionalLong affectedRowCount = OptionalLong.empty();

    public Optional<ConnectorTableHandle> getOverwriteBaseHandle() {
        return overwriteBaseHandle;
    }

    public void setOverwriteBaseHandle(ConnectorTableHandle overwriteBaseHandle) {
        this.overwriteBaseHandle = Optional.of(overwriteBaseHandle);
    }

    public boolean isReportRemovedRows() {
        return reportRemovedRows;
    }

    public void setReportRemovedRows(boolean reportRemovedRows) {
        this.reportRemovedRows = reportRemovedRows;
    }

    public OptionalLong getAffectedRowCount() {
        return affectedRowCount;
    }

    public void setAffectedRowCount(long affectedRowCount) {
        this.affectedRowCount = OptionalLong.of(affectedRowCount);
    }
}
