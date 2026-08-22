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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

/** Safety checks shared by connector copy-on-write DML commands. */
final class ConnectorCopyOnWriteUtils {
    private ConnectorCopyOnWriteUtils() {
    }

    static void requireUnrestrictedSource(
            ConnectContext ctx, PluginDrivenExternalTable table) {
        if (ctx.getCurrentUserIdentity().isRootUser()
                || ctx.getCurrentUserIdentity().isAdminUser()) {
            return;
        }
        AccessControllerManager accessManager = ctx.getEnv().getAccessManager();
        String catalogName = table.getDatabase().getCatalog().getName();
        String databaseName = table.getDatabase().getFullName();
        String tableName = table.getName();
        if (!accessManager.evalRowFilterPolicies(ctx.getCurrentUserIdentity(),
                catalogName, databaseName, tableName).isEmpty()) {
            throw new AnalysisException(
                    "Connector copy-on-write DML does not support row filter policies");
        }
        boolean hasDataMask = table.getBaseSchema(true).stream().anyMatch(column ->
                accessManager.evalDataMaskPolicy(ctx.getCurrentUserIdentity(), catalogName,
                        databaseName, tableName, column.getName()).isPresent());
        if (hasDataMask) {
            throw new AnalysisException(
                    "Connector copy-on-write DML does not support data masking policies");
        }
    }
}
