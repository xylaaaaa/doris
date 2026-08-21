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

import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class InsertOverwriteTableCommandTest {

    @Test
    void pluginConnectorOverwriteMustTargetTheFullTable() {
        Assertions.assertDoesNotThrow(
                () -> InsertOverwriteTableCommand.requireFullTableConnectorOverwrite(List.of()));
        Assertions.assertThrows(UserException.class,
                () -> InsertOverwriteTableCommand.requireFullTableConnectorOverwrite(
                        List.of("p20260822")));
    }

    @Test
    void copyOnWriteDeleteRequiresOneConsistentSnapshot() {
        ConnectorTableHandle target = new TestConnectorTableHandle();
        ConnectorTableHandle changed = new TestConnectorTableHandle();

        Assertions.assertSame(target,
                InsertOverwriteTableCommand.requireConsistentConnectorOverwriteSnapshot(
                        target, List.of(target, target)));
        Assertions.assertThrows(AnalysisException.class,
                () -> InsertOverwriteTableCommand.requireConsistentConnectorOverwriteSnapshot(
                        target, List.of(changed)));
    }

    private static final class TestConnectorTableHandle implements ConnectorTableHandle {
        private static final long serialVersionUID = 1L;
    }
}
