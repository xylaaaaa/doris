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

import org.apache.doris.connector.api.handle.ConnectorInsertHandle;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;

/** FE-owned Delta transaction state for one blind append. */
final class DeltaInsertHandle implements ConnectorInsertHandle {

    private final Transaction transaction;
    private final Row transactionState;
    private final DataWriteContext writeContext;

    DeltaInsertHandle(Transaction transaction, Row transactionState,
            DataWriteContext writeContext) {
        this.transaction = transaction;
        this.transactionState = transactionState;
        this.writeContext = writeContext;
    }

    Transaction getTransaction() {
        return transaction;
    }

    Row getTransactionState() {
        return transactionState;
    }

    DataWriteContext getWriteContext() {
        return writeContext;
    }
}
