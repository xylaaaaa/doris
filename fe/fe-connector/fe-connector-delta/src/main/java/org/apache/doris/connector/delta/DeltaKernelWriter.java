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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.write.ConnectorFileCommitInfo;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** Delta Kernel blind-append lifecycle used by the connector write SPI. */
final class DeltaKernelWriter {

    private static final Logger LOG = LogManager.getLogger(DeltaKernelWriter.class);
    private static final String ENGINE_INFO = "Apache Doris native Delta connector";

    private final Engine engine;

    DeltaKernelWriter(Engine engine) {
        this.engine = engine;
    }

    DeltaInsertHandle beginInsert(DeltaTableHandle tableHandle) {
        if (tableHandle.isCatalogManaged()) {
            throw new UnsupportedOperationException(
                    "Catalog-managed Delta writes require Unity catalog commits");
        }
        Transaction transaction = Table.forPath(engine, tableHandle.getTablePath())
                .createTransactionBuilder(engine, ENGINE_INFO, Operation.WRITE)
                .build(engine);
        if (transaction.getReadTableVersion() != tableHandle.getSnapshotVersion()) {
            throw new DorisConnectorException("Delta table changed while preparing INSERT: expected version "
                    + tableHandle.getSnapshotVersion() + " but transaction read version "
                    + transaction.getReadTableVersion());
        }
        List<String> partitionColumns = transaction.getPartitionColumns(engine);
        if (!partitionColumns.isEmpty()) {
            throw new UnsupportedOperationException(
                    "The initial native Delta writer supports only unpartitioned tables");
        }
        Row transactionState = transaction.getTransactionState(engine);
        DataWriteContext writeContext = Transaction.getWriteContext(
                engine, transactionState, Collections.emptyMap());
        return new DeltaInsertHandle(transaction, transactionState, writeContext);
    }

    void finishInsert(DeltaInsertHandle insertHandle,
            Collection<ConnectorFileCommitInfo> files) {
        if (files.isEmpty()) {
            return;
        }
        List<DataFileStatus> dataFiles = new ArrayList<>(files.size());
        for (ConnectorFileCommitInfo file : files) {
            if (!file.getPartitionValues().isEmpty()) {
                throw new IllegalArgumentException(
                        "Unpartitioned Delta append received partition values for "
                                + file.getFilePath());
            }
            dataFiles.add(new DataFileStatus(file.getFilePath(), file.getFileSize(),
                    file.getModificationTime(), Optional.empty()));
        }

        try (CloseableIterator<DataFileStatus> fileIterator = closeableIterator(dataFiles.iterator());
                CloseableIterator<Row> actions = Transaction.generateAppendActions(
                        engine, insertHandle.getTransactionState(), fileIterator,
                        insertHandle.getWriteContext());
                CloseableIterable<Row> actionIterable = CloseableIterable.inMemoryIterable(actions)) {
            TransactionCommitResult result = insertHandle.getTransaction().commit(engine, actionIterable);
            runPostCommitHooks(result);
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to close Delta append resources", e);
        }
    }

    private void runPostCommitHooks(TransactionCommitResult result) {
        for (PostCommitHook hook : result.getPostCommitHooks()) {
            try {
                hook.threadSafeInvoke(engine);
            } catch (IOException e) {
                LOG.warn("Delta version {} committed, but post-commit hook {} failed",
                        result.getVersion(), hook.getType(), e);
            }
        }
    }

    private static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator) {
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void close() {
            }
        };
    }
}
