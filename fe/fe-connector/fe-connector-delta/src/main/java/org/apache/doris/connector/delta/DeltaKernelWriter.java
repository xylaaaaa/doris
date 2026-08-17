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
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Delta Kernel blind-append lifecycle used by the connector write SPI. */
final class DeltaKernelWriter {

    private static final Logger LOG = LogManager.getLogger(DeltaKernelWriter.class);
    private static final String ENGINE_INFO = "Apache Doris native Delta connector";
    private static final int MAX_COMMIT_RETRIES = 3;

    private final Engine engine;

    DeltaKernelWriter(Engine engine) {
        this.engine = engine;
    }

    DeltaInsertHandle beginInsert(DeltaTableHandle tableHandle) {
        return beginInsert(tableHandle, null);
    }

    DeltaInsertHandle beginInsert(DeltaTableHandle tableHandle, String applicationId) {
        if (!tableHandle.isExternalTable()) {
            throw new UnsupportedOperationException(
                    "Unity managed Delta writes require catalog commits");
        }
        io.delta.kernel.TransactionBuilder transactionBuilder = Table.forPath(
                engine, tableHandle.getTablePath())
                .createTransactionBuilder(engine, ENGINE_INFO, Operation.WRITE)
                .withMaxRetries(MAX_COMMIT_RETRIES);
        if (applicationId != null && !applicationId.isBlank()) {
            transactionBuilder.withTransactionId(engine, applicationId, 0L);
        }
        Transaction transaction = transactionBuilder.build(engine);
        if (transaction.getReadTableVersion() != tableHandle.getSnapshotVersion()) {
            throw new DorisConnectorException("Delta table changed while preparing INSERT: expected version "
                    + tableHandle.getSnapshotVersion() + " but transaction read version "
                    + transaction.getReadTableVersion());
        }
        Row transactionState = transaction.getTransactionState(engine);
        return new DeltaInsertHandle(this, transaction, transactionState);
    }

    void finishInsert(DeltaInsertHandle insertHandle,
            Collection<ConnectorFileCommitInfo> files) {
        if (files.isEmpty()) {
            return;
        }
        Transaction transaction = insertHandle.getTransaction();
        List<String> partitionColumns = transaction.getPartitionColumns(engine);
        StructType schema = transaction.getSchema(engine);
        Set<String> expectedPartitionColumns = Set.copyOf(partitionColumns);
        List<Row> appendActions = new ArrayList<>(files.size());
        for (ConnectorFileCommitInfo file : files) {
            if (!file.getPartitionValues().keySet().equals(expectedPartitionColumns)) {
                throw new DorisConnectorException(
                        "Delta append file partition columns do not match table metadata: expected "
                                + partitionColumns + " but received "
                                + file.getPartitionValues().keySet());
            }
            Map<String, Literal> partitionValues = toPartitionLiterals(
                    schema, partitionColumns, file);
            DataWriteContext writeContext = Transaction.getWriteContext(
                    engine, insertHandle.getTransactionState(), partitionValues);
            DataFileStatus dataFile = new DataFileStatus(file.getFilePath(), file.getFileSize(),
                    file.getModificationTime(), Optional.empty());
            appendActions.addAll(generateAppendActions(
                    insertHandle.getTransactionState(), writeContext, dataFile));
        }

        try (CloseableIterable<Row> actionIterable = CloseableIterable.inMemoryIterable(
                closeableIterator(appendActions.iterator()))) {
            TransactionCommitResult result = transaction.commit(engine, actionIterable);
            runPostCommitHooks(result);
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to close Delta append resources", e);
        }
    }

    private List<Row> generateAppendActions(Row transactionState,
            DataWriteContext writeContext, DataFileStatus dataFile) {
        List<Row> actions = new ArrayList<>(1);
        try (CloseableIterator<DataFileStatus> fileIterator = closeableIterator(
                List.of(dataFile).iterator());
                CloseableIterator<Row> actionIterator = Transaction.generateAppendActions(
                        engine, transactionState, fileIterator, writeContext)) {
            while (actionIterator.hasNext()) {
                actions.add(actionIterator.next());
            }
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to close Delta append resources", e);
        }
        return actions;
    }

    private static Map<String, Literal> toPartitionLiterals(StructType schema,
            List<String> partitionColumns, ConnectorFileCommitInfo file) {
        Map<String, Literal> literals = new LinkedHashMap<>();
        for (String column : partitionColumns) {
            DataType dataType = schema.get(column).getDataType();
            if (file.getNullPartitionColumns().contains(column)) {
                literals.put(column, Literal.ofNull(dataType));
            } else {
                literals.put(column, parsePartitionLiteral(
                        dataType, file.getPartitionValues().get(column), column));
            }
        }
        return literals;
    }

    private static Literal parsePartitionLiteral(
            DataType dataType, String value, String column) {
        try {
            if (dataType instanceof BooleanType) {
                if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
                    return Literal.ofBoolean(true);
                }
                if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
                    return Literal.ofBoolean(false);
                }
                throw new IllegalArgumentException("invalid boolean");
            }
            if (dataType instanceof ByteType) {
                return Literal.ofByte(Byte.parseByte(value));
            }
            if (dataType instanceof ShortType) {
                return Literal.ofShort(Short.parseShort(value));
            }
            if (dataType instanceof IntegerType) {
                return Literal.ofInt(Integer.parseInt(value));
            }
            if (dataType instanceof LongType) {
                return Literal.ofLong(Long.parseLong(value));
            }
            if (dataType instanceof FloatType) {
                return Literal.ofFloat(Float.parseFloat(value));
            }
            if (dataType instanceof DoubleType) {
                return Literal.ofDouble(Double.parseDouble(value));
            }
            if (dataType instanceof DecimalType) {
                DecimalType decimal = (DecimalType) dataType;
                return Literal.ofDecimal(new BigDecimal(value),
                        decimal.getPrecision(), decimal.getScale());
            }
            if (dataType instanceof StringType) {
                return Literal.ofString(value);
            }
            if (dataType instanceof DateType) {
                return Literal.ofDate(Math.toIntExact(LocalDate.parse(value).toEpochDay()));
            }
            if (dataType instanceof TimestampType) {
                return Literal.ofTimestamp(PartitionUtils.tryParseTimestamp(value));
            }
            if (dataType instanceof TimestampNTZType) {
                return Literal.ofTimestampNtz(PartitionUtils.tryParseTimestamp(value));
            }
        } catch (RuntimeException e) {
            throw new DorisConnectorException(
                    "Invalid Delta partition value for column '" + column
                            + "' of type " + dataType, e);
        }
        throw new UnsupportedOperationException(
                "Unsupported Delta partition type for column '" + column + "': " + dataType);
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
