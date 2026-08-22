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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsTrue;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nullable;

/** Snapshot-pinned copy-on-write UPDATE for plugin connector tables. */
public final class ConnectorUpdateCommand extends Command implements ForwardWithSync, Explainable {
    private final List<String> nameParts;
    private final @Nullable String tableAlias;
    private final List<EqualTo> assignments;
    private final LogicalPlan logicalQuery;
    private final Optional<LogicalPlan> cte;

    /** Creates a connector UPDATE command from the parser-owned target and query plan. */
    public ConnectorUpdateCommand(List<String> nameParts, @Nullable String tableAlias,
            List<EqualTo> assignments, LogicalPlan logicalQuery, Optional<LogicalPlan> cte) {
        super(PlanType.UPDATE_COMMAND);
        this.nameParts = ImmutableList.copyOf(nameParts);
        this.tableAlias = tableAlias;
        this.assignments = ImmutableList.copyOf(assignments);
        this.logicalQuery = logicalQuery;
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        PluginDrivenExternalTable table = requireUpdateTable(ctx);
        ConnectorCopyOnWriteUtils.requireUnrestrictedSource(ctx, table);
        requireReadWritePrivileges(ctx, table);
        UpdatePlan updatePlan = buildUpdatePlan(ctx, table);
        ConnectorTableHandle baseHandle = getCurrentTableHandle(table);
        long affectedRows = countAffectedRows(
                ctx, buildCountSql(baseHandle, updatePlan.predicate));
        if (affectedRows == 0) {
            ctx.getState().setOk(0, 0, "");
            ctx.updateReturnRows(0);
            return;
        }
        InsertOverwriteTableCommand overwrite = buildOverwriteCommand(updatePlan.query);
        overwrite.setConnectorOverwriteBaseHandle(baseHandle);
        overwrite.setConnectorAffectedRowCount(affectedRows);
        overwrite.run(ctx, executor);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        PluginDrivenExternalTable table = requireUpdateTable(ctx);
        ConnectorCopyOnWriteUtils.requireUnrestrictedSource(ctx, table);
        return buildOverwriteCommand(buildUpdatePlan(ctx, table).query).getExplainPlan(ctx);
    }

    private PluginDrivenExternalTable requireUpdateTable(ConnectContext ctx) {
        List<String> qualifiedName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedName, ctx.getEnv(), Optional.empty());
        if (!(table instanceof PluginDrivenExternalTable)) {
            throw new AnalysisException("Connector UPDATE requires a plugin-driven external table");
        }
        PluginDrivenExternalTable connectorTable = (PluginDrivenExternalTable) table;
        if (!connectorTable.supportsUpdate()) {
            throw new AnalysisException("Connector does not support UPDATE for table: "
                    + connectorTable.getName());
        }
        return connectorTable;
    }

    private static void requireReadWritePrivileges(
            ConnectContext ctx, PluginDrivenExternalTable table) {
        String catalogName = table.getDatabase().getCatalog().getName();
        String databaseName = table.getDatabase().getFullName();
        boolean canRead = ctx.getEnv().getAccessManager().checkTblPriv(
                ctx, catalogName, databaseName, table.getName(), PrivPredicate.SELECT);
        boolean canWrite = ctx.getEnv().getAccessManager().checkTblPriv(
                ctx, catalogName, databaseName, table.getName(), PrivPredicate.LOAD);
        if (!canRead || !canWrite) {
            throw new AnalysisException(
                    "Connector copy-on-write UPDATE requires SELECT and LOAD privileges on "
                            + catalogName + "." + databaseName + "." + table.getName());
        }
    }

    private ConnectorTableHandle getCurrentTableHandle(PluginDrivenExternalTable table) {
        PluginDrivenExternalCatalog catalog =
                (PluginDrivenExternalCatalog) table.getCatalog();
        Connector connector = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        return metadata.getTableHandle(
                session, table.getRemoteDbName(), table.getRemoteName())
                .orElseThrow(() -> new AnalysisException("Table not found while planning UPDATE: "
                        + table.getRemoteDbName() + "." + table.getRemoteName()));
    }

    UpdatePlan buildUpdatePlan(
            ConnectContext ctx, PluginDrivenExternalTable table) {
        requireSimpleUpdateShape(logicalQuery, cte);
        Optional<Expression> predicate = Optional.empty();
        LogicalPlan source = logicalQuery;
        if (logicalQuery instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) logicalQuery;
            predicate = Optional.of(filter.getPredicate());
            source = (LogicalPlan) filter.child();
        }
        predicate.ifPresent(ConnectorUpdateCommand::requireDeterministicPredicate);

        Map<String, Expression> updates = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : assignments) {
            List<String> columnParts = ((UnboundSlot) assignment.left()).getNameParts();
            UpdateCommand.checkAssignmentColumn(
                    ctx, columnParts, nameParts, tableAlias);
            updates.put(columnParts.get(columnParts.size() - 1), assignment.right());
        }

        String sourceTableName = tableAlias != null
                ? tableAlias : Util.getTempTableDisplayName(table.getName());
        List<NamedExpression> projects = new ArrayList<>();
        for (Column column : table.getBaseSchema(true)) {
            if (!column.isVisible()) {
                continue;
            }
            UnboundSlot original = new UnboundSlot(sourceTableName, column.getName());
            Expression assignment = updates.remove(column.getName());
            if (assignment == null) {
                projects.add(original);
                continue;
            }
            Expression updated = predicate
                    .<Expression>map(value -> new If(new IsTrue(value), assignment, original))
                    .orElse(assignment);
            projects.add(new UnboundAlias(updated, column.getName()));
        }
        if (!updates.isEmpty()) {
            throw new AnalysisException("unknown column in assignment list: "
                    + String.join(", ", updates.keySet()));
        }
        return new UpdatePlan(new LogicalProject<>(projects, source), predicate);
    }

    private InsertOverwriteTableCommand buildOverwriteCommand(LogicalPlan updateQuery) {
        LogicalPlan sink = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                nameParts, ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of(),
                false, TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.UPDATE, updateQuery);
        return new InsertOverwriteTableCommand(
                sink, Optional.empty(), Optional.empty(), Optional.empty());
    }

    static void requireSimpleUpdateShape(
            LogicalPlan query, Optional<LogicalPlan> cte) {
        boolean unsupportedPlan = cte.isPresent()
                || query.anyMatch(plan -> plan instanceof LogicalJoin
                        || plan instanceof LogicalLimit
                        || plan instanceof LogicalSort)
                || query.collect(UnboundRelation.class::isInstance).size() != 1;
        if (unsupportedPlan) {
            throw new AnalysisException(
                    "Connector copy-on-write UPDATE does not support FROM, CTE, subquery, "
                            + "ORDER BY, or LIMIT");
        }
    }

    private static void requireDeterministicPredicate(Expression predicate) {
        if (predicate.anyMatch(expression -> expression instanceof SubqueryExpr)
                || predicate.containsNondeterministic()) {
            throw new AnalysisException(
                    "Connector copy-on-write UPDATE requires a deterministic predicate without subqueries");
        }
    }

    String buildCountSql(
            ConnectorTableHandle baseHandle, Optional<Expression> predicate) {
        OptionalLong version = baseHandle.getCopyOnWriteSnapshotVersion();
        if (!version.isPresent()) {
            throw new AnalysisException(
                    "Connector UPDATE requires a numeric snapshot version for affected-row counting");
        }
        String tableName = ConnectorCopyOnWriteUtils.quoteQualifiedName(nameParts);
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(tableName)
                .append(" FOR VERSION AS OF ")
                .append(version.getAsLong());
        if (tableAlias != null) {
            sql.append(" AS ").append(ConnectorCopyOnWriteUtils.quoteIdentifier(tableAlias));
        }
        predicate.ifPresent(value -> sql.append(" WHERE ").append(value.toSql()));
        return sql.toString();
    }

    static long countAffectedRows(ConnectContext ctx, String sql) {
        return ConnectorCopyOnWriteUtils.countRows(ctx, sql, "UPDATE");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.UPDATE;
    }

    static final class UpdatePlan {
        private final LogicalPlan query;
        private final Optional<Expression> predicate;

        private UpdatePlan(LogicalPlan query, Optional<Expression> predicate) {
            this.query = query;
            this.predicate = predicate;
        }

        LogicalPlan getQuery() {
            return query;
        }

        Optional<Expression> getPredicate() {
            return predicate;
        }
    }
}
