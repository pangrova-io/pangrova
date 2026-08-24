package org.pangrova.coordinator.query.execution.dispatch;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.ExplainStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.spi.connector.data.TypedRow;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Executes statements that require compilation to a Substrait plan and
 * dispatch to native workers: DELETE, UPDATE, MERGE, INSERT SELECT, EXPLAIN.
 *
 * <p>DELETE/UPDATE/MERGE and INSERT SELECT are routed through the native DML
 * dispatch pipeline via {@link NativeDmlCompileFunction}. EXPLAIN compiles
 * the inner statement and returns the plan text.</p>
 */
public final class PlanExecutionFactory implements StatementExecutionFactory<Statement> {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(PlanExecutionFactory.class);

    private final NativeDmlCompileFunction dmlDispatch;
    private final Consumer<String> cacheInvalidator;
    private final BiFunction<String, CoordinatorSessionContext, String> explainCompiler;
    private final org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker;
    private final org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry;
    private final org.pangrova.coordinator.query.security.AccessControlEnforcer accessControlEnforcer;

    /**
     * @param ctx bundled dependencies for plan execution
     */
    public PlanExecutionFactory(PlanExecutionFactoryContext ctx) {
        this.dmlDispatch = checkNotNull(ctx.dmlDispatch(), "dmlDispatch");
        this.cacheInvalidator = ctx.cacheInvalidator();
        this.explainCompiler = checkNotNull(ctx.explainCompiler(), "explainCompiler");
        this.writeCommitTracker = ctx.writeCommitTracker();
        this.catalogRegistry = ctx.catalogRegistry();
        this.accessControlEnforcer = ctx.accessControlEnforcer();
    }

    @Override
    public Set<Class<? extends Statement>> supportedTypes() {
        return Set.of(DeleteStatement.class, UpdateStatement.class,
                MergeStatement.class, InsertSelectStatement.class,
                ExplainStatement.class);
    }

    @Override
    public StatementResult execute(Statement statement, StatementExecutionContext ctx) {
        if (statement instanceof ExplainStatement s) {
            return executeExplain(s, ctx);
        }
        if (statement instanceof InsertSelectStatement s) {
            return executeInsertSelect(s, ctx);
        }
        if (statement instanceof DeleteStatement delStmt) {
            return executeDeleteWithMetadataFallback(delStmt, ctx);
        }
        if (statement instanceof UpdateStatement updStmt) {
            return executeDml(updStmt, ctx);
        }
        if (statement instanceof MergeStatement mergeStmt) {
            return executeDml(mergeStmt, ctx);
        }
        throw new UnsupportedOperationException(
                "Unsupported plan statement: " + statement.getClass().getSimpleName());
    }

    /**
     * Executes DELETE with metadata-level fast paths when possible:
     * <ul>
     *   <li>DELETE ALL (no WHERE): uses connector metadata overwrite or deleteByFilter</li>
     *   <li>DELETE WHERE: attempts connector-native deleteByFilter, falls back to native worker DML</li>
     * </ul>
     */
    private StatementResult executeDeleteWithMetadataFallback(DeleteStatement stmt,
                                                              StatementExecutionContext ctx) {
        org.pangrova.spi.connector.metadata.table.QualifiedTableName ref =
                TableNameExtractor.extractQualified(stmt);
        if (ref == null) {
            return executeDml(stmt, ctx);
        }

        org.pangrova.spi.connector.ConnectorSession connectorSession =
                org.pangrova.coordinator.query.statement.TableResolver.buildSession(ref.catalog());
        org.pangrova.metadata.catalog.CatalogHandle catalogHandle =
                catalogRegistry.getCatalog(ref.catalog());
        org.pangrova.spi.connector.metadata.ConnectorMetadata metadata =
                catalogHandle.getMetadata(connectorSession);
        org.pangrova.spi.connector.metadata.table.ConnectorTableHandle tableHandle =
                org.pangrova.coordinator.query.statement.TableResolver.requireTableHandle(
                        metadata, connectorSession, ref);

        if (stmt.where().isEmpty()) {
            java.util.List<String> partKeys = tableHandle.partitionKeys();
            if (!partKeys.isEmpty()) {
                // Partitioned table: use the connector's efficient metadata-only delete path,
                // exactly as we do for the unpartitioned case. Routing through CoW here would
                // scan the entire table into ArrayList<TypedRow>, which is catastrophically wrong.
                metadata.deleteByFilter(connectorSession, tableHandle,
                        new org.pangrova.spi.connector.metadata.DeleteFilter.All());
                LOG.info("event=metadata_delete_all_partitioned table={} queryId={}", ref,
                        ctx.session().queryId());
                transitionToFinished(ctx.stateMachine());
                return new StatementResult(List.of(), List.of(), ref.toString(), StatementCategory.DML);
            } else {
                var overwriteHandle = metadata.beginOverwrite(connectorSession, tableHandle);
                var deleteSink = catalogHandle.getPageSinkProvider()
                        .createOverwritePageSink(
                                org.pangrova.spi.connector.ConnectorTransactionHandle.EMPTY,
                                connectorSession, overwriteHandle);
                try {
                    byte[] commitMeta = deleteSink.finishWithMetadata();
                    metadata.finishDistributedWrite(connectorSession, overwriteHandle,
                            java.util.List.of(commitMeta));
                } catch (Exception e) {
                    deleteSink.abort();
                    throw e;
                } finally {
                    deleteSink.close();
                }
            }
            LOG.info("event=metadata_delete_all table={} queryId={}", ref, ctx.session().queryId());
            transitionToFinished(ctx.stateMachine());
            return new StatementResult(List.of(), List.of(), ref.toString(), StatementCategory.DML);
        }

        // Copy-on-write DELETE: read all rows, filter out matching, overwrite
        org.pangrova.spi.connector.metadata.DeleteFilter filter =
                org.pangrova.coordinator.query.statement.dml.DmlExecutor.convertToDeleteFilter(stmt.where().get());
        if (filter != null) {
            var tableColumns = metadata.getColumnHandles(connectorSession, tableHandle).keySet();
            validateFilterColumns(filter, tableColumns);

            // Try connector-native deleteByFilter first (e.g., Paimon handles
            // per-partition overwrite with correct merge-on-read semantics)
            try {
                metadata.deleteByFilter(connectorSession, tableHandle, filter);
                LOG.info("event=native_delete_by_filter table={} queryId={}", ref, ctx.session().queryId());
                transitionToFinished(ctx.stateMachine());
                return new StatementResult(List.of(), List.of(), ref.toString(), StatementCategory.DML);
            } catch (UnsupportedOperationException e) {
                // Connector does not support predicate-based delete — route to native worker.
                LOG.info("event=delete_by_filter_unsupported table={} reason={}", ref, e.getMessage());
            } catch (RuntimeException e) {
                // Connector threw an unexpected error — fall back to native worker DML
                // rather than failing the query outright.
                LOG.warn("event=delete_by_filter_error_fallback table={} error={}", ref, e.getMessage(), e);
            }

            // Route to native worker execution instead of coordinator-level CoW
            // to avoid loading the entire table into coordinator JVM heap.
            LOG.info("event=delete_native_fallback table={} queryId={}", ref, ctx.session().queryId());
            return executeDml(stmt, ctx);
        }

        // Fallback: native path for complex WHERE clauses.
        LOG.info("event=delete_fallback_native table={} queryId={}", ref, ctx.session().queryId());
        return executeDml(stmt, ctx);
    }

    // ── Coordinator-level CoW code removed ──
    // All DML (DELETE WHERE, UPDATE, MERGE) now routes through executeDml()
    // to native workers, eliminating the unbounded ArrayList<TypedRow> OOM risk.

    private static void validateFilterColumns(
            org.pangrova.spi.connector.metadata.DeleteFilter filter,
            java.util.Set<String> tableColumns) {
        var referencedColumns = new java.util.HashSet<String>();
        collectFilterColumns(filter, referencedColumns);
        for (String col : referencedColumns) {
            if (tableColumns.stream().noneMatch(tc -> tc.equalsIgnoreCase(col))) {
                throw new org.pangrova.common.exception.PangrovaException(
                        org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Column '" + col + "' does not exist in table. Available columns: "
                                + tableColumns);
            }
        }
    }

    private static void collectFilterColumns(
            org.pangrova.spi.connector.metadata.DeleteFilter filter,
            java.util.Set<String> columns) {
        if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.Equal f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.NotEqual f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.LessThan f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.LessThanOrEqual f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.GreaterThan f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.GreaterThanOrEqual f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.IsNull f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.IsNotNull f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.In f) {
            columns.add(f.column());
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.And f) {
            f.filters().forEach(sub -> collectFilterColumns(sub, columns));
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.Or f) {
            f.filters().forEach(sub -> collectFilterColumns(sub, columns));
        } else if (filter instanceof org.pangrova.spi.connector.metadata.DeleteFilter.Not f) {
            collectFilterColumns(f.filter(), columns);
        }
    }

    private static void transitionToFinished(org.pangrova.coordinator.query.QueryStateMachine sm) {
        if (sm == null) {
            return;
        }
        if (!sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.SCHEDULING)) {
            LOG.warn("event=transition_failed target=SCHEDULING");
            return;
        }
        if (!sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.RUNNING)) {
            LOG.warn("event=transition_failed target=RUNNING");
            return;
        }
        if (!sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHING)) {
            LOG.warn("event=transition_failed target=FINISHING");
            return;
        }
        if (!sm.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED)) {
            LOG.warn("event=transition_failed target=FINISHED");
        }
    }

    private StatementResult executeDml(Statement statement, StatementExecutionContext ctx) {
        registerWriteContextIfConfigured(statement, ctx.session().queryId());
        DispatchResult result = dmlDispatch.apply(ctx.stateMachine(), ctx.session(), statement);
        if (result == null) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "Native DML dispatch returned null for "
                            + statement.getClass().getSimpleName());
        }
        String mutatedTable = TableNameExtractor.extract(statement);
        return new StatementResult(List.of(), List.of(),
                mutatedTable, StatementCategory.DML);
    }

    /**
     * Registers write context with the commit tracker before native DML dispatch.
     */
    private void registerWriteContextIfConfigured(Statement statement, String queryId) {
        if (writeCommitTracker == null || catalogRegistry == null) {
            return;
        }
        if (statement instanceof DeleteStatement delStmt && delStmt.where().isEmpty()) {
            return;
        }
        org.pangrova.spi.connector.metadata.table.QualifiedTableName ref =
                TableNameExtractor.extractQualified(statement);
        if (ref == null) {
            return;
        }
        org.pangrova.spi.connector.ConnectorSession connectorSession =
                org.pangrova.coordinator.query.statement.TableResolver.buildSession(ref.catalog());
        org.pangrova.metadata.catalog.CatalogHandle catalogHandle =
                catalogRegistry.getCatalog(ref.catalog());
        org.pangrova.spi.connector.metadata.ConnectorMetadata metadata =
                catalogHandle.getMetadata(connectorSession);
        org.pangrova.spi.connector.metadata.table.ConnectorTableHandle tableHandle =
                org.pangrova.coordinator.query.statement.TableResolver.requireTableHandle(
                        metadata, connectorSession, ref);
        org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle outputHandle =
                resolveOutputHandle(metadata, connectorSession, tableHandle, statement);
        writeCommitTracker.registerWriteContext(
                queryId, ref.catalog(), outputHandle, connectorSession);
    }

    private static org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle resolveOutputHandle(
            org.pangrova.spi.connector.metadata.ConnectorMetadata metadata,
            org.pangrova.spi.connector.ConnectorSession session,
            org.pangrova.spi.connector.metadata.table.ConnectorTableHandle tableHandle,
            Statement statement) {
        // Choose the connector write-path that matches the actual statement.
        // INSERT INTO SELECT uses beginInsert (append semantics);
        // INSERT OVERWRITE SELECT uses beginOverwrite (replace semantics);
        // All other DML (UPDATE, MERGE, INSERT OVERWRITE VALUES, DELETE) use beginOverwrite.
        // DELETE uses OVERWRITE because DmlOperationConverter.convertDelete() inverts
        // the WHERE filter to produce surviving rows — the write is a full replacement
        // of the affected data files, not a delete-marker operation.
        if (statement instanceof InsertSelectStatement iss) {
            return iss.overwrite()
                    ? metadata.beginOverwrite(session, tableHandle)
                    : metadata.beginInsert(session, tableHandle);
        }
        return metadata.beginOverwrite(session, tableHandle);
    }

    private StatementResult executeInsertSelect(InsertSelectStatement stmt,
                                                 StatementExecutionContext ctx) {
        return executeDml(stmt, ctx);
    }

    private StatementResult executeExplain(ExplainStatement stmt,
                                            StatementExecutionContext ctx) {
        String innerSql = new AstPrinter().print(stmt.innerStatement());
        String planText = explainCompiler.apply(innerSql, ctx.session());
        List<TypedRow> rows = List.of(
                TypedRow.builder().putString("plan", planText).build());
        return new StatementResult(rows, List.of("plan"),
                null, StatementCategory.EXPLAIN);
    }
}
