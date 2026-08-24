package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.CoordinatorMemoryGuard;
import org.pangrova.coordinator.query.execution.dispatch.NativeDmlCompileFunction;
import org.pangrova.coordinator.query.task.NativeWriteCommitTracker;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.InExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Routes DML statements to specialized executors.
 *
 * <p>Handles INSERT and INSERT OVERWRITE via coordinator-local PageSink.
 * DELETE, UPDATE, MERGE, and INSERT SELECT are compiled to Substrait plans
 * and dispatched to native workers via {@link NativeDmlCompileFunction}.
 *
 * <p><b>Current mode: coordinator-local DML.</b> INSERT and INSERT OVERWRITE
 * execute within the coordinator process. DELETE and UPDATE are compiled
 * to Substrait plans and dispatched to native workers via
 * {@link NativeDmlCompileFunction}.
 */
public final class DmlExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DmlExecutor.class);

    private final CatalogRegistry catalogRegistry;
    private final Consumer<String> writeCallback;
    private final NativeDmlCompileFunction compileAndDispatch;
    private final NativeWriteCommitTracker writeCommitTracker;
    private final InsertSelectExecutor insertSelectExecutor;
    private final CoordinatorMemoryGuard memoryGuard;
    private final int maxConcurrentDml;
    final Semaphore dmlConcurrency;
    private final long dmlTimeoutMs;

    /** Estimated bytes per TypedRow cell (map entry + ColumnValue + key). */
    private static final long ESTIMATED_BYTES_PER_CELL = 128L;

    /** Test-only constructor with default config. */
    DmlExecutor(CatalogRegistry catalogRegistry) {
        this(catalogRegistry, DmlExecutorConfig.defaults());
    }

    /** Primary constructor accepting config bundle. */
    public DmlExecutor(CatalogRegistry catalogRegistry, DmlExecutorConfig config) {
        this.catalogRegistry = catalogRegistry;
        this.writeCallback = config.writeCallback();
        this.compileAndDispatch = config.compileAndDispatch();
        this.writeCommitTracker = config.writeCommitTracker();
        this.memoryGuard = config.memoryGuard();
        this.insertSelectExecutor = new InsertSelectExecutor(catalogRegistry, config.writeCallback());
        this.maxConcurrentDml = config.maxConcurrentDml();
        this.dmlConcurrency = new Semaphore(config.maxConcurrentDml());
        this.dmlTimeoutMs = config.dmlTimeoutMs();
    }

    /**
     * Acquires a DML concurrency permit, throwing if the timeout expires.
     *
     * <p>The returned {@link DmlPermit} is an {@link AutoCloseable} that releases
     * the permit exactly once when closed. Always use try-with-resources to bind
     * the permit's lifetime to a single block, eliminating the risk of double-release
     * or leaked permits if a refactor changes the layout of the calling method.</p>
     */
    DmlPermit acquireDmlPermit() {
        try {
            if (!dmlConcurrency.tryAcquire(dmlTimeoutMs, TimeUnit.MILLISECONDS)) {
                // BUG-D48 fix: use stored maxConcurrentDml field instead of
                // availablePermits + queueLength which gives incorrect value
                throw new IllegalStateException(
                        "DML concurrency limit reached (max="
                                + maxConcurrentDml
                                + "), timed out after " + dmlTimeoutMs + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DML permit acquisition interrupted", e);
        }
        return new DmlPermit(dmlConcurrency);
    }

    /**
     * Idempotent permit handle. Released exactly once via {@link #close()}.
     */
    static final class DmlPermit implements AutoCloseable {
        private final Semaphore semaphore;
        private boolean released;

        DmlPermit(Semaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void close() {
            if (!released) {
                released = true;
                semaphore.release();
            }
        }
    }

    // ── INSERT INTO ──

    public List<TypedRow> executeInsert(InsertStatement stmt) {
        try (DmlPermit ignored = acquireDmlPermit()) {
            QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
            ConnectorSession session = TableResolver.buildSession(ref.catalog());
            CatalogHandle catalogHandle = catalogRegistry.getCatalog(ref.catalog());
            ConnectorMetadata metadata = catalogHandle.getMetadata(session);

            ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(metadata, session, ref);
            List<String> columns = TableResolver.resolveInsertColumns(
                    stmt.columns(), metadata, session, tableHandle);
            Page page = PageBuilder.buildPage(columns, stmt.values());
            DmlValidator.validateNoNanOrInfinity(page);
            // F-MM-GOV: enforce per-table FILE MIME whitelist before any write.
            FileColumnMimeValidator.validate(
                    metadata.getTableMetadata(session, tableHandle), columns, stmt.values());

            writeTrackedPage(catalogHandle, session, metadata, tableHandle, page);
            notifyWrite(ref);
            LOG.info("Inserted {} row(s) into {}", stmt.values().size(), ref);
            return List.of();
        }
    }

    private void writeTrackedPage(CatalogHandle catalogHandle, ConnectorSession session,
                                  ConnectorMetadata metadata, ConnectorTableHandle tableHandle,
                                  Page page) {
        long estimatedBytes = estimatePageBytes(page);
        trackAllocation(estimatedBytes);
        try {
            ConnectorOutputTableHandle outputHandle = metadata.beginInsert(session, tableHandle);
            DmlSinkOperations.writePageViaSink(catalogHandle, session, outputHandle, page);
        } finally {
            trackDeallocation(estimatedBytes);
        }
    }

    // ── INSERT OVERWRITE ──

    public List<TypedRow> executeInsertOverwrite(InsertOverwriteStatement stmt) {
        try (DmlPermit ignored = acquireDmlPermit()) {
            QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
            ConnectorSession session = buildPartitionSession(ref.catalog());
            CatalogHandle catalogHandle = catalogRegistry.getCatalog(ref.catalog());
            ConnectorMetadata metadata = catalogHandle.getMetadata(session);

            ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(metadata, session, ref);
            List<String> columns = TableResolver.resolveInsertColumns(
                    stmt.columns(), metadata, session, tableHandle);
            Page page = PageBuilder.buildPage(columns, stmt.values());
            DmlValidator.validateNoNanOrInfinity(page);
            // F-MM-GOV: enforce per-table FILE MIME whitelist before any write.
            FileColumnMimeValidator.validate(
                    metadata.getTableMetadata(session, tableHandle), columns, stmt.values());

            java.util.Map<String, String> staticPartition = resolvePartitionSpec(stmt.partitionSpec());
            writeTrackedOverwritePage(catalogHandle, session, metadata, tableHandle, page, staticPartition);
            notifyWrite(ref);
            LOG.info("Insert-overwrite {} row(s) into {}", stmt.values().size(), ref);
            return List.of();
        }
    }

    private static java.util.Map<String, String> resolvePartitionSpec(
            java.util.Optional<java.util.Map<String, org.pangrova.parser.ast.Literal>> specOpt) {
        return specOpt
                .map(spec -> spec.entrySet().stream()
                        .collect(java.util.stream.Collectors.toMap(
                                java.util.Map.Entry::getKey,
                                e -> e.getValue().value().toString())))
                .orElse(java.util.Map.of());
    }

    private void writeTrackedOverwritePage(CatalogHandle catalogHandle, ConnectorSession session,
                                           ConnectorMetadata metadata, ConnectorTableHandle tableHandle,
                                           Page page, java.util.Map<String, String> staticPartition) {
        long estimatedBytes = estimatePageBytes(page);
        trackAllocation(estimatedBytes);
        try {
            ConnectorOutputTableHandle outputHandle = staticPartition.isEmpty()
                    ? metadata.beginOverwrite(session, tableHandle)
                    : metadata.beginOverwrite(session, tableHandle, staticPartition);
            DmlSinkOperations.writePageViaOverwriteSink(catalogHandle, session, outputHandle, page);
        } finally {
            trackDeallocation(estimatedBytes);
        }
    }

    // ── DELETE ──

    /**
     * Executes DELETE via metadata-level row filter when possible,
     * falling back to native copy-on-write for complex WHERE clauses.
     *
     * <p>Simple WHERE conditions (equality, IS NULL, AND/OR combinations)
     * are pushed down to the connector's deleteByFilter which performs the
     * delete at the metadata level without requiring a full table rewrite.</p>
     */
    public List<TypedRow> executeDelete(DeleteStatement stmt,
                                        QueryStateMachine sm,
                                        CoordinatorSessionContext session) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
        try (DmlPermit ignored = acquireDmlPermit()) {
            var connectorSession = TableResolver.buildSession(ref.catalog());
            var catalogHandle = catalogRegistry.getCatalog(ref.catalog());
            var metadata = catalogHandle.getMetadata(connectorSession);
            var tableHandle = TableResolver.requireTableHandle(metadata, connectorSession, ref);

            if (stmt.where().isEmpty()) {
                // DELETE ALL: pure metadata operation via connector deleteByFilter(all).
                metadata.deleteByFilter(connectorSession, tableHandle,
                        new DeleteFilter.All());
                notifyWrite(ref);
                LOG.info("action=delete-all table={} queryId={}", ref, session.queryId());
                sm.tryTransition(QueryStateMachine.State.SCHEDULING);
                sm.tryTransition(QueryStateMachine.State.RUNNING);
                sm.tryTransition(QueryStateMachine.State.FINISHING);
                sm.tryTransition(QueryStateMachine.State.FINISHED);
                return List.of();
            }

            // Try metadata-level delete: convert WHERE to DeleteFilter.
            DeleteFilter filter = convertToDeleteFilter(stmt.where().get());
            if (filter != null) {
                metadata.deleteByFilter(connectorSession, tableHandle, filter);
                notifyWrite(ref);
                LOG.info("action=metadata-delete table={} queryId={}", ref, session.queryId());
                sm.tryTransition(QueryStateMachine.State.SCHEDULING);
                sm.tryTransition(QueryStateMachine.State.RUNNING);
                sm.tryTransition(QueryStateMachine.State.FINISHING);
                sm.tryTransition(QueryStateMachine.State.FINISHED);
                return List.of();
            }

            // Fallback: native copy-on-write path for complex WHERE clauses.
            if (compileAndDispatch == null) {
                throw new IllegalStateException("DELETE WHERE requires native execution engine");
            }
            registerNativeWriteContext(ref, session.queryId());
            compileAndDispatch.apply(sm, session, stmt);
            notifyWrite(ref);
            LOG.info("action=native-delete table={} queryId={}", ref, session.queryId());
            return List.of();
        }
    }

    /**
     * Converts a parser Expression to a DeleteFilter for metadata-level deletion.
     * Returns null if the expression is too complex to convert.
     */
    public static DeleteFilter convertToDeleteFilter(Expression expr) {
        if (expr instanceof BinaryExpression binary) {
            return convertBinaryToDeleteFilter(binary);
        }
        if (expr instanceof IsNullExpression isNull) {
            String column = extractColumnName(isNull.operand());
            if (column == null) return null;
            return isNull.negated() ? new DeleteFilter.IsNotNull(column)
                                    : new DeleteFilter.IsNull(column);
        }
        if (expr instanceof NotExpression not) {
            DeleteFilter inner = convertToDeleteFilter(not.expression());
            return inner != null ? new DeleteFilter.Not(inner) : null;
        }
        if (expr instanceof InExpression inExpr) {
            String column = extractColumnName(inExpr.value());
            if (column == null) return null;
            List<Object> values = inExpr.list().stream()
                    .map(DmlExecutor::extractLiteralValue)
                    .toList();
            if (values.contains(null)) return null;
            return new DeleteFilter.In(column, values);
        }
        if (expr instanceof NotInExpression notInExpr) {
            String column = extractColumnName(notInExpr.value());
            if (column == null) return null;
            List<Object> values = notInExpr.list().stream()
                    .map(DmlExecutor::extractLiteralValue)
                    .toList();
            if (values.contains(null)) return null;
            return new DeleteFilter.Not(new DeleteFilter.In(column, values));
        }
        return null;
    }

    private static DeleteFilter convertBinaryToDeleteFilter(BinaryExpression binary) {
        // Handle AND / OR
        if (binary.operator() == BinaryExpression.Operator.AND) {
            DeleteFilter left = convertToDeleteFilter(binary.left());
            DeleteFilter right = convertToDeleteFilter(binary.right());
            if (left == null || right == null) return null;
            return new DeleteFilter.And(List.of(left, right));
        }
        if (binary.operator() == BinaryExpression.Operator.OR) {
            DeleteFilter left = convertToDeleteFilter(binary.left());
            DeleteFilter right = convertToDeleteFilter(binary.right());
            if (left == null || right == null) return null;
            return new DeleteFilter.Or(List.of(left, right));
        }
        // Handle comparison operators
        String column = extractColumnName(binary.left());
        Object value = extractLiteralValue(binary.right());
        // Try reversed: value op column
        if (column == null && value == null) return null;
        if (column == null) {
            column = extractColumnName(binary.right());
            value = extractLiteralValue(binary.left());
        }
        if (column == null || value == null) return null;
        return switch (binary.operator()) {
            case EQUAL -> new DeleteFilter.Equal(column, value);
            case NOT_EQUAL -> new DeleteFilter.NotEqual(column, value);
            case LESS_THAN -> new DeleteFilter.LessThan(column, value);
            case LESS_THAN_OR_EQUAL -> new DeleteFilter.LessThanOrEqual(column, value);
            case GREATER_THAN -> new DeleteFilter.GreaterThan(column, value);
            case GREATER_THAN_OR_EQUAL -> new DeleteFilter.GreaterThanOrEqual(column, value);
            default -> null;
        };
    }

    private static String extractColumnName(Expression expr) {
        if (expr instanceof Identifier id) {
            return id.parts().get(id.parts().size() - 1);
        }
        return null;
    }

    private static Object extractLiteralValue(Expression expr) {
        if (expr instanceof Literal lit) {
            LiteralValue lv = lit.value();
            if (lv instanceof LiteralValue.IntegerValue iv) return iv.value();
            if (lv instanceof LiteralValue.StringValue sv) return sv.value();
            if (lv instanceof LiteralValue.DecimalValue dv) return dv.value();
            if (lv instanceof LiteralValue.BooleanValue bv) return bv.value();
        }
        return null;
    }

    // ── UPDATE ──

    /**
     * Executes UPDATE via native workers with full query lifecycle context.
     * Registers write context for commit metadata collection before dispatch.
     *
     * @throws IllegalStateException if the native dispatch function is not configured
     */
    public List<TypedRow> executeUpdate(UpdateStatement stmt,
                                        QueryStateMachine sm,
                                        CoordinatorSessionContext session) {
        if (compileAndDispatch == null) {
            throw new IllegalStateException("UPDATE requires native execution engine");
        }
        try (DmlPermit ignored = acquireDmlPermit()) {
            QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
            registerNativeWriteContext(ref, session.queryId());
            compileAndDispatch.apply(sm, session, stmt);
            notifyWrite(ref);
            LOG.info("action=native-update table={} queryId={}", ref, session.queryId());
            return List.of();
        }
    }

    // ── MERGE ──

    /**
     * Executes MERGE via native workers with full query lifecycle context.
     * Registers write context for commit metadata collection before dispatch.
     *
     * @throws IllegalStateException if the native dispatch function is not configured
     */
    public List<TypedRow> executeMerge(MergeStatement stmt,
                                       QueryStateMachine sm,
                                       CoordinatorSessionContext session) {
        if (compileAndDispatch == null) {
            throw new IllegalStateException("MERGE requires native execution engine");
        }
        return executeMergeNative(stmt, sm, session);
    }

    private List<TypedRow> executeMergeNative(MergeStatement stmt,
                                              QueryStateMachine sm,
                                              CoordinatorSessionContext session) {
        try (DmlPermit ignored = acquireDmlPermit()) {
            QualifiedTableName ref = TableResolver.resolveTableRef(stmt.targetTable().parts());
            registerNativeWriteContext(ref, session.queryId());
            compileAndDispatch.apply(sm, session, stmt);
            notifyWrite(ref);
            LOG.info("action=native-merge table={} queryId={}", ref, session.queryId());
            return List.of();
        }
    }

    // ── INSERT SELECT ──
    // INSERT SELECT is handled by PlanExecutionFactory via the
    // InsertSelectDispatchPipeline (worker-direct-write mode). The SELECT is compiled
    // to a Substrait plan and dispatched to native workers; workers write
    // directly to the target connector. No coordinator-local execution path.

    // ── INSERT SELECT (write phase) ──

    public void writeInsertSelectRows(InsertSelectStatement stmt,
                               Page page,
                               List<String> selectOutputColumns) {
        insertSelectExecutor.writeInsertSelectRows(stmt, page, selectOutputColumns);
    }

    // ── Helpers ──

    /**
     * Registers write context with the commit tracker before native DML dispatch.
     * This enables TaskStatusReceiver to collect commitMetadata from worker
     * task status reports and call finishDistributedWrite() on query completion.
     *
     * <p>Uses {@code beginDelete()} for all row-level mutation types (DELETE, UPDATE, MERGE)
     * because the SPI models all row-level mutations through the same handle type.
     * The connector treats UPDATE as delete+insert and MERGE as a combination
     * of delete/insert operations — the output handle captures the base snapshot for
     * conflict detection, which is the same regardless of mutation type.</p>
     */
    private void registerNativeWriteContext(QualifiedTableName ref, String queryId) {
        if (writeCommitTracker == null) {
            LOG.debug("action=skip-write-context-registration queryId={} reason=tracker_not_configured", queryId);
            return;
        }
        ConnectorSession connectorSession = TableResolver.buildSession(ref.catalog());
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(ref.catalog());
        ConnectorMetadata metadata = catalogHandle.getMetadata(connectorSession);
        ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(metadata, connectorSession, ref);
        ConnectorOutputTableHandle outputHandle = metadata.beginDelete(connectorSession, tableHandle);
        writeCommitTracker.registerWriteContext(queryId, ref.catalog(), outputHandle, connectorSession);
        LOG.info("action=write-context-registered queryId={} table={}", queryId, ref);
    }

    private static ConnectorSession buildPartitionSession(String catalog) {
        return TableResolver.buildSession(catalog);
    }

    private void notifyWrite(QualifiedTableName ref) {
        if (writeCallback != null) {
            writeCallback.accept(ref.toString());
        }
    }

    private void trackAllocation(long bytes) {
        if (memoryGuard != null) {
            memoryGuard.trackAllocation(bytes);
        }
    }

    private void trackDeallocation(long bytes) {
        if (memoryGuard != null) {
            memoryGuard.trackDeallocation(bytes);
        }
    }

    static long estimatePageBytes(Page page) {
        return (long) page.getPositionCount() * page.columns().size() * ESTIMATED_BYTES_PER_CELL;
    }
}
