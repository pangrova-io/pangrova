package org.pangrova.coordinator.query.execution;

import org.pangrova.planner.analysis.AuthorizedRelation;
import org.pangrova.planner.substrait.CompiledScanSchemas;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.session.SessionTableNames;
import org.pangrova.query.api.enums.QueryState;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.coordinator.query.execution.phase.DistributedDispatchPhase;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionContext;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;
import org.pangrova.coordinator.query.execution.phase.QueryLifecycleCoordinator;
import org.pangrova.coordinator.query.execution.phase.StatementDispatchPhase;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.connector.api.metadata.ConnectorReadDiscovery;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.observability.sdk.tracing.QueryTracer;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.IncrementalReadStatement;
import org.pangrova.parser.syntax.TimeTravelSqlClauses;
import org.pangrova.parser.syntax.TimeTravelSqlClauses.Analysis;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.planner.analysis.PlannedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the core query execution pipeline: compiling SQL, dispatching DML statements,
 * and coordinating the phase-based execution flow.
 *
 * <p>Thread safety: stateless beyond references to thread-safe collaborators.</p>
 */
@org.pangrova.common.concurrency.ThreadSafe
final class QueryPipelineExecutor {

    private static final int SQL_PREVIEW_LIMIT = 80;

    private static final Logger LOG = LoggerFactory.getLogger(QueryPipelineExecutor.class);

    private final SecurityPolicyApplier securityPolicyApplier;
    private final StatementDispatcher statementDispatcher;
    private final QueryTracer queryTracer;
    private final QueryLineageCapture lineageCapture;
    private final List<QueryExecutionPhase> pipelinePhases;
    private final QuerySchedulingEngine schedulingEngine;
    private final CatalogRegistry catalogRegistry;
    private final ViewExpander viewExpander;
    private volatile org.pangrova.planner.advisor.QueryPatternRecorder patternRecorder;

    QueryPipelineExecutor(SecurityPolicyApplier securityPolicyApplier,
                          StatementDispatcher statementDispatcher,
                          QueryTracer queryTracer,
                          QueryLineageCapture lineageCapture,
                          QuerySchedulingEngine schedulingEngine,
                          CatalogRegistry catalogRegistry) {
        this.securityPolicyApplier = securityPolicyApplier;
        this.statementDispatcher = statementDispatcher;
        this.queryTracer = queryTracer;
        this.lineageCapture = lineageCapture;
        this.schedulingEngine = schedulingEngine;
        this.catalogRegistry = catalogRegistry;
        this.viewExpander = new ViewExpander(catalogRegistry);
        this.pipelinePhases = buildPipeline();
    }

    void discardControlResult(String queryId) {
        if (statementDispatcher != null) statementDispatcher.discardControlResult(queryId);
    }

    // ── Pipeline execution ──

    void setPatternRecorder(org.pangrova.planner.advisor.QueryPatternRecorder recorder) {
        this.patternRecorder = recorder;
    }

    /**
     * Executes statement dispatch, protected compilation, and distributed dispatch.
     */
    CompletionStage<DispatchResult> doExecute(QueryStateMachine sm, String sql, QuerySession session) {
        if (!sm.tryTransition(QueryState.PLANNING)) {
            return CompletableFuture.completedFuture(null);
        }
        Span querySpan = queryTracer != null
                ? queryTracer.startQuerySpan(session.queryId(), sql) : null;
        try {
            var parsed = TimeTravelSqlClauses.extract(sql);
            var compiler = securityPolicyApplier.getCompiler();
            QueryExecutionContext initialCtx;
            if (parsed.statement() instanceof IncrementalReadStatement changes) {
                var incremental = IncrementalSql.bind(parsed, changes, session);
                initialCtx = QueryExecutionContext.initial(sm, incremental.analysis(), incremental.session(), compiler);
            } else {
                initialCtx = QueryExecutionContext.initial(sm, parsed, session, compiler);
            }
            String primaryTable = patternRecorder != null && parsed.statement() instanceof Query query
                    ? extractFirstBaseTable(query) : "unknown";
            var coordinator = new QueryLifecycleCoordinator(
                    pipelinePhases,
                    () -> LOG.debug("event=query_start queryId={}", session.queryId()),
                    () -> {
                        LOG.debug("event=query_end queryId={}", session.queryId());
                        recordQueryCompletion(session.queryId(), primaryTable);
                    });
            return coordinator.execute(initialCtx).whenComplete((result, failure) -> {
                if (querySpan != null) querySpan.end();
            });
        } catch (RuntimeException | Error failure) {
            if (querySpan != null) querySpan.end();
            throw failure;
        }
    }

    private void recordQueryCompletion(String queryId, String tableName) {
        if (patternRecorder != null) {
            try {
                patternRecorder.record(new org.pangrova.planner.advisor.QueryPatternRecorder.QueryPattern(
                        tableName, java.util.Set.of(), java.util.Set.of(),
                        java.util.Set.of(), java.util.Set.of(),
                        false, java.time.Instant.now()));
            } catch (Exception e) {
                LOG.warn("event=pattern_record_failed queryId={} error={}", queryId, e.getMessage());
            }
        }
    }

    private static String extractFirstBaseTable(Query query) {
        var tables = query.from();
        if (tables == null || tables.isEmpty()) {
            return "unknown";
        }
        var tableRef = tables.get(0);
        if (tableRef.isDerived() || tableRef.isTableFunction()) {
            return "unknown";
        }
        return String.join(".", tableRef.name().parts());
    }

    // ── DML compilation and dispatch ──

    /** Compiles reusable read intent through the current authenticated query authority. */
    AuthorizedRelation compileAuthorizedRelation(String sql, QuerySession session) {
        requireAuthorizedSession(session);
        SqlCompiler compiler = securityPolicyApplier.getCompiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session),
                session.publicationRevision());
        var parsed = TimeTravelSqlClauses.extract(sql);
        var statement = parsed.statement();
        if (!(statement instanceof org.pangrova.parser.api.ast.Query)
                && !(statement instanceof org.pangrova.parser.api.ast.CteQuery)) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Reusable relational intent must be a read-only query");
        }
        var normalized = extractTimeTravelFromSql(parsed, session);
        if (!normalized.snapshotOverrides().isEmpty()) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Reusable relational intent cannot override its explicit source snapshot");
        }
        var result = compiler.compileForPolicyApplication(normalized.parsedSql());
        if (result.toPlannedQuery().dmlWriteTarget().isPresent()) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Reusable relational intent cannot contain a write operation");
        }
        var protectedResult = securityPolicyApplier.applyPoliciesObserved(result.toPlannedQuery(), session);
        PlannedQuery plan = compiler.optimizeProtectedQuery(protectedResult.plan());
        byte[] bytes = plan.serializeValidated(compiler);
        return new AuthorizedRelation(bytes, plan.splitSources(), plan.columnNames(),
                plan.queryOutputColumnTypeNames(), plan.effectivePlanDigest(),
                plan.isSplitIndependent(), CompiledScanSchemas.extract(bytes), protectedResult.policies(),
                plan.serializeAuthorizedStageGraph(compiler, 0));
    }

    private static void requireAuthorizedSession(QuerySession session) {
        if (session == null || session.userIdentity() == null
                || session.tenantId() == null || session.tenantId().isBlank()) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Protected compilation requires an authenticated tenant context");
        }
    }

    /** Compiles a DML override that cannot be represented by the ordinary AST route. */
    private CompletionStage<DispatchResult> startDmlSql(QueryStateMachine sm, String sql,
            QuerySession session, String writeModeOverride) {
        SqlCompiler compiler = securityPolicyApplier.getCompiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session),
                session.publicationRevision());
        var observed = securityPolicyApplier.applyPoliciesObserved(compiler.compileToPlannedQuery(sql), session);
        PlannedQuery plan = observed.plan();
        LOG.info("event=dml_plan_dump sql_preview={} plan=\n{}",
                sql.length() > SQL_PREVIEW_LIMIT ? sql.substring(0, SQL_PREVIEW_LIMIT) + "..." : sql,
                plan.planExplanation());
        DispatchQueryContext ctx = new DispatchQueryContext(Map.of(), sql, compiler);
        ctx = enrichWithDmlWriteContext(plan, ctx);
        Statement statement = compiler.parseToAst(sql);
        ctx = withWriteObservation(ctx, WritePolicyObservation.capture(observed, compiler, statement));
        if (writeModeOverride != null && ctx.dmlWriteContext().isPresent()) {
            ctx = ctx.withDmlWriteContext(new DmlWriteContext(
                    ctx.dmlWriteContext().get().targetTable(), writeModeOverride,
                    ctx.dmlWriteContext().get().partitionSpec(), ctx.dmlWriteContext().get().policyObservation()));
        }
        return schedulingEngine.startQuery(sm, session, plan, ctx, statement);
    }

    /**
     * Compiles a pre-parsed DML Statement and dispatches the resulting plan to
     * native workers, bypassing both StatementDispatchPhase and the SQL string
     * round-trip.
     */
    CompletionStage<DispatchResult> startDmlStatement(QueryStateMachine sm,
            org.pangrova.parser.api.ast.Statement stmt, QuerySession session) {
        SqlCompiler compiler = securityPolicyApplier.getCompiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session),
                session.publicationRevision());

        // A MERGE must be dispatched as one Native write and one catalog commit.
        if (stmt instanceof MergeStatement merge) {
            boolean hasUpdate = merge.clauses().stream().anyMatch(
                    c -> c instanceof MergeStatement.MergeClause.WhenMatchedUpdate);
            boolean hasDelete = merge.clauses().stream().anyMatch(
                    c -> c instanceof MergeStatement.MergeClause.WhenMatchedDelete);
            boolean hasInsert = merge.clauses().stream().anyMatch(
                    c -> c instanceof MergeStatement.MergeClause.WhenNotMatchedInsert);
            if (hasDelete && (hasUpdate || hasInsert)) {
                throw new PlannerException(
                        PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                        "MERGE with DELETE and another action cannot be represented "
                                + "as one atomic native write");
            }
            if (hasUpdate || hasDelete) {
                String sql = new org.pangrova.parser.format.AstPrinter().print(merge);
                return startDmlSql(sm, sql, session, "OVERWRITE");
            }
        }

        var observed = securityPolicyApplier.applyPoliciesObserved(
                compiler.compileStatementToPlannedQuery(stmt), session);
        PlannedQuery plan = observed.plan();
        // Keep the source dialect in the dispatch context for retries and diagnostics.
        String sqlForRetry = new org.pangrova.parser.format.AstPrinter().print(stmt);
        DispatchQueryContext ctx = new DispatchQueryContext(
                Map.of(), sqlForRetry, compiler);
        ctx = enrichWithDmlWriteContext(plan, ctx);
        ctx = withWriteObservation(ctx, WritePolicyObservation.capture(observed, compiler, stmt));
        ctx = adjustNativeDmlWriteMode(stmt, ctx);

        return schedulingEngine.startQuery(sm, session, plan, ctx, stmt);
    }

    private static DispatchQueryContext adjustNativeDmlWriteMode(
            org.pangrova.parser.api.ast.Statement stmt, DispatchQueryContext ctx) {
        if (ctx.dmlWriteContext().isEmpty()) {
            return ctx;
        }
        if (stmt instanceof MergeStatement merge) {
            return withMergeWriteMode(merge, ctx);
        }
        if (stmt instanceof DeleteStatement) {
            return withWriteMode(ctx, "OVERWRITE");
        }
        if (stmt instanceof org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement iss
                && iss.overwrite()) {
            return withWriteMode(ctx, "OVERWRITE");
        }
        if (stmt instanceof org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement overwrite) {
            return withWriteModeAndPartitions(
                    ctx, "OVERWRITE", staticPartitionSpec(overwrite));
        }
        return ctx;
    }

    private static DispatchQueryContext withMergeWriteMode(
            MergeStatement merge, DispatchQueryContext ctx) {
        boolean hasWhenMatched = merge.clauses().stream().anyMatch(
                c -> c instanceof MergeStatement.MergeClause.WhenMatchedUpdate
                  || c instanceof MergeStatement.MergeClause.WhenMatchedDelete);
        return withWriteMode(ctx, hasWhenMatched ? "OVERWRITE" : "INSERT");
    }

    private static DispatchQueryContext withWriteMode(
            DispatchQueryContext ctx, String writeMode) {
        return ctx.withDmlWriteContext(new DmlWriteContext(
                ctx.dmlWriteContext().get().targetTable(), writeMode,
                ctx.dmlWriteContext().get().partitionSpec(), ctx.dmlWriteContext().get().policyObservation()));
    }

    private static DispatchQueryContext withWriteModeAndPartitions(
            DispatchQueryContext ctx, String writeMode, List<String> partitionSpec) {
        return ctx.withDmlWriteContext(new DmlWriteContext(
                ctx.dmlWriteContext().get().targetTable(), writeMode, partitionSpec,
                ctx.dmlWriteContext().get().policyObservation()));
    }

    private static DispatchQueryContext withWriteObservation(
            DispatchQueryContext ctx, WritePolicyObservation observation) {
        var write = ctx.dmlWriteContext().orElseThrow(() ->
                new IllegalStateException("Compiled mutation lacks a write target"));
        return ctx.withDmlWriteContext(new DmlWriteContext(write.targetTable(), write.writeMode(),
                write.partitionSpec(), Optional.of(observation)));
    }

    private static List<String> staticPartitionSpec(
            org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement statement) {
        return statement.partitionSpec().stream()
                .flatMap(spec -> spec.entrySet().stream())
                .sorted(java.util.Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + partitionValue(entry.getValue()))
                .toList();
    }

    private static String partitionValue(org.pangrova.parser.api.ast.Literal literal) {
        org.pangrova.parser.api.ast.LiteralValue value = literal.value();
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.IntegerValue integer) {
            return Long.toString(integer.value());
        }
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.DecimalValue decimal) {
            return decimal.value().toPlainString();
        }
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.StringValue string) {
            return string.value();
        }
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.BooleanValue bool) {
            return Boolean.toString(bool.value());
        }
        return temporalPartitionValue(value);
    }

    private static String temporalPartitionValue(org.pangrova.parser.api.ast.LiteralValue value) {
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.DateValue date) {
            return date.value();
        }
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.TimestampValue timestamp) {
            return timestamp.value();
        }
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.TimestampWithTimeZoneValue timestamp) {
            return timestamp.value();
        }
        if (value instanceof org.pangrova.parser.api.ast.LiteralValue.NullValue) {
            throw new IllegalArgumentException(
                    "Static overwrite partition value must not be NULL");
        }
        throw new IllegalArgumentException(
                "Unsupported static overwrite partition literal: " + value.getClass().getSimpleName());
    }

    // ── Internal helpers ──

    SqlCompiler getCompiler() {
        return securityPolicyApplier.getCompiler();
    }

    private CompletionStage<DispatchResult> executeDistributedDispatch(QueryExecutionContext ctx) {
        return schedulingEngine.startQuery(
                ctx.stateMachine(), ctx.session(), ctx.compiledPlan(),
                new DispatchQueryContext(ctx.snapshotOverrides(), ctx.sql(), ctx.compiler(), ctx.preOptColumnNames()),
                ctx.parsedSql() == null ? null : ctx.parsedSql().statement());
    }

    private CompilationPhase.TimeTravelResult extractTimeTravelFromSql(
            Analysis parsed, QuerySession context) {
        var expanded = viewExpander.expand(parsed, context);
        return TimeTravelExtractor.extract(expanded, context,
                (catalog, schema, table, refType, refName) -> resolveRefSnapshot(
                        context, catalog, schema, table, refType, refName));
    }

    private Optional<TimeTravelExtractor.ResolvedReference> resolveRefSnapshot(
            QuerySession context,
            String catalog, String schema, String table,
            String refType, String refName) {
        if (catalogRegistry == null) {
            return Optional.empty();
        }
        String selectedCatalog = canonicalName(catalog, catalogRegistry.listCatalogNames());
        if (selectedCatalog == null) {
            return Optional.empty();
        }
        securityPolicyApplier.requireBranchRead(selectedCatalog, schema, table, context);
        catalogRegistry.requireVisibleDescriptor(selectedCatalog, context.tenantId());
        var session = org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                context, selectedCatalog, schema);
        var metadata = catalogRegistry.metadataForQuery(selectedCatalog, session);
        boolean exactLookup = metadata.supportsExactTableNameLookup()
                && schema.indexOf('.') < 0 && table.indexOf('$') < 0;
        if (exactLookup) {
            var discovery = metadata.discoverRead(session, new SchemaTableName(schema, table));
            if (discovery != null) {
                return selectReference(new QualifiedTableName(selectedCatalog, schema, table),
                        discovery, refType, refName);
            }
        }
        String selectedSchema = canonicalName(schema, metadata.listSchemaNames(session));
        if (selectedSchema == null) return Optional.empty();
        session = org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                context, selectedCatalog, selectedSchema);
        String selectedTable = canonicalName(table, metadata.listTables(session, java.util.Optional.of(selectedSchema))
                .stream().map(SchemaTableName::table).toList());
        if (selectedTable == null) return Optional.empty();
        var tableName = new SchemaTableName(selectedSchema, selectedTable);
        var qualified = new QualifiedTableName(selectedCatalog, selectedSchema, selectedTable);
        if (exactLookup) {
            var discovery = metadata.discoverRead(session, tableName);
            return discovery == null ? Optional.empty() : selectReference(qualified, discovery, refType, refName);
        }
        var snapshot = metadata.resolveRefSnapshotId(session, tableName, refType, refName);
        return snapshot.isEmpty() ? Optional.empty() : Optional.of(
                new TimeTravelExtractor.ResolvedReference(snapshot.getAsLong(), qualified, Optional.empty()));
    }

    private static Optional<TimeTravelExtractor.ResolvedReference> selectReference(
            QualifiedTableName table, ConnectorReadDiscovery discovery, String refType, String refName) {
        var snapshot = discovery.referenceSnapshot().apply(refType, refName);
        return snapshot.isEmpty() ? Optional.empty() : Optional.of(
                new TimeTravelExtractor.ResolvedReference(snapshot.getAsLong(), table, Optional.of(discovery)));
    }

    private static String canonicalName(String requested, List<String> names) {
        if (names.contains(requested)) return requested;
        var matches = names.stream().filter(name -> name.equalsIgnoreCase(requested)).toList();
        if (matches.size() > 1) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Ambiguous case-insensitive source name: " + requested);
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    /**
     * Extracts DML operation metadata from a LogicalTableModify RelNode and
     * attaches it to the dispatch context.
     */
    static DispatchQueryContext enrichWithDmlWriteContext(PlannedQuery plan,
                                                          DispatchQueryContext ctx) {
        return plan.dmlWriteTarget()
                .map(target -> {
                    var parts = target.qualifiedTableName();
                    if (parts.size() != 3) {
                        throw new IllegalArgumentException("Distributed write target requires three name components");
                    }
                    var tableName = new org.pangrova.connector.api.metadata.table.QualifiedTableName(
                            parts.get(0), parts.get(1), parts.get(2));
                    return ctx.withDmlWriteContext(new DmlWriteContext(
                            tableName, target.operation().name(), List.of(), Optional.empty()));
                })
                .orElse(ctx);
    }

    private org.pangrova.planner.analysis.MetadataTableQuery resolveMetadataListing(java.util.List<String> parts,
            org.pangrova.connector.api.enums.MetadataTableKind kind, QuerySession context) {
        if (catalogRegistry == null) {
            throw new IllegalStateException("SHOW metadata requires the query catalog registry");
        }
        var table = SessionTableNames.resolveTableRef(parts, context);
        var session = SessionContextConverter.toConnectorSession(
                context, table.catalog(), table.schema());
        var metadata = catalogRegistry.getCatalog(table.catalog()).getMetadata(session);
        var listing = metadata.getMetadataListing(session, kind).orElseThrow(() ->
                new org.pangrova.common.exception.PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Catalog '" + table.catalog() + "' has no Native metadata listing for " + kind));
        return new org.pangrova.planner.analysis.MetadataTableQuery(
                List.of(table.catalog(), table.schema(), table.table()), listing);
    }

    private List<QueryExecutionPhase> buildPipeline() {
        var phases = new ArrayList<QueryExecutionPhase>();
        phases.add(new StatementDispatchPhase(statementDispatcher, this::resolveMetadataListing));
        phases.add(new CompilationPhase(
                (plan, sess) -> {
                    var observed = securityPolicyApplier.applyPoliciesObserved(plan, sess);
                    return new CompilationPhase.ProtectedPlan(observed.plan(),
                            !observed.policies().isEmpty());
                },
                this::extractTimeTravelFromSql,
                lineageCapture,
                ignored -> securityPolicyApplier.getCompiler()));
        phases.add(new DistributedDispatchPhase(this::executeDistributedDispatch));
        return List.copyOf(phases);
    }
}
