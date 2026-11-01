package org.pangrova.coordinator.query.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.coordinator.query.execution.phase.DistributedDispatchPhase;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionContext;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;
import org.pangrova.coordinator.query.execution.phase.QueryLifecycleCoordinator;
import org.pangrova.coordinator.query.execution.phase.StatementDispatchPhase;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.metadata.core.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.observability.tracing.QueryTracer;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.PlannerException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.observability.Span;
import org.pangrova.planner.PlannedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the core query execution pipeline: compiling SQL, dispatching DML statements,
 * and coordinating the phase-based execution flow.
 *
 * <p>Thread safety: stateless beyond references to thread-safe collaborators.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
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

    // ── Pipeline execution ──

    void setPatternRecorder(org.pangrova.planner.advisor.QueryPatternRecorder recorder) {
        this.patternRecorder = recorder;
    }

    /**
     * Executes the full query pipeline: statement dispatch → compilation →
     * cache lookup → distributed dispatch.
     */
    DispatchResult doExecute(QueryStateMachine sm, String sql,
                             CoordinatorSessionContext session) {
        if (!sm.tryTransition(QueryStateMachine.State.PLANNING)) {
            return null;
        }
        Span querySpan = queryTracer != null
                ? queryTracer.startQuerySpan(session.queryId(), sql) : null;
        try {
            var initialCtx = QueryExecutionContext.initial(
                    sm, sql, session, securityPolicyApplier.getCompiler());
            var coordinator = new QueryLifecycleCoordinator(
                    pipelinePhases,
                    () -> LOG.debug("event=query_start queryId={}", session.queryId()),
                    () -> {
                        LOG.debug("event=query_end queryId={}", session.queryId());
                        recordQueryCompletion(session.queryId(), sql, session);
                    });
            return coordinator.execute(initialCtx);
        } finally {
            if (querySpan != null) {
                querySpan.end();
            }
        }
    }

    private void recordQueryCompletion(String queryId, String sql,
                                        CoordinatorSessionContext session) {
        if (patternRecorder != null) {
            try {
                String tableName = extractPrimaryTableFromSql(sql);
                patternRecorder.record(new org.pangrova.planner.advisor.QueryPatternRecorder.QueryPattern(
                        tableName, java.util.Set.of(), java.util.Set.of(),
                        java.util.Set.of(), java.util.Set.of(),
                        false, java.time.Instant.now()));
            } catch (Exception e) {
                LOG.warn("event=pattern_record_failed queryId={} error={}", queryId, e.getMessage());
            }
        }
    }

    /**
     * Extracts a best-effort primary table name from a SQL string.
     * Uses the parser AST to extract the first table reference.
     */
    private String extractPrimaryTableFromSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return "unknown";
        }
        SqlCompiler currentCompiler = securityPolicyApplier.getCompiler();
        return extractTableFromAst(sql, currentCompiler);
    }

    private static String extractTableFromAst(String sql, SqlCompiler compiler) {
        if (compiler == null) {
            return "unknown";
        }
        try {
            var stmt = compiler.parseToAst(sql);
            if (!(stmt instanceof org.pangrova.parser.ast.Query query)) {
                return "unknown";
            }
            return extractFirstBaseTable(query);
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String extractFirstBaseTable(org.pangrova.parser.ast.Query query) {
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
    AuthorizedRelation compileAuthorizedRelation(String sql, CoordinatorSessionContext session) {
        requireAuthorizedSession(session);
        SqlCompiler compiler = securityPolicyApplier.getCompiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session));
        var statement = compiler.parseToAst(sql);
        if (!(statement instanceof org.pangrova.parser.ast.Query)
                && !(statement instanceof org.pangrova.parser.ast.CteQuery)) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Reusable relational intent must be a read-only query");
        }
        var normalized = extractTimeTravelFromSql(sql, compiler, session);
        if (!normalized.snapshotOverrides().isEmpty()) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Reusable relational intent cannot override its explicit source snapshot");
        }
        var result = compiler.compileForPolicyApplication(normalized.strippedSql());
        if (result.toPlannedQuery().dmlWriteTarget().isPresent()) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Reusable relational intent cannot contain a write operation");
        }
        var protectedResult = securityPolicyApplier.applyPoliciesObserved(result.toPlannedQuery(), session);
        PlannedQuery plan = compiler.optimizeProtectedQuery(protectedResult.plan());
        byte[] bytes = plan.serializeValidated(compiler);
        return new AuthorizedRelation(bytes, plan.splitSources(), plan.columnNames(),
                plan.queryOutputColumnTypeNames(), plan.effectivePlanDigest(),
                plan.isSplitIndependent(), AuthorizedRelationSchemas.extract(bytes), protectedResult.policies(),
                plan.serializeAuthorizedStageGraph(compiler, 0));
    }

    private static void requireAuthorizedSession(CoordinatorSessionContext session) {
        if (session == null || session.userIdentity() == null
                || session.tenantId() == null || session.tenantId().isBlank()) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "Protected compilation requires an authenticated tenant context");
        }
    }

    /**
     * Compiles DML SQL and dispatches the resulting plan to native workers,
     * bypassing StatementDispatchPhase.
     */
    DispatchResult executeDmlSql(QueryStateMachine sm, String sql,
                                 CoordinatorSessionContext session) {
        return executeDmlSql(sm, sql, session, null);
    }

    /**
     * Compiles DML SQL with an optional write mode override.
     * When writeModeOverride is non-null, it replaces the Calcite-derived write mode.
     */
    DispatchResult executeDmlSql(QueryStateMachine sm, String sql,
                                 CoordinatorSessionContext session,
                                 String writeModeOverride) {
        SqlCompiler compiler = securityPolicyApplier.getCompiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session));
        var observed = securityPolicyApplier.applyPoliciesObserved(compiler.compileToPlannedQuery(sql), session);
        PlannedQuery plan = observed.plan();
        LOG.info("event=dml_plan_dump sql_preview={} plan=\n{}",
                sql.length() > SQL_PREVIEW_LIMIT ? sql.substring(0, SQL_PREVIEW_LIMIT) + "..." : sql,
                plan.planExplanation());
        DispatchQueryContext ctx = new DispatchQueryContext(Map.of(), sql, compiler);
        ctx = enrichWithDmlWriteContext(plan, ctx);
        ctx = withWriteObservation(ctx, WritePolicyObservation.capture(observed, compiler, compiler.parseToAst(sql)));
        if (writeModeOverride != null && ctx.dmlWriteContext().isPresent()) {
            ctx = ctx.withDmlWriteContext(new DmlWriteContext(
                    ctx.dmlWriteContext().get().targetTable(), writeModeOverride,
                    ctx.dmlWriteContext().get().partitionSpec(), ctx.dmlWriteContext().get().policyObservation()));
        }
        return schedulingEngine.dispatchQuery(sm, session, plan, ctx);
    }

    /**
     * Compiles a pre-parsed DML Statement and dispatches the resulting plan to
     * native workers, bypassing both StatementDispatchPhase and the SQL string
     * round-trip.
     */
    DispatchResult executeDmlStatement(QueryStateMachine sm,
                                       org.pangrova.parser.ast.Statement stmt,
                                       CoordinatorSessionContext session) {
        SqlCompiler compiler = securityPolicyApplier.getCompiler().forSession(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(session));

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
                String sql = new org.pangrova.parser.AstPrinter().print(merge);
                return executeDmlSql(sm, sql, session, "OVERWRITE");
            }
        }

        var observed = securityPolicyApplier.applyPoliciesObserved(
                compiler.compileStatementToPlannedQuery(stmt), session);
        PlannedQuery plan = observed.plan();
        // The dispatch context is reparsed for enrichment and retry. Calcite's
        // SqlNode unparser emits VALUES ROW(...), which the Pangrova SQL grammar
        // does not accept; preserve the source dialect when serializing the AST.
        String sqlForRetry = new org.pangrova.parser.AstPrinter().print(stmt);
        DispatchQueryContext ctx = new DispatchQueryContext(
                Map.of(), sqlForRetry, compiler);
        ctx = enrichWithDmlWriteContext(plan, ctx);
        ctx = withWriteObservation(ctx, WritePolicyObservation.capture(observed, compiler, stmt));
        ctx = adjustNativeDmlWriteMode(stmt, ctx);

        return schedulingEngine.dispatchQuery(sm, session, plan, ctx);
    }

    private static DispatchQueryContext adjustNativeDmlWriteMode(
            org.pangrova.parser.ast.Statement stmt, DispatchQueryContext ctx) {
        if (ctx.dmlWriteContext().isEmpty()) {
            return ctx;
        }
        if (stmt instanceof MergeStatement merge) {
            return withMergeWriteMode(merge, ctx);
        }
        if (stmt instanceof DeleteStatement) {
            return withWriteMode(ctx, "OVERWRITE");
        }
        if (stmt instanceof org.pangrova.parser.ast.statement.dml.InsertSelectStatement iss
                && iss.overwrite()) {
            return withWriteMode(ctx, "OVERWRITE");
        }
        if (stmt instanceof org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement overwrite) {
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
            org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement statement) {
        return statement.partitionSpec().stream()
                .flatMap(spec -> spec.entrySet().stream())
                .sorted(java.util.Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + partitionValue(entry.getValue()))
                .toList();
    }

    private static String partitionValue(org.pangrova.parser.ast.Literal literal) {
        org.pangrova.parser.ast.LiteralValue value = literal.value();
        if (value instanceof org.pangrova.parser.ast.LiteralValue.IntegerValue integer) {
            return Long.toString(integer.value());
        }
        if (value instanceof org.pangrova.parser.ast.LiteralValue.DecimalValue decimal) {
            return decimal.value().toPlainString();
        }
        if (value instanceof org.pangrova.parser.ast.LiteralValue.StringValue string) {
            return string.value();
        }
        if (value instanceof org.pangrova.parser.ast.LiteralValue.BooleanValue bool) {
            return Boolean.toString(bool.value());
        }
        return temporalPartitionValue(value);
    }

    private static String temporalPartitionValue(org.pangrova.parser.ast.LiteralValue value) {
        if (value instanceof org.pangrova.parser.ast.LiteralValue.DateValue date) {
            return date.value();
        }
        if (value instanceof org.pangrova.parser.ast.LiteralValue.TimestampValue timestamp) {
            return timestamp.value();
        }
        if (value instanceof org.pangrova.parser.ast.LiteralValue.TimestampWithTimeZoneValue timestamp) {
            return timestamp.value();
        }
        if (value instanceof org.pangrova.parser.ast.LiteralValue.NullValue) {
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

    private DispatchResult executeDistributedDispatch(QueryExecutionContext ctx) {
        PlannedQuery plan = (PlannedQuery) ctx.compiledPlan();
        return schedulingEngine.dispatchQuery(
                ctx.stateMachine(), ctx.session(), plan,
                new DispatchQueryContext(ctx.snapshotOverrides(), ctx.sql(),
                        ctx.compiler(), ctx.preOptColumnNames(),
                        Optional.empty(), Optional.empty(), Optional.empty(),
                        ctx.runtimeFilterDescriptors()));
    }

    private CompilationPhase.TimeTravelResult extractTimeTravelFromSql(
            String sql, SqlCompiler compiler, CoordinatorSessionContext context) {
        String expanded = viewExpander.expand(sql, compiler, context);
        return TimeTravelExtractor.extract(expanded, context,
                (catalog, schema, table, refType, refName) -> resolveRefSnapshot(
                        context, catalog, schema, table, refType, refName));
    }

    private java.util.OptionalLong resolveRefSnapshot(
            CoordinatorSessionContext context,
            String catalog, String schema, String table,
            String refType, String refName) {
        if (catalogRegistry == null) {
            return java.util.OptionalLong.empty();
        }
        String selectedCatalog = canonicalName(catalog, catalogRegistry.listCatalogNames());
        if (selectedCatalog == null) {
            return java.util.OptionalLong.empty();
        }
        CatalogHandle handle = catalogRegistry.getCatalog(selectedCatalog);
        var session = org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                context, selectedCatalog, schema);
        var metadata = handle.getMetadata(session);
        String selectedSchema = canonicalName(schema, metadata.listSchemaNames(session));
        if (selectedSchema == null) return java.util.OptionalLong.empty();
        session = org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                context, selectedCatalog, selectedSchema);
        String selectedTable = canonicalName(table, metadata.listTables(session, java.util.Optional.of(selectedSchema))
                .stream().map(SchemaTableName::table).toList());
        if (selectedTable == null) return java.util.OptionalLong.empty();
        return metadata.resolveRefSnapshotId(session,
                new SchemaTableName(selectedSchema, selectedTable), refType, refName);
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
                    var tableName = new org.pangrova.spi.connector.metadata.table.QualifiedTableName(
                            parts.get(0), parts.get(1), parts.get(2));
                    return ctx.withDmlWriteContext(new DmlWriteContext(
                            tableName, target.operation().name(), List.of(), Optional.empty()));
                })
                .orElse(ctx);
    }

    private org.pangrova.planner.MetadataTableQuery resolveMetadataListing(java.util.List<String> parts,
            org.pangrova.spi.connector.metadata.MetadataTableKind kind, CoordinatorSessionContext context) {
        if (catalogRegistry == null) {
            throw new IllegalStateException("SHOW metadata requires the query catalog registry");
        }
        var table = org.pangrova.coordinator.query.statement.TableResolver.resolveTableRef(parts, context);
        var session = org.pangrova.coordinator.query.statement.TableResolver.buildSession(
                context, table.catalog(), table.schema());
        var metadata = catalogRegistry.getCatalog(table.catalog()).getMetadata(session);
        var listing = metadata.getMetadataListing(session, kind).orElseThrow(() ->
                new org.pangrova.common.exception.PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Catalog '" + table.catalog() + "' has no Native metadata listing for " + kind));
        return new org.pangrova.planner.MetadataTableQuery(
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
