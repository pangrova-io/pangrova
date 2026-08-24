package org.pangrova.coordinator.query.execution;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.pangrova.coordinator.ai.cache.SemanticCache;
import org.pangrova.coordinator.ai.cache.SemanticCacheEntry;

import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.phase.CacheLookupPhase;
import org.pangrova.coordinator.query.execution.phase.CompilationPhase;
import org.pangrova.coordinator.query.execution.phase.DistributedDispatchPhase;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionContext;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;
import org.pangrova.coordinator.query.execution.phase.QueryLifecycleCoordinator;
import org.pangrova.coordinator.query.execution.phase.StatementDispatchPhase;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.coordinator.query.statement.StatementConnectorSession;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.observability.tracing.QueryTracer;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.planner.SqlCompiler;
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

    private static final Logger LOG = LoggerFactory.getLogger(QueryPipelineExecutor.class);

    private final SecurityPolicyApplier securityPolicyApplier;
    private final QueryCacheService cacheService;
    private final StatementDispatcher statementDispatcher;
    private final QueryTracer queryTracer;
    private final QueryLineageCapture lineageCapture;
    private final List<QueryExecutionPhase> pipelinePhases;
    private final QuerySchedulingEngine schedulingEngine;
    private final MergeDecomposer mergeDecomposer;
    private final CatalogRegistry catalogRegistry;
    private final ViewExpander viewExpander;
    private volatile SemanticCache semanticCache;
    private volatile org.pangrova.planner.advisor.QueryPatternRecorder patternRecorder;
    private volatile org.pangrova.spi.ai.embedding.EmbeddingProvider embeddingProvider;

    QueryPipelineExecutor(SecurityPolicyApplier securityPolicyApplier,
                          QueryCacheService cacheService,
                          StatementDispatcher statementDispatcher,
                          QueryTracer queryTracer,
                          QueryLineageCapture lineageCapture,
                          QuerySchedulingEngine schedulingEngine,
                          MergeDecomposer mergeDecomposer,
                          CatalogRegistry catalogRegistry) {
        this.securityPolicyApplier = securityPolicyApplier;
        this.cacheService = cacheService;
        this.statementDispatcher = statementDispatcher;
        this.queryTracer = queryTracer;
        this.lineageCapture = lineageCapture;
        this.schedulingEngine = schedulingEngine;
        this.mergeDecomposer = mergeDecomposer;
        this.catalogRegistry = catalogRegistry;
        this.viewExpander = new ViewExpander(catalogRegistry);
        this.pipelinePhases = buildPipeline();
    }

    // ── Pipeline execution ──

    void setSemanticCache(SemanticCache cache) {
        this.semanticCache = cache;
    }

    void setPatternRecorder(org.pangrova.planner.advisor.QueryPatternRecorder recorder) {
        this.patternRecorder = recorder;
    }

    void setEmbeddingProvider(org.pangrova.spi.ai.embedding.EmbeddingProvider provider) {
        this.embeddingProvider = provider;
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
                        cacheCompletedQuery(sql, session);
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
     * Caches a completed query in the semantic cache. The cache entry requires
     * an embedding vector generated from an EmbeddingProvider. When no provider
     * is available, caching is skipped entirely to avoid polluting the cache with
     * zero-vector entries that can never produce a meaningful similarity match.
     */
    private void cacheCompletedQuery(String sql, CoordinatorSessionContext session) {
        var cache = this.semanticCache;
        if (cache == null || sql == null || sql.isBlank()) {
            return;
        }
        var provider = this.embeddingProvider;
        if (provider == null) {
            // No embedding provider wired — skip caching to avoid zero-vector entries
            // that would result in permanent zero similarity and wasted memory.
            LOG.debug("event=semantic_cache_skip_no_provider queryId={}", session.queryId());
            return;
        }
        try {
            float[] embedding = provider.embed(sql);
            var entry = new SemanticCacheEntry(
                    sql,
                    embedding,
                    session.tenantId(),
                    session.userIdentity() != null ? session.userIdentity().principal() : null,
                    Set.of(),
                    new byte[0],
                    Instant.now());
            cache.put(entry);
        } catch (org.pangrova.coordinator.ai.embedding.EmbeddingModelNotReadyException notReady) {
            // Embedding feature enabled but no unambiguous default model registered yet.
            // Expected/recoverable — skip caching this query without warn-level noise.
            LOG.debug("event=semantic_cache_skip_no_model queryId={} reason={}",
                    session.queryId(), notReady.getMessage());
        } catch (Exception e) {
            LOG.warn("event=semantic_cache_put_failed queryId={} error={}",
                    session.queryId(), e.getMessage());
        }
    }

    /**
     * Extracts a best-effort primary table name from a SQL string.
     * Uses the parser AST to extract the first table reference.
     * Falls back to simple string parsing if AST extraction fails.
     */
    private String extractPrimaryTableFromSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return "unknown";
        }
        SqlCompiler currentCompiler = securityPolicyApplier.getCompiler();
        String astTable = extractTableFromAst(sql, currentCompiler);
        if (!"unknown".equals(astTable)) {
            return astTable;
        }
        return extractTableFromString(sql);
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

    private static String extractTableFromString(String sql) {
        String upper = sql.toUpperCase().trim();
        int fromIdx = upper.indexOf(" FROM ");
        if (fromIdx < 0) {
            fromIdx = upper.indexOf("\nFROM ");
        }
        if (fromIdx < 0) {
            fromIdx = upper.indexOf("\tFROM ");
        }
        if (fromIdx < 0) {
            return "unknown";
        }
        String afterFrom = sql.substring(fromIdx + 6).trim();
        int end = afterFrom.length();
        for (int i = 0; i < afterFrom.length(); i++) {
            char c = afterFrom.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == ',' || c == ';'
                    || c == ')' || c == '(' ) {
                end = i;
                break;
            }
        }
        String tableName = afterFrom.substring(0, end).trim();
        return tableName.isEmpty() ? "unknown" : tableName;
    }

    // ── DML compilation and dispatch ──

    /**
     * Dispatches a pre-compiled plan to native workers.
     * Used by InsertSelectDispatchPipeline to send the SELECT sub-query plan
     * directly to workers without re-entering the planner pipeline.
     */
    DispatchResult dispatchPlan(QueryStateMachine sm,
                                CoordinatorSessionContext session,
                                PlannedQuery plan) {
        DispatchQueryContext ctx = new DispatchQueryContext(
                Map.of(), "", securityPolicyApplier.getCompiler());
        ctx = enrichWithDmlWriteContext(plan, ctx);
        return schedulingEngine.dispatchQuery(sm, session, plan, ctx);
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
        SqlCompiler compiler = securityPolicyApplier.getCompiler();
        PlannedQuery plan = compiler.compileToPlannedQuery(sql);
        LOG.info("event=dml_plan_dump sql_preview={} plan=\n{}",
                sql.length() > 80 ? sql.substring(0, 80) + "..." : sql,
                plan.planExplanation());
        DispatchQueryContext ctx = new DispatchQueryContext(Map.of(), sql, compiler);
        ctx = enrichWithDmlWriteContext(plan, ctx);
        if (writeModeOverride != null && ctx.dmlWriteContext().isPresent()) {
            ctx = ctx.withDmlWriteContext(new DmlWriteContext(
                    ctx.dmlWriteContext().get().targetTable(), writeModeOverride));
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
        SqlCompiler compiler = securityPolicyApplier.getCompiler();

        // Complete MERGE with both UPDATE and DELETE clauses: decompose into
        // sequential operations via MergeDecomposer.
        if (stmt instanceof MergeStatement merge) {
            boolean hasUpdate = merge.clauses().stream().anyMatch(
                    c -> c instanceof MergeStatement.MergeClause.WhenMatchedUpdate);
            boolean hasDelete = merge.clauses().stream().anyMatch(
                    c -> c instanceof MergeStatement.MergeClause.WhenMatchedDelete);
            boolean hasInsert = merge.clauses().stream().anyMatch(
                    c -> c instanceof MergeStatement.MergeClause.WhenNotMatchedInsert);
            if (hasUpdate && hasDelete) {
                return mergeDecomposer.executeCompleteMerge(
                        sm, merge, session, compiler);
            }
            if (hasUpdate && hasInsert) {
                return mergeDecomposer.executeMergeUpdateInsert(
                        sm, merge, session, compiler);
            }
            // MERGE with only UPDATE (no INSERT, no DELETE): route through SQL
            // compilation path. The AST-to-SqlNode conversion produces incorrect
            // column mappings in convertMerge(); the SQL path works correctly.
            // Use AstPrinter (not compiler.statementToSql) to avoid the buggy
            // AST→SqlNode→SQL round-trip.
            if (hasUpdate) {
                String sql = new org.pangrova.parser.AstPrinter().print(merge);
                return executeDmlSql(sm, sql, session, "OVERWRITE");
            }
        }

        PlannedQuery plan = compiler.compileStatementToPlannedQuery(stmt);
        String sqlForRetry = compiler.statementToSql(stmt);
        DispatchQueryContext ctx = new DispatchQueryContext(
                Map.of(), sqlForRetry, compiler);
        ctx = enrichWithDmlWriteContext(plan, ctx);
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
                ctx.dmlWriteContext().get().targetTable(), writeMode));
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
            String sql, SqlCompiler compiler) {
        String expanded = viewExpander.expand(sql, compiler);
        return TimeTravelExtractor.extract(expanded, compiler, this::resolveRefSnapshot);
    }

    private java.util.OptionalLong resolveRefSnapshot(
            String catalog, String schema, String table,
            String refType, String refName) {
        if (catalogRegistry == null) {
            return java.util.OptionalLong.empty();
        }
        CatalogHandle handle = catalogRegistry.getCatalog(catalog);
        if (handle == null) {
            return java.util.OptionalLong.empty();
        }
        var session = new StatementConnectorSession(catalog, schema);
        var stn = new SchemaTableName(schema, table);
        return handle.getMetadata(session).resolveRefSnapshotId(
                session, stn, refType, refName);
    }

    /**
     * Extracts DML operation metadata from a LogicalTableModify RelNode and
     * attaches it to the dispatch context.
     */
    static DispatchQueryContext enrichWithDmlWriteContext(PlannedQuery plan,
                                                          DispatchQueryContext ctx) {
        return plan.dmlWriteTarget()
                .map(target -> {
                    String tableName = String.join(".", target.qualifiedTableName());
                    return ctx.withDmlWriteContext(new DmlWriteContext(tableName, target.operation().name()));
                })
                .orElse(ctx);
    }

    private List<QueryExecutionPhase> buildPipeline() {
        var phases = new ArrayList<QueryExecutionPhase>();
        phases.add(new StatementDispatchPhase(statementDispatcher));
        phases.add(new CompilationPhase(
                (plan, sess) -> securityPolicyApplier.applyPolicies(plan, sess),
                this::extractTimeTravelFromSql,
                lineageCapture,
                ignored -> securityPolicyApplier.getCompiler()));
        phases.add(new CacheLookupPhase(cacheService));
        phases.add(new DistributedDispatchPhase(
                this::executeDistributedDispatch, cacheService));
        return List.copyOf(phases);
    }
}
