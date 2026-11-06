package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import org.pangrova.planner.api.error.PlannerException;
import org.pangrova.planner.api.invocation.AiInvocationPlanHint;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.core.substrait.ConversionResult;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.planner.core.validation.NativeCapabilityRegistry;
import org.pangrova.planner.core.validation.SubstraitPlanValidator;
import org.pangrova.planner.core.validation.PlanValidationResult;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.pangrova.connector.api.session.ConnectorSession;

/**
 * End-to-end SQL compilation: SQL text → Substrait Plan bytes.
 * Core compile chain: planner.plan() → converter.convert() → validator.validate() → serializer.serialize().
 * Compilation failures are reported before any task is dispatched.
 *
 * <p>Security concerns (access control, column masking, row-level security) are handled
 * by dedicated components in {@code org.pangrova.coordinator.query.security}.
 */
public class SqlCompiler {

    private final PangrovaPlanner planner;
    private final SubstraitConverter converter;
    private final SubstraitPlanSerializer serializer;
    private final SubstraitPlanValidator validator;
    private final Function<ConnectorSession, PangrovaPlanner> sessionPlanner;
    private final org.pangrova.planner.core.cache.QueryResultCache queryResultCache;
    private final CompilerSessionScope sessionScope;

    public SqlCompiler() {
        this(new PangrovaPlanner(), new SubstraitConverter(),
                new SubstraitPlanSerializer(),
                new SubstraitPlanValidator(NativeCapabilityRegistry.defaults()));
    }

    public SqlCompiler(PangrovaPlanner planner, SubstraitConverter converter,
                       SubstraitPlanSerializer serializer) {
        this(planner, converter, serializer,
                new SubstraitPlanValidator(NativeCapabilityRegistry.defaults()));
    }

    /**
     * Creates a compiler with an explicit plan validator.
     *
     * @param planner planner producing optimized relational plans
     * @param converter converter translating plans to Substrait
     * @param serializer serializer producing the wire-format Substrait plan
     * @param validator validator checking Substrait plan compatibility
     */
    public SqlCompiler(PangrovaPlanner planner, SubstraitConverter converter,
                       SubstraitPlanSerializer serializer,
                       SubstraitPlanValidator validator) {
        this(planner, converter, serializer, validator, null, planner.getQueryResultCache(), null);
    }

    private SqlCompiler(PangrovaPlanner planner, SubstraitConverter converter,
                        SubstraitPlanSerializer serializer, SubstraitPlanValidator validator,
                        Function<ConnectorSession, PangrovaPlanner> sessionPlanner,
                        org.pangrova.planner.core.cache.QueryResultCache queryResultCache,
                        CompilerSessionScope sessionScope) {
        this.planner = planner;
        this.converter = converter;
        this.serializer = serializer;
        this.validator = validator;
        this.sessionPlanner = sessionPlanner;
        this.queryResultCache = queryResultCache;
        this.sessionScope = sessionScope;
    }

    /** Uses active Worker inventories for coarse validation; exact task binding is still required. */
    public SqlCompiler withNativeCapabilities(java.util.function.Supplier<NativeCapabilityRegistry> capabilities) {
        return new SqlCompiler(planner, converter, serializer, new SubstraitPlanValidator(capabilities),
                sessionPlanner, queryResultCache, sessionScope);
    }

    /** Installs the metadata boundary without depending on its implementation. */
    public SqlCompiler withSessionPlanner(Function<ConnectorSession, PangrovaPlanner> factory) {
        return new SqlCompiler(planner, converter, serializer, validator,
                Objects.requireNonNull(factory, "sessionPlanner"), queryResultCache, null);
    }

    /** Process-level invalidation service, independent of query metadata binding. */
    public SqlCompiler withQueryResultCache(org.pangrova.planner.core.cache.QueryResultCache cache) {
        return new SqlCompiler(planner, converter, serializer, validator, sessionPlanner, cache, sessionScope);
    }

    public org.pangrova.planner.core.cache.QueryResultCache queryResultCache() {
        return queryResultCache;
    }

    /** Captures a fresh query-local schema/planner and retains it through serialization retries. */
    public SqlCompiler forSession(ConnectorSession session) {
        if (sessionPlanner == null) {
            if (sessionScope != null && !sessionScope.equals(CompilerSessionScope.of(session))) {
                throw new IllegalArgumentException("A query compiler cannot be rebound to another session");
            }
            return this;
        }
        CompilerSessionScope scope = CompilerSessionScope.of(session);
        return new SqlCompiler(Objects.requireNonNull(sessionPlanner.apply(scope), "query planner"),
                converter, serializer, validator, null, queryResultCache, scope);
    }

    /** AQE uses the same query planner that produced and serialized its fragments. */
    public SubPlanReoptimizer subPlanReoptimizer() {
        PangrovaPlanner queryPlanner = planningPlanner();
        return new SubPlanReoptimizer() {
            @Override public PlanFragment reoptimize(PlanFragment fragment) {
                return fragment.reoptimize(queryPlanner);
            }

            @Override public PlanFragment reoptimize(PlanFragment fragment,
                    org.pangrova.planner.api.adaptive.JoinStrategy hint) {
                return fragment.reoptimize(queryPlanner, hint);
            }
        };
    }

    private PangrovaPlanner planningPlanner() {
        if (sessionPlanner != null) {
            throw new IllegalStateException("Metadata-aware SQL compilation requires forSession binding");
        }
        return planner;
    }

    /** Returns the underlying planner for advanced feature access. */
    public PangrovaPlanner planner() {
        return planningPlanner();
    }

    /**
     * Compile SQL text into a PlanResult (RelNode + metadata).
     */
    public PlanResult compileToResult(String sql) {
        return planningPlanner().plan(sql);
    }

    /** Validated query input for the authority/policy phase, before relational optimization. */
    public PlanResult compileForPolicyApplication(String sql) {
        return planningPlanner().planForPolicyApplication(sql);
    }

    /** Binds an expanded metadata listing before any security-sensitive optimization. */
    public PlanResult compileForPolicyApplication(MetadataTableQuery query) {
        return planningPlanner().planForPolicyApplication(query);
    }

    /** Continues the same query-local compilation after its policies have been applied. */
    public PlannedQuery optimizeProtectedQuery(PlannedQuery query) {
        return planningPlanner().optimizeProtectedQuery(query);
    }

    /**
     * Continues compilation after policies were applied, optionally allowing
     * materialized view substitution when the caller proved the query is unprotected.
     *
     * @param query policy-applied plan
     * @param allowMaterializationRewrite whether transparent rewrite is safe
     * @return optimized plan
     */
    public PlannedQuery optimizeProtectedQuery(PlannedQuery query,
            boolean allowMaterializationRewrite) {
        return planningPlanner().optimizeProtectedQuery(query, allowMaterializationRewrite);
    }

    /**
     * Returns the diagnostic decision of the most recent materialized-view rewrite.
     *
     * @return rewrite decision; disabled before any attempt or without a catalog
     */
    public org.pangrova.planner.core.mv.MaterializedViewRewriteDecision
            lastMaterializedViewRewrite() {
        return planningPlanner().lastMaterializedViewRewrite();
    }

    /**
     * Compile SQL text into a {@link PlannedQuery} — a Calcite-free opaque wrapper
     * suitable for use by coordinator code without any Calcite imports.
     */
    public PlannedQuery compileToPlannedQuery(String sql) {
        return planningPlanner().plan(sql).toPlannedQuery();
    }

    /**
     * Compile a materialization definition without recursively rewriting it
     * through another published materialization.
     *
     * @param sql definition SQL
     * @return planned definition
     */
    public PlannedQuery compileDefinitionQuery(String sql) {
        return planningPlanner().planDefinition(sql).toPlannedQuery();
    }

    /**
     * Compile SQL text into a Calcite RelNode without Substrait conversion.
     */
    public RelNode compileToRelNode(String sql) {
        return planningPlanner().plan(sql).plan();
    }

    /**
     * Compile a pre-parsed AST Statement into a Calcite RelNode without Substrait conversion.
     * Bypasses the AST → SQL string → re-parse round-trip.
     */
    public RelNode compileStatementToRelNode(org.pangrova.parser.api.ast.Statement stmt) {
        return planningPlanner().planFromAst(stmt).plan();
    }

    /**
     * Compile a pre-parsed AST Statement into a {@link PlannedQuery}.
     */
    public PlannedQuery compileStatementToPlannedQuery(org.pangrova.parser.api.ast.Statement stmt) {
        return planningPlanner().planFromAst(stmt).toPlannedQuery();
    }

    /**
     * Parse SQL text into an AST Statement without Calcite conversion.
     * Used to detect DDL/DML statements that bypass the Calcite planner.
     */
    public org.pangrova.parser.api.ast.Statement parseToAst(String sql) {
        return planner.parseToAst(sql);
    }

    /**
     * Convert, validate against native engine capabilities, and serialize.
     */
    public byte[] convertValidateAndSerialize(RelNode relNode, List<String> preOptColumnNames) {
        return convertValidateAndSerializeFragment(relNode, preOptColumnNames).planBytes();
    }

    /** Converts and validates a fragment while preserving typed relation intents. */
    public SerializedPlanFragment convertValidateAndSerializeFragment(
            RelNode relNode, List<String> preOptColumnNames) {
        return doConvertValidateAndSerialize(relNode, preOptColumnNames);
    }

    private SerializedPlanFragment doConvertValidateAndSerialize(
            RelNode relNode, List<String> preOptColumnNames) {
        ConversionResult convResult = converter.convertWithResult(relNode, preOptColumnNames);
        PlanValidationResult result = validator.validate(convResult.plan());
        // Only converter-produced governed AI intents are deferred to exact
        // Worker/model binding. All other unsupported nodes fail before dispatch.
        var deferredFunctions = convResult.aiInvocationHints().stream()
                .map(AiInvocationPlanHint::functionName)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        var unsupported = result.unsupportedNodes().stream()
                .filter(node -> node.kind()
                        != org.pangrova.planner.core.validation.UnsupportedNode.Kind.FUNCTION
                        || !deferredFunctions.contains(node.nodeType().toLowerCase(java.util.Locale.ROOT)))
                .toList();
        if (!unsupported.isEmpty()) {
            throw new PlannerException(PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                    "Plan contains unsupported operators: " + unsupported);
        }
        return new SerializedPlanFragment(
                serializer.serialize(convResult.plan()),
                convResult.retrievalTopKHints(),
                convResult.aiInvocationHints());
    }

    /** Compiles SQL through the single Calcite optimization and Substrait conversion path. */
    public byte[] compile(String sql) {
        return compile(sql, "");
    }

    /** Compiles SQL with its query identity; failures propagate before task dispatch. */
    public byte[] compile(String sql, String queryId) {
        PlanResult result = compileToResult(sql);
        return convertValidateAndSerialize(result.plan(), result.preOptimizationColumnNames());
    }
}
