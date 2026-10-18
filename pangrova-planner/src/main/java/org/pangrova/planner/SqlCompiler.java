package org.pangrova.planner;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.substrait.ConversionResult;
import org.pangrova.planner.substrait.SubstraitConversionException;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.pangrova.planner.validation.SubstraitPlanValidator;
import org.pangrova.planner.validation.PlanValidationResult;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.pangrova.spi.connector.ConnectorSession;

/**
 * End-to-end SQL compilation: SQL text → Substrait Plan bytes.
 * Core compile chain: planner.plan() → converter.convert() → validator.validate() → serializer.serialize().
 * Includes safe-plan retry on Calcite REDUCE_EXPRESSIONS bugs.
 *
 * <p>Security concerns (access control, column masking, row-level security) are handled
 * by dedicated components in {@code org.pangrova.coordinator.query.security}.
 */
public class SqlCompiler {

    private static final Logger LOG = LoggerFactory.getLogger(SqlCompiler.class);

    private final PangrovaPlanner planner;
    private final SubstraitConverter converter;
    private final SubstraitPlanSerializer serializer;
    private final SubstraitPlanValidator validator;
    private final Function<ConnectorSession, PangrovaPlanner> sessionPlanner;
    private final org.pangrova.planner.cache.QueryResultCache queryResultCache;
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
                        org.pangrova.planner.cache.QueryResultCache queryResultCache,
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
    public SqlCompiler withQueryResultCache(org.pangrova.planner.cache.QueryResultCache cache) {
        return new SqlCompiler(planner, converter, serializer, validator, sessionPlanner, cache, sessionScope);
    }

    public org.pangrova.planner.cache.QueryResultCache queryResultCache() {
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
                    org.pangrova.planner.adaptive.JoinStrategy hint) {
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

    /** Continues the same query-local compilation after its policies have been applied. */
    public PlannedQuery optimizeProtectedQuery(PlannedQuery query) {
        return planningPlanner().optimizeProtectedQuery(query);
    }

    /**
     * Compile SQL text into a {@link PlannedQuery} — a Calcite-free opaque wrapper
     * suitable for use by coordinator code without any Calcite imports.
     */
    public PlannedQuery compileToPlannedQuery(String sql) {
        return planningPlanner().plan(sql).toPlannedQuery();
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
    public RelNode compileStatementToRelNode(org.pangrova.parser.ast.Statement stmt) {
        return planningPlanner().planFromAst(stmt).plan();
    }

    /**
     * Compile a pre-parsed AST Statement into a {@link PlannedQuery}.
     */
    public PlannedQuery compileStatementToPlannedQuery(org.pangrova.parser.ast.Statement stmt) {
        return planningPlanner().planFromAst(stmt).toPlannedQuery();
    }

    /**
     * Parse SQL text into an AST Statement without Calcite conversion.
     * Used to detect DDL/DML statements that bypass the Calcite planner.
     */
    public org.pangrova.parser.ast.Statement parseToAst(String sql) {
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
                        != org.pangrova.planner.validation.UnsupportedNode.Kind.FUNCTION
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

    /**
     * Compile SQL text into Substrait Plan bytes.
     * If Substrait conversion fails due to Calcite REDUCE_EXPRESSIONS producing
     * invalid field references (Integer.MIN_VALUE index), retries with safe
     * optimization that skips expression reduction rules.
     */
    public byte[] compile(String sql) {
        return compile(sql, "");
    }

    /**
     * Compile SQL text into Substrait Plan bytes with query ID for log correlation.
     */
    public byte[] compile(String sql, String queryId) {
        PlanResult result = compileToResult(sql);
        try {
            return convertValidateAndSerialize(result.plan(), result.preOptimizationColumnNames());
        } catch (Exception e) {
            if (isReduceExpressionsBug(e)) {
                LOG.warn("event=substrait_conversion_failed_invalid_ref action=retry_safe sql_length={} queryId={}",
                        sql.length(), queryId);
                PlanResult safeResult = planningPlanner().planSafe(sql);
                return convertValidateAndSerialize(safeResult.plan(), safeResult.preOptimizationColumnNames());
            }
            throw e;
        }
    }

    private static boolean isReduceExpressionsBug(Exception e) {
        if (e instanceof SubstraitConversionException) {
            String msg = e.getMessage();
            return msg != null && (msg.contains("REDUCE_EXPRESSIONS bug")
                    || msg.contains("out of bounds"));
        }
        if (e instanceof ArrayIndexOutOfBoundsException
                || e instanceof IndexOutOfBoundsException) {
            return hasCalciteReduceExpressionsInStack(e);
        }
        return false;
    }

    private static boolean hasCalciteReduceExpressionsInStack(Exception e) {
        for (StackTraceElement frame : e.getStackTrace()) {
            String className = frame.getClassName();
            if (className.contains("ReduceExpressionsRule")
                    || className.contains("RexSimplify")) {
                return true;
            }
        }
        return false;
    }
}
