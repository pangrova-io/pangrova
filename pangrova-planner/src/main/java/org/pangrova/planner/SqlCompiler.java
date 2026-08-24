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
        this.planner = planner;
        this.converter = converter;
        this.serializer = serializer;
        this.validator = validator;
    }

    /** Returns the underlying planner for advanced feature access. */
    public PangrovaPlanner planner() {
        return planner;
    }

    /**
     * Compile SQL text into a PlanResult (RelNode + metadata).
     */
    public PlanResult compileToResult(String sql) {
        return planner.plan(sql);
    }

    /**
     * Compile SQL text into a {@link PlannedQuery} — a Calcite-free opaque wrapper
     * suitable for use by coordinator code without any Calcite imports.
     */
    public PlannedQuery compileToPlannedQuery(String sql) {
        return planner.plan(sql).toPlannedQuery();
    }

    /**
     * Compile SQL text into a Calcite RelNode without Substrait conversion.
     */
    public RelNode compileToRelNode(String sql) {
        return planner.plan(sql).plan();
    }

    /**
     * Compile a pre-parsed AST Statement into a Calcite RelNode without Substrait conversion.
     * Bypasses the AST → SQL string → re-parse round-trip.
     */
    public RelNode compileStatementToRelNode(org.pangrova.parser.ast.Statement stmt) {
        return planner.planFromAst(stmt).plan();
    }

    /**
     * Compile a pre-parsed AST Statement into a {@link PlannedQuery}.
     */
    public PlannedQuery compileStatementToPlannedQuery(org.pangrova.parser.ast.Statement stmt) {
        return planner.planFromAst(stmt).toPlannedQuery();
    }

    /**
     * Converts an AST Statement to a SQL string via Calcite SqlNode serialization.
     * Used to provide originalSql for the REDUCE_EXPRESSIONS retry path when
     * the caller only has a pre-parsed AST (e.g. DML dispatch via executeDmlStatement).
     */
    public String statementToSql(org.pangrova.parser.ast.Statement stmt) {
        return planner.statementToSql(stmt);
    }

    /**
     * Parse SQL text into an AST Statement without Calcite conversion.
     * Used to detect DDL/DML statements that bypass the Calcite planner.
     */
    public org.pangrova.parser.ast.Statement parseToAst(String sql) {
        return planner.parseToAst(sql);
    }

    /**
     * Convert a Calcite RelNode to Substrait and serialize to Protobuf bytes.
     */
    public byte[] convertAndSerialize(RelNode relNode, List<String> preOptColumnNames) {
        ConversionResult result = converter.convertWithResult(relNode, preOptColumnNames);
        return serializer.serialize(result.plan(), result.crossJoinDetails());
    }

    /**
     * Convert, validate against native engine capabilities, and serialize.
     */
    public byte[] convertValidateAndSerialize(RelNode relNode, List<String> preOptColumnNames) {
        return doConvertValidateAndSerialize(relNode, preOptColumnNames);
    }

    /**
     * Convert, validate, and serialize with safe-plan retry support.
     * Called from QueryDispatcher which passes the original SQL for retry.
     */
    public byte[] convertValidateAndSerializeWithRetry(RelNode relNode,
                                                        List<String> preOptColumnNames,
                                                        String originalSql) {
        try {
            return doConvertValidateAndSerialize(relNode, preOptColumnNames);
        } catch (Exception e) {
            if (isReduceExpressionsBug(e) && originalSql != null) {
                LOG.warn("event=substrait_conversion_invalid_ref action=retry_safe_plan");
                PlanResult safeResult = planner.planSafe(originalSql);
                return doConvertValidateAndSerialize(safeResult.plan(),
                        safeResult.preOptimizationColumnNames());
            }
            throw e;
        }
    }

    private byte[] doConvertValidateAndSerialize(RelNode relNode, List<String> preOptColumnNames) {
        ConversionResult convResult = converter.convertWithResult(relNode, preOptColumnNames);
        PlanValidationResult result = validator.validate(convResult.plan());
        switch (result.status()) {
            case JAVA_ONLY -> throw new PlannerException(
                    PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                    "Plan contains unsupported operators: " + result.unsupportedNodes());
            case PARTIAL_NATIVE -> LOG.info(
                    "event=partial_native_plan action=proceed_with_fallback unsupported={}",
                    result.unsupportedNodes());
            case FULLY_NATIVE -> { /* no-op */ }
            default -> throw new IllegalStateException(
                    "Unhandled native support status: " + result.status());
        }
        return serializer.serialize(convResult.plan(), convResult.crossJoinDetails());
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
            return convertAndSerialize(result.plan(), result.preOptimizationColumnNames());
        } catch (Exception e) {
            if (isReduceExpressionsBug(e)) {
                LOG.warn("event=substrait_conversion_failed_invalid_ref action=retry_safe sql_length={} queryId={}",
                        sql.length(), queryId);
                PlanResult safeResult = planner.planSafe(sql);
                return convertAndSerialize(safeResult.plan(), safeResult.preOptimizationColumnNames());
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
