package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Calcite RexNode expressions to Substrait Expression.
 * Handles literals, input references, function calls, CASE, CAST, and window-over expressions.
 */
final class ExpressionConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionConverter.class);

    private static final Map<String, String> EXTRACT_UNIT_TO_FUNCTION = Map.ofEntries(
            Map.entry("YEAR", "year"), Map.entry("MONTH", "month"),
            Map.entry("DAY", "day"), Map.entry("HOUR", "hour"),
            Map.entry("MINUTE", "minute"), Map.entry("SECOND", "second"),
            Map.entry("QUARTER", "quarter"), Map.entry("DOW", "day_of_week"),
            Map.entry("DOY", "day_of_year"), Map.entry("WEEK", "week"));

    private final TypeMapper typeMapper;
    private final FunctionMapper functionMapper;
    private final WindowFunctionConverter windowConverter;
    private final RexBuilder rexBuilder;
    private final LiteralConverter literalConverter;
    private final ComparisonArgCoercer comparisonArgCoercer;
    private int correlateOuterFieldOffset = 0;

    ExpressionConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                     WindowFunctionConverter windowConverter, RexBuilder rexBuilder) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
        this.windowConverter = windowConverter;
        this.rexBuilder = rexBuilder;
        this.literalConverter = new LiteralConverter(typeMapper);
        this.comparisonArgCoercer = new ComparisonArgCoercer(this::convert);
    }

    void setCorrelateOuterFieldOffset(int offset) {
        this.correlateOuterFieldOffset = offset;
    }

    Expression convert(RexNode rexNode) {
        if (rexNode instanceof RexLiteral lit) return literalConverter.convert(lit);
        if (rexNode instanceof RexInputRef ref) return convertInputRef(ref);
        if (rexNode instanceof RexOver over) return windowConverter.convertOver(over, this::convert);
        return convertNonLeaf(rexNode);
    }

    private Expression convertNonLeaf(RexNode rexNode) {
        if (rexNode instanceof RexFieldAccess fa) return convertFieldAccess(fa);
        if (rexNode instanceof RexCall call) return convertCall(call);
        throw unsupported("RexNode: " + rexNode.getClass().getSimpleName());
    }

    private Expression convertInputRef(RexInputRef ref) {
        int idx = ref.getIndex();
        if (idx < 0) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Invalid field reference index " + idx
                            + " (likely Calcite REDUCE_EXPRESSIONS bug)");
        }
        Type type = typeMapper.map(ref.getType());
        return FieldReference.newRootStructReference(idx, type);
    }

    /**
     * Converts RexFieldAccess to a Substrait FieldReference.
     * Handles three cases:
     * 1. Correlated variable reference: offset by left input field count
     * 2. Struct field access (e.g., info.age): generates nested struct_field reference
     * 3. Nested field access (e.g., a.b.c): recursively builds nested references
     */
    private Expression convertFieldAccess(RexFieldAccess fieldAccess) {
        Type type = typeMapper.map(fieldAccess.getType());
        int fieldIndex = fieldAccess.getField().getIndex();

        // In a Substrait JoinRel context (after decorrelation), correlated-variable
        // field references must be offset by the left input's field count so that
        // FieldReference indices resolve to outer-row fields, not inner-row fields.
        if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
            return FieldReference.newRootStructReference(fieldIndex + correlateOuterFieldOffset, type);
        }

        // Struct field access: info.age where info is a column (RexInputRef)
        // Generate nested reference: root[rootIndex].struct_field[fieldIndex]
        if (fieldAccess.getReferenceExpr() instanceof RexInputRef inputRef) {
            int rootIndex = inputRef.getIndex();
            Type structType = typeMapper.map(inputRef.getType());
            return FieldReference.newRootStructReference(rootIndex, structType)
                    .dereferenceStruct(fieldIndex);
        }

        // Nested field access (e.g., a.b.c) — recursively dereference
        if (fieldAccess.getReferenceExpr() instanceof RexFieldAccess nestedAccess) {
            LOG.debug("event=nested_field_access depth=2+ field={}", fieldAccess.getField().getName());
            Expression inner = convertFieldAccess(nestedAccess);
            if (inner instanceof FieldReference fr) {
                return fr.dereferenceStruct(fieldIndex);
            }
        }

        return FieldReference.newRootStructReference(fieldIndex, type);
    }

    private Expression convertCall(RexCall call) {
        return switch (call.getKind()) {
            case CAST -> convertCast(call);
            case CASE -> convertCase(call);
            case SEARCH -> convertSearch(call);
            case SINGLE_VALUE -> convert(call.getOperands().get(0));
            case ARRAY_VALUE_CONSTRUCTOR -> convertArrayConstructor(call);
            case EXTRACT -> convertExtract(call);
            case TRIM -> convertTrim(call);
            case IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE -> convertIsTruthPredicate(call);
            default -> convertScalarFunction(call);
        };
    }

    /**
     * Rewrites Calcite's three-valued truth predicates into existing Substrait
     * primitives (is_null/is_not_null + and/or/not). Calcite wraps nullable
     * boolean conditions with IS TRUE (e.g. aggregate FILTER clauses, where
     * NULL must count as false); Substrait has no direct equivalent.
     *
     *   x IS TRUE      → x IS NOT NULL AND x
     *   x IS NOT TRUE  → x IS NULL OR NOT x
     *   x IS FALSE     → x IS NOT NULL AND NOT x
     *   x IS NOT FALSE → x IS NULL OR x
     */
    private Expression convertIsTruthPredicate(RexCall call) {
        RexNode operand = call.getOperands().get(0);
        RexNode rewritten = switch (call.getKind()) {
            case IS_TRUE -> rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.AND,
                    rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL, operand),
                    operand);
            case IS_NOT_TRUE -> rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
                    rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL, operand),
                    rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT, operand));
            case IS_FALSE -> rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.AND,
                    rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL, operand),
                    rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT, operand));
            case IS_NOT_FALSE -> rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
                    rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL, operand),
                    operand);
            default -> throw unsupported("truth predicate: " + call.getKind());
        };
        return convert(rewritten);
    }

    /**
     * Converts Calcite TRIM(flag, trimChar, source) to a Substrait scalar function.
     * Calcite represents TRIM as: operand[0] = SYMBOL literal (BOTH/LEADING/TRAILING),
     * operand[1] = trim character literal, operand[2] = source expression.
     * Maps to Velox functions: trim/ltrim/rtrim.
     */
    private Expression convertTrim(RexCall call) {
        RexLiteral flagLiteral = (RexLiteral) call.getOperands().get(0);
        String flag = flagLiteral.getValue().toString();
        String fnName = switch (flag) {
            case "LEADING" -> "ltrim";
            case "TRAILING" -> "rtrim";
            default -> "trim";  // BOTH
        };
        Expression sourceExpr = convert(call.getOperands().get(2));
        // For custom trim character (non-space), pass it as second argument
        List<Expression> args;
        RexNode trimCharNode = call.getOperands().get(1);
        if (trimCharNode instanceof RexLiteral trimLit) {
            String trimChar = trimLit.getValueAs(String.class);
            if (trimChar != null && !trimChar.equals(" ")) {
                Expression trimCharExpr = convert(trimCharNode);
                args = List.of(sourceExpr, trimCharExpr);
            } else {
                args = List.of(sourceExpr);
            }
        } else {
            args = List.of(sourceExpr, convert(trimCharNode));
        }
        FunctionReference ref = new FunctionReference("string", fnName);
        var variant = functionMapper.resolveScalarVariant(ref);
        Type outputType = typeMapper.map(call.getType());
        return ImmutableExpression.ScalarFunctionInvocation.builder()
                .declaration(variant)
                .outputType(outputType)
                .arguments(args)
                .build();
    }

    /**
     * Converts Calcite EXTRACT(timeUnit FROM expr) to a Substrait scalar function.
     * Calcite represents EXTRACT as a RexCall with two operands:
     * operand[0] = RexLiteral containing the TimeUnitRange enum (YEAR, MONTH, etc.)
     * operand[1] = the date/timestamp expression.
     * Each time unit maps to a dedicated Velox datetime function (year, month, day, etc.).
     */
    private Expression convertExtract(RexCall call) {
        RexLiteral unitLiteral = (RexLiteral) call.getOperands().get(0);
        String unitName = unitLiteral.getValue().toString();
        String substraitFnName = EXTRACT_UNIT_TO_FUNCTION.get(unitName);
        if (substraitFnName == null) {
            throw unsupported("EXTRACT time unit: " + unitName);
        }
        Expression sourceExpr = convert(call.getOperands().get(1));
        FunctionReference ref = new FunctionReference("datetime", substraitFnName);
        var variant = functionMapper.resolveScalarVariant(ref);
        Type outputType = typeMapper.map(call.getType());
        return ImmutableExpression.ScalarFunctionInvocation.builder()
                .declaration(variant)
                .outputType(outputType)
                .arguments(List.of(sourceExpr))
                .build();
    }

    /**
     * Converts Calcite ARRAY_VALUE_CONSTRUCTOR (e.g. ARRAY[1.0, 2.0, 3.0]) to a
     * Substrait ListLiteral. Each operand is converted to a Substrait expression.
     * Literal DECIMAL elements are coerced to FP64 for native engine compatibility.
     * Non-literal elements (column references) are not supported — callers must
     * rewrite such expressions into a ScalarFunctionInvocation before reaching here.
     */
    private Expression convertArrayConstructor(RexCall call) {
        List<Expression> elements = call.getOperands().stream()
                .map(this::coerceToFp64IfDecimal)
                .toList();
        List<Expression.Literal> literals = extractLiterals(elements);
        return ImmutableExpression.ListLiteral.builder()
                .nullable(call.getType().isNullable())
                .values(literals)
                .build();
    }

    private static List<Expression.Literal> extractLiterals(List<Expression> elements) {
        boolean allLiterals = elements.stream().allMatch(e -> e instanceof Expression.Literal);
        if (!allLiterals) {
            throw unsupported("array constructor with non-literal elements: "
                    + "use ScalarFunctionInvocation for runtime arrays");
        }
        return elements.stream().map(e -> (Expression.Literal) e).toList();
    }

    /**
     * Coerces a DECIMAL literal to FP64 for array elements and division operations.
     * The native engine's divide() and C++ convertListLiteral only handle fp64/fp32/integer
     * protobuf fields. Without this coercion, decimal values cause type mismatch.
     *
     * <p>Returns the original Expression unchanged for non-literal operands
     * (e.g. column references in ARRAY[col1, col2]).</p>
     */
    private Expression coerceToFp64IfDecimal(RexNode op) {
        Expression expr = convert(op);
        if (op instanceof RexLiteral lit
                && lit.getTypeName() == SqlTypeName.DECIMAL
                && expr instanceof Expression.Literal) {
            BigDecimal value = lit.getValueAs(BigDecimal.class);
            if (value != null) {
                return ExpressionCreator.fp64(lit.getType().isNullable(), value.doubleValue());
            }
        }
        return expr;
    }

    private Expression convertScalarFunction(RexCall call) {
        String opName = call.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
        if ("VARIANT_GET".equals(opName)) {
            return convertVariantGet(call);
        }
        FunctionReference ref = functionMapper.map(call.getOperator());
        var variant = functionMapper.resolveScalarVariant(ref);
        List<Expression> args = comparisonArgCoercer.convertComparisonArgs(call);
        args = comparisonArgCoercer.coerceDecimalLiteralsToInteger(call.getOperands(), args);
        Type outputType = typeMapper.map(call.getType());
        return ImmutableExpression.ScalarFunctionInvocation.builder()
                .declaration(variant)
                .outputType(outputType)
                .arguments(args)
                .build();
    }

    private Expression convertVariantGet(RexCall call) {
        FunctionReference ref = functionMapper.map(call.getOperator());
        var variant = functionMapper.resolveScalarVariant(ref);
        Expression jsonArg = convert(call.getOperands().get(0));
        Expression pathArg = convertVariantPath(call.getOperands().get(1));
        Type outputType = typeMapper.map(call.getType());
        return ImmutableExpression.ScalarFunctionInvocation.builder()
                .declaration(variant)
                .outputType(outputType)
                .arguments(List.of(jsonArg, pathArg))
                .build();
    }

    private Expression convertVariantPath(RexNode pathNode) {
        if (pathNode instanceof RexLiteral lit && lit.getTypeName() == SqlTypeName.CHAR) {
            String path = lit.getValueAs(String.class);
            if (path != null && !path.startsWith("$")) {
                String jsonPath = path.startsWith("[") ? "$" + path : "$." + path;
                return ExpressionCreator.string(lit.getType().isNullable(), jsonPath);
            }
        }
        return convert(pathNode);
    }

    /**
     * Converts Calcite CASE expression to Substrait IfThen.
     * Calcite CASE operands: [cond0, then0, cond1, then1, ..., elseVal].
     */
    private Expression convertCase(RexCall call) {
        List<RexNode> operands = call.getOperands();
        int operandCount = operands.size();
        if (operandCount % 2 == 0) {
            throw unsupported("CASE expression with even operand count (missing ELSE): " + operandCount);
        }
        SqlTypeName dominantType = findDominantBranchType(operands);
        List<Expression.IfClause> clauses = buildIfClauses(operands, dominantType);
        Expression elseExpr = convertCaseBranch(operands.get(operandCount - 1), dominantType);
        return ImmutableExpression.IfThen.builder()
                .ifClauses(clauses)
                .elseClause(elseExpr)
                .build();
    }

    private List<Expression.IfClause> buildIfClauses(List<RexNode> operands, SqlTypeName dominantType) {
        List<Expression.IfClause> clauses = new ArrayList<>();
        for (int i = 0; i < operands.size() - 1; i += 2) {
            Expression condition = convert(operands.get(i));
            Expression thenExpr = convertCaseBranch(operands.get(i + 1), dominantType);
            clauses.add(ImmutableExpression.IfClause.builder()
                    .condition(condition)
                    .then(thenExpr)
                    .build());
        }
        return clauses;
    }

    /**
     * Scans CASE then-branches to find the dominant non-literal type.
     * If any then-branch is DOUBLE/FLOAT, returns that type so DECIMAL
     * literals in other branches can be coerced. Falls back to the
     * first then-branch type.
     */
    private static SqlTypeName findDominantBranchType(List<RexNode> operands) {
        for (int i = 1; i < operands.size() - 1; i += 2) {
            SqlTypeName t = operands.get(i).getType().getSqlTypeName();
            if (t == SqlTypeName.DOUBLE || t == SqlTypeName.FLOAT) {
                return t;
            }
        }
        SqlTypeName elseType = operands.get(operands.size() - 1).getType().getSqlTypeName();
        if (elseType == SqlTypeName.DOUBLE || elseType == SqlTypeName.FLOAT) {
            return elseType;
        }
        return operands.get(1).getType().getSqlTypeName();
    }

    /**
     * Converts a CASE branch (then/else) expression, coercing DECIMAL literals
     * to DOUBLE/FLOAT when the dominant branch type requires it. The native engine requires
     * all SWITCH branches to have identical types.
     */
    private Expression convertCaseBranch(RexNode node, SqlTypeName dominantType) {
        if (node instanceof RexLiteral lit && !lit.isNull()) {
            SqlTypeName branchType = lit.getType().getSqlTypeName();
            if (branchType == SqlTypeName.DECIMAL
                    && (dominantType == SqlTypeName.DOUBLE || dominantType == SqlTypeName.FLOAT)) {
                boolean nullable = lit.getType().isNullable();
                BigDecimal value = lit.getValueAs(BigDecimal.class);
                return ExpressionCreator.fp64(nullable, value != null ? value.doubleValue() : 0.0);
            }
        }
        return convert(node);
    }

    /**
     * Converts Calcite CAST to a Substrait Cast expression, preserving target type.
     */
    private Expression convertCast(RexCall call) {
        Expression input = convert(call.getOperands().get(0));
        Type targetType = typeMapper.map(call.getType());
        return ImmutableExpression.Cast.builder()
                .input(input)
                .type(targetType)
                .failureBehavior(Expression.FailureBehavior.THROW_EXCEPTION)
                .build();
    }

    /**
     * Expands Calcite SEARCH (Sarg) back to standard comparison expressions.
     * Calcite's REDUCE_EXPRESSIONS rules compress IN/BETWEEN into SEARCH nodes;
     * we expand them so the Substrait converter sees standard operators.
     */
    private Expression convertSearch(RexCall call) {
        if (rexBuilder == null) {
            throw unsupported("SEARCH without RexBuilder context");
        }
        RexNode expanded = RexUtil.expandSearch(rexBuilder, null, call);
        return convert(expanded);
    }

    private static SubstraitConversionException unsupported(String detail) {
        return new SubstraitConversionException(
                PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                "Unsupported " + detail);
    }
}
