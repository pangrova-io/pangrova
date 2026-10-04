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
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.pangrova.planner.AiInvocationPlanHint;

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
    private final AiInvocationBinder aiInvocationBinder;
    private final FunctionArgumentNormalizer argumentNormalizer;
    private int correlateOuterFieldOffset = 0;

    ExpressionConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                     WindowFunctionConverter windowConverter, RexBuilder rexBuilder,
                     LiteralConverter literalConverter, Consumer<AiInvocationPlanHint> aiInvocationSink) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
        this.windowConverter = windowConverter;
        this.rexBuilder = rexBuilder;
        this.literalConverter = literalConverter;
        this.aiInvocationBinder = new AiInvocationBinder(aiInvocationSink);
        this.argumentNormalizer = new FunctionArgumentNormalizer(rexBuilder, literalConverter);
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

    Expression convertValuesLiteral(RexLiteral literal, RelDataType fieldType) {
        return literalConverter.convert(literal, fieldType);
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
            case ROW -> convertStructConstructor(call, call.getType());
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
        var flagValue = flagLiteral.getValue();
        if (flagValue == null) throw unsupported("TRIM requires a non-null flag");
        String flag = flagValue.toString();
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
        var unitValue = unitLiteral.getValue();
        if (unitValue == null) throw unsupported("EXTRACT requires a non-null unit");
        String unitName = unitValue.toString();
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
     * Every literal uses the array's declared component type, including precision
     * and nullability, through the shared literal converter.
     * Runtime elements use the canonical Nested.List constructor; no function
     * name or implicit numeric coercion is invented for the wire contract.
     */
    private Expression convertArrayConstructor(RexCall call) {
        return convertArrayConstructor(call, call.getType());
    }

    private Expression convertArrayConstructor(RexCall call, RelDataType arrayType) {
        RelDataType componentType = arrayType.getComponentType();
        if (componentType == null) throw unsupported("array constructor without a component type");
        List<Expression> elements = call.getOperands().stream()
                .map(operand -> convertArrayElement(operand, componentType))
                .toList();
        if (elements.isEmpty()) {
            return convertEmptyArray(arrayType);
        }
        if (!elements.stream().allMatch(element -> element instanceof Expression.Literal)) {
            return ImmutableExpression.NestedList.builder().nullable(arrayType.isNullable()).values(elements).build();
        }
        return ImmutableExpression.ListLiteral.builder()
                .nullable(arrayType.isNullable())
                .values(elements.stream().map(element -> (Expression.Literal) element).toList())
                .build();
    }

    private Expression convertArrayElement(RexNode operand, RelDataType componentType) {
        if (operand instanceof RexLiteral literal) return literalConverter.convert(literal, componentType);
        if (operand instanceof RexCall nested && nested.getKind() == org.apache.calcite.sql.SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
            return convertArrayConstructor(nested, componentType);
        }
        if (operand instanceof RexCall row && row.getKind() == org.apache.calcite.sql.SqlKind.ROW) {
            return convertStructConstructor(row, componentType);
        }
        Expression converted = convert(operand);
        Type declared = typeMapper.map(componentType);
        if (declared.equals(converted.getType())) return converted;
        return ImmutableExpression.Cast.builder().input(converted).type(declared)
                .failureBehavior(Expression.FailureBehavior.THROW_EXCEPTION).build();
    }

    private Expression convertStructConstructor(RexCall call, RelDataType rowType) {
        if (rowType.getSqlTypeName() != SqlTypeName.ROW || call.getOperands().size() != rowType.getFieldCount()) {
            throw unsupported("ROW constructor differs from its declared field types");
        }
        List<Expression> fields = new ArrayList<>();
        for (int field = 0; field < rowType.getFieldCount(); field++) {
            fields.add(convertArrayElement(call.getOperands().get(field), rowType.getFieldList().get(field).getType()));
        }
        return ImmutableExpression.NestedStruct.builder().fields(fields).nullable(rowType.isNullable()).build();
    }

    private Expression convertScalarFunction(RexCall call) {
        String opName = call.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
        if ("VARIANT_GET".equals(opName)) {
            return convertVariantGet(call);
        }
        FunctionReference ref = functionMapper.map(call.getOperator());
        var variant = functionMapper.resolveScalarVariant(ref);
        List<Expression> args = call.getOperands().stream().map(this::convert).toList();
        args = argumentNormalizer.normalize(ref, call, args);
        args = aiInvocationBinder.bind(ref, call, opName, args);
        Type outputType = typeMapper.map(call.getType());
        var rewrite = org.pangrova.protocol.function.declaration.FunctionBindingDeclarations
                .rewrite(ref.namespace(), ref.name());
        if (rewrite.isPresent()) return DeclaredScalarProgram.lower(rewrite.orElseThrow(), args, outputType, functionMapper);
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
        RelDataType branchType = call.getType();
        List<Expression.IfClause> clauses = buildIfClauses(operands, branchType);
        Expression elseExpr = convertCaseBranch(operands.get(operandCount - 1), branchType);
        return ImmutableExpression.IfThen.builder()
                .ifClauses(clauses)
                .elseClause(elseExpr)
                .build();
    }

    private List<Expression.IfClause> buildIfClauses(List<RexNode> operands, RelDataType branchType) {
        List<Expression.IfClause> clauses = new ArrayList<>();
        for (int i = 0; i < operands.size() - 1; i += 2) {
            Expression condition = convert(operands.get(i));
            Expression thenExpr = convertCaseBranch(operands.get(i + 1), branchType);
            clauses.add(ImmutableExpression.IfClause.builder()
                    .condition(condition)
                    .then(thenExpr)
                    .build());
        }
        return clauses;
    }

    /** CASE branches use the validator's declared result type, never a guessed dominant branch. */
    private Expression convertCaseBranch(RexNode node, RelDataType branchType) {
        var target = rexBuilder.getTypeFactory().createTypeWithNullability(branchType, node.getType().isNullable());
        if (node instanceof RexLiteral literal) {
            return literalConverter.convert(literal, target);
        }
        return convert(org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
                rexBuilder.getTypeFactory(), node.getType(), target) ? node : rexBuilder.makeCast(target, node));
    }

    /**
     * Converts Calcite CAST to a Substrait Cast expression, preserving target type.
     */
    private Expression convertCast(RexCall call) {
        RexNode operand = call.getOperands().get(0);
        if (operand instanceof RexCall array
                && array.getKind() == org.apache.calcite.sql.SqlKind.ARRAY_VALUE_CONSTRUCTOR
                && array.getOperands().isEmpty()
                && call.getType().getSqlTypeName() == SqlTypeName.ARRAY) {
            return convertEmptyArray(call.getType());
        }
        Expression input = convert(call.getOperands().get(0));
        Type targetType = typeMapper.map(call.getType());
        return ImmutableExpression.Cast.builder()
                .input(input)
                .type(targetType)
                .failureBehavior(Expression.FailureBehavior.THROW_EXCEPTION)
                .build();
    }

    private Expression convertEmptyArray(RelDataType arrayType) {
        RelDataType componentType = arrayType.getComponentType();
        if (componentType == null
                || componentType.getSqlTypeName() == SqlTypeName.NULL
                || componentType.getSqlTypeName() == SqlTypeName.ANY) {
            throw unsupported("empty array requires a typed element context");
        }
        return ImmutableExpression.EmptyListLiteral.builder()
                .nullable(arrayType.isNullable())
                .elementType(typeMapper.map(componentType))
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
