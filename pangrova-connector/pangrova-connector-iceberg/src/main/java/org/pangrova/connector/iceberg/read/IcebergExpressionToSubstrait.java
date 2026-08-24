package org.pangrova.connector.iceberg.read;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.protobuf.ByteString;
import io.substrait.proto.Expression;
import io.substrait.proto.Expression.FieldReference;
import io.substrait.proto.Expression.Literal;
import io.substrait.proto.Expression.ReferenceSegment;
import io.substrait.proto.Expression.ScalarFunction;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.BoundUnaryPredicate;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.False;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Iceberg bound expressions to serialized Substrait ExtendedExpression bytes.
 *
 * <p>Supports comparison operators (EQ, NEQ, LT, LT_EQ, GT, GT_EQ),
 * logical combinators (AND, OR, NOT), set predicates (IN), and
 * unary predicates (IS_NULL, NOT_NULL). Unsupported expressions
 * cause the converter to return {@link Optional#empty()}, leaving
 * the predicate as a Java-side residual filter.</p>
 */
public final class IcebergExpressionToSubstrait {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergExpressionToSubstrait.class);

    private static final String FUNCTIONS_COMPARISON_URI = "/functions_comparison.yaml";
    private static final String FUNCTIONS_BOOLEAN_URI = "/functions_boolean.yaml";
    private static final int COMPARISON_URI_ANCHOR = 1;
    private static final int BOOLEAN_URI_ANCHOR = 2;

    private static final Map<String, Integer> FUNCTION_REFS = new HashMap<>();
    private static int nextFunctionRef = 1;

    static {
        FUNCTION_REFS.put("equal", nextFunctionRef++);
        FUNCTION_REFS.put("not_equal", nextFunctionRef++);
        FUNCTION_REFS.put("lt", nextFunctionRef++);
        FUNCTION_REFS.put("lte", nextFunctionRef++);
        FUNCTION_REFS.put("gt", nextFunctionRef++);
        FUNCTION_REFS.put("gte", nextFunctionRef++);
        FUNCTION_REFS.put("and", nextFunctionRef++);
        FUNCTION_REFS.put("or", nextFunctionRef++);
        FUNCTION_REFS.put("not", nextFunctionRef++);
        FUNCTION_REFS.put("is_null", nextFunctionRef++);
        FUNCTION_REFS.put("is_not_null", nextFunctionRef++);
    }

    /**
     * Converts an Iceberg residual expression to serialized Substrait ExtendedExpression bytes.
     *
     * @param residual   the bound Iceberg expression from FileScanTask.residual()
     * @param tableSchema the Iceberg table schema for column index resolution
     * @return serialized Substrait bytes, or empty if the expression cannot be converted
     */
    public Optional<byte[]> convert(org.apache.iceberg.expressions.Expression residual,
                                    Schema tableSchema) {
        if (residual == null) {
            return Optional.empty();
        }
        if (residual instanceof True) {
            return Optional.empty();
        }
        if (residual instanceof False) {
            return Optional.empty();
        }

        try {
            Map<Integer, Integer> fieldIdToIndex = buildFieldIdToIndexMap(tableSchema);
            Expression substraitExpr = convertExpression(residual, tableSchema, fieldIdToIndex);
            if (substraitExpr == null) {
                return Optional.empty();
            }
            byte[] serialized = buildExtendedExpression(substraitExpr, tableSchema);
            return Optional.of(serialized);
        } catch (UnsupportedExpressionException e) {
            LOG.debug("event=unsupported_iceberg_expression expression={}", residual, e);
            return Optional.empty();
        }
    }

    private Expression convertExpression(org.apache.iceberg.expressions.Expression expr,
                                         Schema schema,
                                         Map<Integer, Integer> fieldIdToIndex) {
        if (expr instanceof And and) {
            return convertBinaryBoolean("and", and.left(), and.right(), schema, fieldIdToIndex);
        }
        if (expr instanceof Or or) {
            return convertBinaryBoolean("or", or.left(), or.right(), schema, fieldIdToIndex);
        }
        if (expr instanceof Not not) {
            Expression child = convertExpression(not.child(), schema, fieldIdToIndex);
            if (child == null) {
                throw new UnsupportedExpressionException("NOT operand not convertible");
            }
            return buildUnaryBooleanFunction("not", child);
        }
        if (expr instanceof BoundPredicate<?> predicate) {
            return convertPredicate(predicate, schema, fieldIdToIndex);
        }
        throw new UnsupportedExpressionException("Unsupported expression type: " + expr.getClass().getSimpleName());
    }

    private Expression convertBinaryBoolean(String name,
                                            org.apache.iceberg.expressions.Expression leftOperand,
                                            org.apache.iceberg.expressions.Expression rightOperand,
                                            Schema schema,
                                            Map<Integer, Integer> fieldIdToIndex) {
        Expression left = convertExpression(leftOperand, schema, fieldIdToIndex);
        Expression right = convertExpression(rightOperand, schema, fieldIdToIndex);
        if (left == null || right == null) {
            throw new UnsupportedExpressionException(name.toUpperCase() + " operand not convertible");
        }
        return buildBooleanFunction(name, left, right);
    }

    private Expression convertPredicate(BoundPredicate<?> predicate,
                                        Schema schema,
                                        Map<Integer, Integer> fieldIdToIndex) {
        int fieldId = predicate.ref().fieldId();
        Integer columnIndex = fieldIdToIndex.get(fieldId);
        if (columnIndex == null) {
            throw new UnsupportedExpressionException("Field ID not found in schema: " + fieldId);
        }
        Types.NestedField field = schema.findField(fieldId);
        Type substraitType = mapIcebergType(field.type());

        if (predicate instanceof BoundUnaryPredicate<?> unary) {
            return convertUnaryPredicate(unary, columnIndex, substraitType);
        }
        if (predicate instanceof BoundLiteralPredicate<?> literal) {
            return convertLiteralPredicate(literal, columnIndex, substraitType);
        }
        if (predicate instanceof BoundSetPredicate<?> setPred) {
            return convertSetPredicate(setPred, columnIndex, substraitType);
        }
        throw new UnsupportedExpressionException(
                "Unsupported predicate type: " + predicate.getClass().getSimpleName());
    }

    private Expression convertUnaryPredicate(BoundUnaryPredicate<?> predicate,
                                             int columnIndex,
                                             Type substraitType) {
        Expression fieldRef = buildFieldReference(columnIndex, substraitType);
        return switch (predicate.op()) {
            case IS_NULL -> buildUnaryComparisonFunction("is_null", fieldRef);
            case NOT_NULL -> buildUnaryComparisonFunction("is_not_null", fieldRef);
            default -> throw new UnsupportedExpressionException(
                    "Unsupported unary op: " + predicate.op());
        };
    }

    private Expression convertLiteralPredicate(BoundLiteralPredicate<?> predicate,
                                               int columnIndex,
                                               Type substraitType) {
        Expression fieldRef = buildFieldReference(columnIndex, substraitType);
        Literal literalValue = convertLiteral(predicate.literal().value(), substraitType);
        Expression literalExpr = Expression.newBuilder().setLiteral(literalValue).build();

        String functionName = switch (predicate.op()) {
            case EQ -> "equal";
            case NOT_EQ -> "not_equal";
            case LT -> "lt";
            case LT_EQ -> "lte";
            case GT -> "gt";
            case GT_EQ -> "gte";
            default -> throw new UnsupportedExpressionException(
                    "Unsupported comparison op: " + predicate.op());
        };
        return buildComparisonFunction(functionName, fieldRef, literalExpr, substraitType);
    }

    private Expression convertSetPredicate(BoundSetPredicate<?> predicate,
                                           int columnIndex,
                                           Type substraitType) {
        if (predicate.op() != org.apache.iceberg.expressions.Expression.Operation.IN
                && predicate.op() != org.apache.iceberg.expressions.Expression.Operation.NOT_IN) {
            throw new UnsupportedExpressionException("Unsupported set op: " + predicate.op());
        }

        Expression fieldRef = buildFieldReference(columnIndex, substraitType);
        Expression.SingularOrList.Builder orListBuilder = Expression.SingularOrList.newBuilder()
                .setValue(fieldRef);

        Set<?> literalSet = predicate.literalSet();
        for (Object value : literalSet) {
            Literal lit = convertLiteral(value, substraitType);
            orListBuilder.addOptions(Expression.newBuilder().setLiteral(lit).build());
        }

        Expression inExpr = Expression.newBuilder()
                .setSingularOrList(orListBuilder.build())
                .build();

        if (predicate.op() == org.apache.iceberg.expressions.Expression.Operation.NOT_IN) {
            return buildUnaryBooleanFunction("not", inExpr);
        }
        return inExpr;
    }

    private Expression buildFieldReference(int columnIndex, Type substraitType) {
        return Expression.newBuilder()
                .setSelection(FieldReference.newBuilder()
                        .setDirectReference(ReferenceSegment.newBuilder()
                                .setStructField(ReferenceSegment.StructField.newBuilder()
                                        .setField(columnIndex)
                                        .build())
                                .build())
                        .setRootReference(FieldReference.RootReference.getDefaultInstance())
                        .build())
                .build();
    }

    private Expression buildComparisonFunction(String name, Expression left,
                                               Expression right, Type outputType) {
        int funcRef = FUNCTION_REFS.get(name);
        return Expression.newBuilder()
                .setScalarFunction(ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .setOutputType(Type.newBuilder()
                                .setBool(Type.Boolean.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                                        .build())
                                .build())
                        .addArguments(FunctionArgument.newBuilder().setValue(left).build())
                        .addArguments(FunctionArgument.newBuilder().setValue(right).build())
                        .build())
                .build();
    }

    private Expression buildBooleanFunction(String name, Expression left, Expression right) {
        int funcRef = FUNCTION_REFS.get(name);
        return Expression.newBuilder()
                .setScalarFunction(ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .setOutputType(Type.newBuilder()
                                .setBool(Type.Boolean.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                                        .build())
                                .build())
                        .addArguments(FunctionArgument.newBuilder().setValue(left).build())
                        .addArguments(FunctionArgument.newBuilder().setValue(right).build())
                        .build())
                .build();
    }

    private Expression buildUnaryBooleanFunction(String name, Expression operand) {
        int funcRef = FUNCTION_REFS.get(name);
        return Expression.newBuilder()
                .setScalarFunction(ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .setOutputType(Type.newBuilder()
                                .setBool(Type.Boolean.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                                        .build())
                                .build())
                        .addArguments(FunctionArgument.newBuilder().setValue(operand).build())
                        .build())
                .build();
    }

    private Expression buildUnaryComparisonFunction(String name, Expression operand) {
        int funcRef = FUNCTION_REFS.get(name);
        return Expression.newBuilder()
                .setScalarFunction(ScalarFunction.newBuilder()
                        .setFunctionReference(funcRef)
                        .setOutputType(Type.newBuilder()
                                .setBool(Type.Boolean.newBuilder()
                                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                                        .build())
                                .build())
                        .addArguments(FunctionArgument.newBuilder().setValue(operand).build())
                        .build())
                .build();
    }

    private Literal convertLiteral(Object value, Type substraitType) {
        if (value == null) {
            return Literal.newBuilder().setNull(substraitType).build();
        }
        Literal scalar = convertScalarLiteral(value);
        if (scalar != null) {
            return scalar;
        }
        Literal complex = convertComplexLiteral(value);
        if (complex != null) {
            return complex;
        }
        throw new UnsupportedExpressionException("Unsupported literal type: " + value.getClass().getSimpleName());
    }

    private Literal convertScalarLiteral(Object value) {
        if (value instanceof Boolean b) {
            return Literal.newBuilder().setBoolean(b).build();
        }
        if (value instanceof Integer i) {
            return Literal.newBuilder().setI32(i).build();
        }
        if (value instanceof Long l) {
            return Literal.newBuilder().setI64(l).build();
        }
        if (value instanceof Float f) {
            return Literal.newBuilder().setFp32(f).build();
        }
        if (value instanceof Double d) {
            return Literal.newBuilder().setFp64(d).build();
        }
        return null;
    }

    private Literal convertComplexLiteral(Object value) {
        if (value instanceof CharSequence s) {
            return Literal.newBuilder()
                    .setString(s.toString())
                    .build();
        }
        if (value instanceof ByteBuffer buf) {
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return Literal.newBuilder()
                    .setBinary(ByteString.copyFrom(bytes))
                    .build();
        }
        if (value instanceof BigDecimal decimal) {
            byte[] unscaled = decimal.unscaledValue().toByteArray();
            return Literal.newBuilder()
                    .setDecimal(Literal.Decimal.newBuilder()
                            .setValue(ByteString.copyFrom(unscaled))
                            .setPrecision(decimal.precision())
                            .setScale(decimal.scale())
                            .build())
                    .build();
        }
        if (value instanceof byte[] bytes) {
            return Literal.newBuilder()
                    .setBinary(ByteString.copyFrom(bytes))
                    .build();
        }
        return null;
    }

    private Type mapIcebergType(org.apache.iceberg.types.Type icebergType) {
        return switch (icebergType.typeId()) {
            case BOOLEAN -> Type.newBuilder()
                    .setBool(Type.Boolean.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case INTEGER -> Type.newBuilder()
                    .setI32(Type.I32.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case LONG -> Type.newBuilder()
                    .setI64(Type.I64.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case FLOAT -> Type.newBuilder()
                    .setFp32(Type.FP32.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case DOUBLE -> Type.newBuilder()
                    .setFp64(Type.FP64.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case STRING -> Type.newBuilder()
                    .setString(Type.String.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case BINARY, FIXED -> Type.newBuilder()
                    .setBinary(Type.Binary.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case DATE -> Type.newBuilder()
                    .setDate(Type.Date.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case TIMESTAMP -> Type.newBuilder()
                    .setTimestamp(Type.Timestamp.newBuilder()
                            .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                    .build();
            case DECIMAL -> mapDecimalType((Types.DecimalType) icebergType);
            default -> throw new UnsupportedExpressionException(
                    "Unsupported Iceberg type for Substrait mapping: " + icebergType);
        };
    }

    private Type mapDecimalType(Types.DecimalType decimalType) {
        return Type.newBuilder()
                .setDecimal(Type.Decimal.newBuilder()
                        .setPrecision(decimalType.precision())
                        .setScale(decimalType.scale())
                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
                .build();
    }

    private byte[] buildExtendedExpression(Expression expr, Schema tableSchema) {
        ExtendedExpression.Builder builder = ExtendedExpression.newBuilder();

        builder.addExtensionUris(SimpleExtensionURI.newBuilder()
                .setExtensionUriAnchor(COMPARISON_URI_ANCHOR)
                .setUri(FUNCTIONS_COMPARISON_URI)
                .build());
        builder.addExtensionUris(SimpleExtensionURI.newBuilder()
                .setExtensionUriAnchor(BOOLEAN_URI_ANCHOR)
                .setUri(FUNCTIONS_BOOLEAN_URI)
                .build());

        for (Map.Entry<String, Integer> entry : FUNCTION_REFS.entrySet()) {
            String funcName = entry.getKey();
            int ref = entry.getValue();
            int uriAnchor = isComparisonFunction(funcName) ? COMPARISON_URI_ANCHOR : BOOLEAN_URI_ANCHOR;
            builder.addExtensions(SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                            SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                                    .setExtensionUriReference(uriAnchor)
                                    .setFunctionAnchor(ref)
                                    .setName(funcName)
                                    .build())
                    .build());
        }

        NamedStruct.Builder namedStruct = NamedStruct.newBuilder();
        Type.Struct.Builder structType = Type.Struct.newBuilder();
        for (Types.NestedField field : tableSchema.columns()) {
            namedStruct.addNames(field.name());
            structType.addTypes(mapIcebergType(field.type()));
        }
        namedStruct.setStruct(structType.build());
        builder.setBaseSchema(namedStruct.build());

        builder.addReferredExpr(ExpressionReference.newBuilder()
                .setExpression(expr)
                .addOutputNames("filter")
                .build());

        return builder.build().toByteArray();
    }

    private static boolean isComparisonFunction(String name) {
        return switch (name) {
            case "equal", "not_equal", "lt", "lte", "gt", "gte", "is_null", "is_not_null" -> true;
            default -> false;
        };
    }

    private static Map<Integer, Integer> buildFieldIdToIndexMap(Schema schema) {
        Map<Integer, Integer> map = new HashMap<>();
        List<Types.NestedField> columns = schema.columns();
        for (int i = 0; i < columns.size(); i++) {
            map.put(columns.get(i).fieldId(), i);
        }
        return map;
    }

    private static final class UnsupportedExpressionException extends RuntimeException {
        UnsupportedExpressionException(String message) {
            super(message);
        }
    }
}
