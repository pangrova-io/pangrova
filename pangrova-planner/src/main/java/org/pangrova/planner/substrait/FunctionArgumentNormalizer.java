package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.v2.FunctionNormalizedArgument;

import java.util.ArrayList;
import java.util.List;

/** Materializes exact numeric signatures and optional constants before hidden runtime arguments. */
final class FunctionArgumentNormalizer {
    record Arguments(List<Expression> values, Type outputType) {}

    private final RexBuilder rexBuilder;
    private final LiteralConverter literalConverter;
    private final TypeMapper typeMapper;

    FunctionArgumentNormalizer(RexBuilder rexBuilder, LiteralConverter literalConverter, TypeMapper typeMapper) {
        this.rexBuilder = rexBuilder;
        this.literalConverter = literalConverter;
        this.typeMapper = typeMapper;
    }

    Arguments normalize(FunctionReference reference, RexCall call, List<Expression> arguments) {
        var definitions = FunctionBindingDeclarations.normalizedArguments(
                "SCALAR", reference.namespace(), reference.name());
        List<Expression> normalized = new ArrayList<>(arguments);
        for (var definition : definitions) {
            int ordinal = definition.getOrdinal();
            if (ordinal == normalized.size()) {
                normalized.add(defaultArgument(definition));
            } else if (ordinal < normalized.size()) {
                normalized.set(ordinal, explicitArgument(definition, call, ordinal));
            } else {
                throw invalid("Optional arguments cannot skip required argument positions");
            }
        }
        return normalizePrimitiveNumbers(call, List.copyOf(normalized));
    }

    private Arguments normalizePrimitiveNumbers(RexCall call, List<Expression> arguments) {
        Type output = typeMapper.map(call.getType());
        boolean arithmetic = switch (call.getKind()) {
            case PLUS, MINUS, TIMES, DIVIDE, MOD -> true;
            default -> false;
        };
        boolean comparison = switch (call.getKind()) {
            case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL,
                    GREATER_THAN, GREATER_THAN_OR_EQUAL, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM -> true;
            default -> false;
        };
        if ((!arithmetic && !comparison) || call.getOperands().size() != 2
                || (arithmetic && !primitiveNumber(call.getType().getSqlTypeName()))
                || call.getOperands().stream().anyMatch(node -> !primitiveNumber(node.getType().getSqlTypeName()))) {
            return new Arguments(arguments, output);
        }
        var factory = rexBuilder.getTypeFactory();
        List<org.apache.calcite.rel.type.RelDataType> numericTypes = new ArrayList<>(
                call.getOperands().stream().map(RexNode::getType).toList());
        // Arithmetic must also honor the validated result width, including floating division.
        if (arithmetic) numericTypes.add(call.getType());
        var common = factory.leastRestrictive(numericTypes);
        if (common == null) throw invalid("Primitive numeric call has no validated common type");
        List<Expression> widened = new ArrayList<>(arguments.size());
        for (int ordinal = 0; ordinal < arguments.size(); ordinal++) {
            Expression argument = arguments.get(ordinal);
            Type target = typeMapper.map(factory.createTypeWithNullability(common,
                    call.getOperands().get(ordinal).getType().isNullable()));
            widened.add(argument.getType().equals(target) ? argument : ImmutableExpression.Cast.builder()
                    .input(argument).type(target).failureBehavior(Expression.FailureBehavior.THROW_EXCEPTION).build());
        }
        // Comparisons retain the validated BOOLEAN result, including null-safe predicates.
        return new Arguments(List.copyOf(widened), comparison ? output
                : typeMapper.map(factory.createTypeWithNullability(common, call.getType().isNullable())));
    }

    private static boolean primitiveNumber(SqlTypeName type) {
        return switch (type) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE -> true;
            default -> false;
        };
    }

    private Expression defaultArgument(FunctionNormalizedArgument definition) {
        var value = definition.getDefaultValue();
        if (!definition.getOptional() || !value.hasStringValue()
                || !value.getType().getSubstraitType().equals(definition.getType().getSkeleton())) {
            throw invalid("Normalized argument has no exact declared string default");
        }
        var type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(value.getStringValue(), type, false);
        return literalConverter.convert(literal, type);
    }

    private Expression explicitArgument(FunctionNormalizedArgument definition, RexCall call, int ordinal) {
        RexNode node = call.getOperands().get(ordinal);
        int smallestPrecision = Integer.MAX_VALUE;
        while (node instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
            if (cast.getOperands().size() != 1 || cast.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
                throw invalid(definition.getName() + " casts must preserve an exact VARCHAR literal");
            }
            int precision = cast.getType().getPrecision();
            if (precision >= 0) smallestPrecision = Math.min(smallestPrecision, precision);
            node = cast.getOperands().get(0);
        }
        if (!definition.getConstant() || !(node instanceof RexLiteral literal) || literal.isNull()) {
            throw invalid(definition.getName() + " must be a non-null constant literal");
        }
        if (literal.getTypeName() != SqlTypeName.CHAR && literal.getTypeName() != SqlTypeName.VARCHAR) {
            throw invalid(definition.getName() + " must be a character literal");
        }
        String value = literal.getValueAs(String.class);
        if (value == null || value.length() > smallestPrecision || !definition.getEnumValuesList().contains(value)) {
            throw invalid(definition.getName() + " must be one of " + definition.getEnumValuesList());
        }
        var type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        return literalConverter.convert(literal, type);
    }

    private static SubstraitConversionException invalid(String message) {
        return new SubstraitConversionException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
