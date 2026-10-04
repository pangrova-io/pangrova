package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
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

/** Materializes declared optional constants before hidden runtime binding arguments. */
final class FunctionArgumentNormalizer {
    private final RexBuilder rexBuilder;
    private final LiteralConverter literalConverter;

    FunctionArgumentNormalizer(RexBuilder rexBuilder, LiteralConverter literalConverter) {
        this.rexBuilder = rexBuilder;
        this.literalConverter = literalConverter;
    }

    List<Expression> normalize(FunctionReference reference, RexCall call, List<Expression> arguments) {
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
        return List.copyOf(normalized);
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
