package org.pangrova.connector.iceberg;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.TreeMap;

/** Exact SQL static scope over identity partition source columns, validated before dispatch. */
final class IcebergStaticOverwriteScope {
    private IcebergStaticOverwriteScope() {}

    static Expression filter(Schema schema, PartitionSpec spec, Map<String, String> partitions) {
        Expression result = Expressions.alwaysTrue();
        for (var entry : new TreeMap<>(partitions).entrySet()) {
            var field = schema.findField(entry.getKey());
            if (field == null || spec.fields().stream().noneMatch(partition ->
                    partition.sourceId() == field.fieldId() && partition.transform().isIdentity())) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                        "Iceberg static overwrite requires an identity partition column: " + entry.getKey());
            }
            Object value = value(field.type(), entry.getValue());
            result = Expressions.and(result, Expressions.equal(entry.getKey(), value));
        }
        return result;
    }

    private static Object value(Type type, String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Iceberg static partition value is missing");
        }
        return switch (type.typeId()) {
            case BOOLEAN -> booleanValue(raw);
            case INTEGER -> Integer.valueOf(raw);
            case LONG -> Long.valueOf(raw);
            case FLOAT -> finiteFloat(raw);
            case DOUBLE -> finiteDouble(raw);
            case DECIMAL -> decimalValue(type, raw);
            case STRING, DATE, TIME, TIMESTAMP, TIMESTAMP_NANO, UUID -> literalValue(type, raw);
            default -> throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Iceberg static partition type is unsupported: " + type);
        };
    }

    private static boolean booleanValue(String raw) {
        if (!raw.equals("true") && !raw.equals("false")) {
            throw new IllegalArgumentException("Invalid Iceberg boolean partition value: " + raw);
        }
        return Boolean.parseBoolean(raw);
    }

    private static float finiteFloat(String raw) {
        float value = Float.parseFloat(raw);
        if (!Float.isFinite(value)) {
            throw new IllegalArgumentException("Non-finite Iceberg partition value");
        }
        return value;
    }

    private static double finiteDouble(String raw) {
        double value = Double.parseDouble(raw);
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("Non-finite Iceberg partition value");
        }
        return value;
    }

    private static Object decimalValue(Type type, String raw) {
        var decimal = (org.apache.iceberg.types.Types.DecimalType) type;
        BigDecimal value = new BigDecimal(raw).setScale(decimal.scale(), RoundingMode.UNNECESSARY);
        if (value.precision() > decimal.precision()) {
            throw new IllegalArgumentException("Iceberg decimal partition exceeds precision");
        }
        return value;
    }

    private static Object literalValue(Type type, String raw) {
        Literal<?> value = Literal.of(raw).to(type);
        if (value == null) {
            throw new IllegalArgumentException("Invalid Iceberg partition value for " + type);
        }
        return value.value();
    }
}
