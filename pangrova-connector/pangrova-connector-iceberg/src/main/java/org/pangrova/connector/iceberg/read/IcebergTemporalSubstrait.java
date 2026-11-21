package org.pangrova.connector.iceberg.read;

import io.substrait.proto.Expression.Literal;
import io.substrait.proto.Type;

/** Preserves timestamp units and timezone semantics in Iceberg residual predicates. */
final class IcebergTemporalSubstrait {

    private IcebergTemporalSubstrait() {}

    static Type timestampType(boolean withTimezone, int precision) {
        if (withTimezone) {
            return Type.newBuilder().setPrecisionTimestampTz(Type.PrecisionTimestampTZ.newBuilder()
                    .setPrecision(precision).setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        }
        return Type.newBuilder().setPrecisionTimestamp(Type.PrecisionTimestamp.newBuilder()
                .setPrecision(precision).setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
    }

    static Literal literal(Object value, Type type) {
        if (!(value instanceof Long timestamp)) {
            return null;
        }
        if (type.hasPrecisionTimestamp()) {
            return Literal.newBuilder().setPrecisionTimestamp(Literal.PrecisionTimestamp.newBuilder()
                    .setValue(timestamp).setPrecision(type.getPrecisionTimestamp().getPrecision())).build();
        }
        if (type.hasPrecisionTimestampTz()) {
            return Literal.newBuilder().setPrecisionTimestampTz(Literal.PrecisionTimestamp.newBuilder()
                    .setValue(timestamp).setPrecision(type.getPrecisionTimestampTz().getPrecision())).build();
        }
        return null;
    }
}
