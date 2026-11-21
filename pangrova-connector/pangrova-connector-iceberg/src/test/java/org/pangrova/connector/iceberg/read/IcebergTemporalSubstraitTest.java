package org.pangrova.connector.iceberg.read;

import io.substrait.proto.ExtendedExpression;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergTemporalSubstraitTest {

    private final IcebergExpressionToSubstrait converter = new IcebergExpressionToSubstrait();

    @Test
    void residualPreservesTimestampLiteralUnitsPrecisionAndTimezone() throws Exception {
        for (Type icebergType : List.of(Types.TimestampType.withoutZone(), Types.TimestampType.withZone(),
                Types.TimestampNanoType.withoutZone(), Types.TimestampNanoType.withZone())) {
            boolean nanos = icebergType.typeId() == Type.TypeID.TIMESTAMP_NANO;
            boolean timezone = nanos ? ((Types.TimestampNanoType) icebergType).shouldAdjustToUTC()
                    : ((Types.TimestampType) icebergType).shouldAdjustToUTC();
            int precision = nanos ? 9 : 6;
            Schema schema = new Schema(Types.NestedField.optional(1, "event_time", icebergType));
            for (long value : new long[] {-1, 1, 1735689600123456789L, Long.MIN_VALUE, Long.MAX_VALUE}) {
                // Iceberg treats an untyped Long timestamp literal as microseconds,
                // even when binding to a nanosecond column. Declare the units explicitly.
                var timestampInput = nanos ? Expressions.nanos(value) : Expressions.micros(value);
                var bound = Binder.bind(schema.asStruct(),
                        Expressions.predicate(Operation.EQ, "event_time", timestampInput), true);
                var bytes = converter.convert(bound, schema).orElseThrow();
                var expression = ExtendedExpression.parseFrom(bytes);
                var field = expression.getBaseSchema().getStruct().getTypes(0);
                var literal = expression.getReferredExpr(0).getExpression().getScalarFunction()
                        .getArguments(1).getValue().getLiteral();
                assertThat(literal.hasI64()).isFalse();
                assertThat(field.hasPrecisionTimestampTz()).isEqualTo(timezone);
                assertThat(literal.hasPrecisionTimestampTz()).isEqualTo(timezone);
                var timestamp = timezone ? literal.getPrecisionTimestampTz() : literal.getPrecisionTimestamp();
                assertThat(timestamp.getPrecision()).isEqualTo(precision);
                assertThat(timestamp.getValue()).isEqualTo(value);
                assertThat(timezone ? field.getPrecisionTimestampTz().getPrecision()
                        : field.getPrecisionTimestamp().getPrecision()).isEqualTo(precision);
            }
        }
    }

    @Test
    void untypedLongIsBoundAsMicrosExactlyOnceForNanoResidual() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "event_time", Types.TimestampNanoType.withoutZone()));
        var bound = Binder.bind(schema.asStruct(), Expressions.equal("event_time", -1L), true);
        var expression = ExtendedExpression.parseFrom(converter.convert(bound, schema).orElseThrow());
        var timestamp = expression.getReferredExpr(0).getExpression().getScalarFunction()
                .getArguments(1).getValue().getLiteral().getPrecisionTimestamp();
        assertThat(timestamp.getPrecision()).isEqualTo(9);
        assertThat(timestamp.getValue()).isEqualTo(-1000L);
    }

    @Test
    void unrelatedNanoColumnDoesNotDisableResidualOnAnotherColumn() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "event_time", Types.TimestampNanoType.withoutZone()),
                Types.NestedField.optional(2, "id", Types.IntegerType.get()));
        var bound = Binder.bind(schema.asStruct(), Expressions.equal("id", 7), true);
        var expression = ExtendedExpression.parseFrom(
                converter.convert(bound, schema).orElseThrow());
        assertThat(expression.getBaseSchema().getStruct().getTypes(0).getPrecisionTimestamp().getPrecision())
                .isEqualTo(9);
    }
}
