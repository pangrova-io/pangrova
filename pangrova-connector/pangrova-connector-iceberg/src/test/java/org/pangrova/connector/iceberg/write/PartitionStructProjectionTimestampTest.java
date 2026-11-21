package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionStructProjectionTimestampTest {

    @Test
    void nanoProjectionPreservesSignedInt64BoundaryAndSubsecond() {
        for (long units : new long[] {Long.MIN_VALUE, -1, 0, 1, 1735689600123456789L, Long.MAX_VALUE}) {
            Instant instant = Instant.ofEpochSecond(Math.floorDiv(units, 1_000_000_000L),
                    Math.floorMod(units, 1_000_000_000L));
            assertThat(project(Types.TimestampNanoType.withoutZone(),
                    LocalDateTime.ofInstant(instant, ZoneOffset.UTC)).get(0, Long.class)).isEqualTo(units);
            assertThat(project(Types.TimestampNanoType.withZone(),
                    instant.atOffset(ZoneOffset.ofHours(8))).get(0, Long.class)).isEqualTo(units);
        }
    }

    @Test
    void microProjectionPreservesTimezoneAndNegativeEpoch() {
        Instant instant = Instant.parse("1969-12-31T23:59:59.999999Z");
        assertThat(project(Types.TimestampType.withoutZone(),
                LocalDateTime.ofInstant(instant, ZoneOffset.UTC)).get(0, Long.class)).isEqualTo(-1);
        assertThat(project(Types.TimestampType.withZone(),
                instant.atOffset(ZoneOffset.ofHours(-7))).get(0, Long.class)).isEqualTo(-1);
    }

    @Test
    void dayPartitionTransformUsesNanosecondUnitsAcrossEpoch() {
        Schema schema = schema(Types.TimestampNanoType.withoutZone());
        PartitionKey key = new PartitionKey(PartitionSpec.builderFor(schema).day("event_time").build(), schema);
        key.partition(project(Types.TimestampNanoType.withoutZone(),
                LocalDateTime.parse("1969-12-31T23:59:59.999999999")));
        assertThat(key.get(0, Integer.class)).isEqualTo(-1);
        key.partition(project(Types.TimestampNanoType.withoutZone(),
                LocalDateTime.parse("1970-01-01T00:00:00.000000001")));
        assertThat(key.get(0, Integer.class)).isZero();
    }

    @Test
    void nanoProjectionRejectsOverflowInsteadOfSaturating() {
        var projection = project(Types.TimestampNanoType.withoutZone(), LocalDateTime.parse("2300-01-01T00:00:00"));
        assertThatThrownBy(() -> projection.get(0, Long.class)).isInstanceOf(ArithmeticException.class);
    }

    private static Schema schema(Type type) {
        return new Schema(Types.NestedField.optional(1, "event_time", type));
    }

    private static PartitionStructProjection project(Type type, Object value) {
        Schema schema = schema(type);
        GenericRecord record = GenericRecord.create(schema);
        record.setField("event_time", value);
        var projection = new PartitionStructProjection(schema);
        projection.wrap(record);
        return projection;
    }
}
