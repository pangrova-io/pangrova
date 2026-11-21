package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.math.BigDecimal;

/**
 * Adapts a {@link GenericRecord} to Iceberg's internal type representation
 * for partition transform evaluation.
 *
 * <p>Iceberg's partition transforms (month, day, hour, etc.) expect internal
 * types: {@code Long} (epoch microseconds or nanoseconds) for timestamps,
 * {@code Integer} (epoch days) for dates. {@link GenericRecord} stores
 * user-facing types ({@code LocalDateTime}, {@code LocalDate}). This adapter
 * bridges the gap without requiring the {@code iceberg-data} module's
 * {@code InternalRecordWrapper}.</p>
 */
final class PartitionStructProjection implements StructLike {

    private static final LocalDate EPOCH_DATE = LocalDate.ofEpochDay(0);

    private final Schema schema;
    private GenericRecord delegate;

    PartitionStructProjection(Schema schema) {
        this.schema = schema;
    }

    void wrap(GenericRecord record) {
        this.delegate = record;
    }

    @Override
    public int size() {
        return schema.columns().size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        Types.NestedField field = schema.columns().get(pos);
        if (field.type().typeId() == Type.TypeID.UNKNOWN) {
            return null;
        }
        Object value = delegate.getField(field.name());
        if (value == null) {
            return null;
        }
        Type.TypeID typeId = field.type().typeId();
        if (typeId == Type.TypeID.TIMESTAMP || typeId == Type.TypeID.TIMESTAMP_NANO) {
            Instant instant = value instanceof LocalDateTime ldt ? ldt.toInstant(ZoneOffset.UTC)
                    : value instanceof OffsetDateTime odt ? odt.toInstant() : null;
            if (instant != null) {
                int precision = typeId == Type.TypeID.TIMESTAMP_NANO ? 9 : 6;
                long fraction = precision == 9 ? instant.getNano() : instant.getNano() / 1000;
                long units = BigDecimal.valueOf(instant.getEpochSecond()).scaleByPowerOfTen(precision)
                        .add(BigDecimal.valueOf(fraction)).longValueExact();
                return javaClass.cast(units);
            }
        }
        if (typeId == Type.TypeID.DATE && value instanceof LocalDate ld) {
            int epochDays = (int) ChronoUnit.DAYS.between(EPOCH_DATE, ld);
            return javaClass.cast(epochDays);
        }
        return javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException("PartitionStructProjection is read-only");
    }
}
