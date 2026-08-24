package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Adapts a {@link GenericRecord} to Iceberg's internal type representation
 * for partition transform evaluation.
 *
 * <p>Iceberg's partition transforms (month, day, hour, etc.) expect internal
 * types: {@code Long} (epoch microseconds) for timestamps without zone,
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
        Object value = delegate.get(pos, Object.class);
        if (value == null) {
            return null;
        }
        Types.NestedField field = schema.columns().get(pos);
        Type.TypeID typeId = field.type().typeId();
        if (typeId == Type.TypeID.TIMESTAMP && value instanceof LocalDateTime ldt) {
            long epochMicros = TimeUnit.SECONDS.toMicros(ldt.toEpochSecond(ZoneOffset.UTC))
                    + TimeUnit.NANOSECONDS.toMicros(ldt.getNano());
            return javaClass.cast(epochMicros);
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
