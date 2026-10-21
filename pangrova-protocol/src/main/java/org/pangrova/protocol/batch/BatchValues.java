package org.pangrova.protocol.batch;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Field-aware conversion at the Arrow/connector boundary, including nested nulls. */
final class BatchValues {
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;
    private static final long NANOS_PER_MICROSECOND = 1_000L;

    private BatchValues() {}

    static Object detach(Field field, Object value) {
        if (uuidDeclaration(field)) return detachUuid(value);
        if (value == null) {
            return null;
        }
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Map) {
            return detachMap(field, (List<?>) value);
        }
        if (type instanceof ArrowType.Struct) {
            return detachStruct(field, (Map<?, ?>) value);
        }
        if (value instanceof List<?> list) {
            return detachList(field, list);
        }
        if (value instanceof Text text) {
            return text.toString();
        }
        if (value instanceof byte[] bytes) {
            return bytes.clone();
        }
        if (value instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
        return detachTemporal(type, value);
    }

    private static boolean uuidDeclaration(Field field) {
        if (field.getType() instanceof UuidType) return true;
        if (!UuidType.EXTENSION_NAME.equals(field.getMetadata().get("ARROW:extension:name"))) return false;
        UuidType.INSTANCE.deserialize(field.getType(),
                field.getMetadata().getOrDefault("ARROW:extension:metadata", ""));
        return true;
    }

    private static Object detachUuid(Object value) {
        if (value == null || value instanceof UUID) return value;
        ByteBuffer bytes;
        if (value instanceof byte[] binary) {
            bytes = ByteBuffer.wrap(binary);
        } else if (value instanceof ByteBuffer buffer) {
            bytes = buffer.duplicate();
        } else {
            throw new IllegalArgumentException("UUID requires a canonical 16-byte value");
        }
        if (bytes.remaining() != UuidType.UUID_BYTE_WIDTH) {
            throw new IllegalArgumentException("UUID requires exactly 16 binary bytes");
        }
        bytes.order(ByteOrder.BIG_ENDIAN);
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    private static List<Object> detachList(Field field, List<?> values) {
        List<Object> result = new ArrayList<>(values.size());
        for (Object element : values) {
            result.add(detach(field.getChildren().get(0), element));
        }
        return result;
    }

    private static Map<Object, Object> detachMap(Field field, List<?> entries) {
        List<Field> children = field.getChildren().get(0).getChildren();
        Map<Object, Object> result = new LinkedHashMap<>(entries.size());
        for (Object entry : entries) {
            Map<?, ?> pair = (Map<?, ?>) entry;
            Object key = detach(children.get(0), pair.get(children.get(0).getName()));
            Object value = detach(children.get(1), pair.get(children.get(1).getName()));
            if (key == null || result.containsKey(key)) {
                throw new IllegalArgumentException("Map keys must be non-null and unique");
            }
            result.put(key, value);
        }
        return result;
    }

    private static Map<String, Object> detachStruct(Field field, Map<?, ?> values) {
        Map<String, Object> result = new LinkedHashMap<>(field.getChildren().size());
        for (Field child : field.getChildren()) {
            result.put(child.getName(), detach(child, values.get(child.getName())));
        }
        return result;
    }

    private static Object detachTemporal(ArrowType type, Object value) {
        if (type instanceof ArrowType.Date date) {
            return detachDate(date, value);
        }
        if (type instanceof ArrowType.Time time && value instanceof Number number) {
            return LocalTime.ofNanoOfDay(Math.multiplyExact(number.longValue(), nanosPerUnit(time.getUnit())));
        }
        if (type instanceof ArrowType.Timestamp timestamp && value instanceof Number number) {
            long unitsPerSecond = NANOS_PER_SECOND / nanosPerUnit(timestamp.getUnit());
            long seconds = Math.floorDiv(number.longValue(), unitsPerSecond);
            long nanos = Math.floorMod(number.longValue(), unitsPerSecond) * nanosPerUnit(timestamp.getUnit());
            return LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
        }
        return value;
    }

    private static Object detachDate(ArrowType.Date date, Object value) {
        if (value instanceof LocalDateTime time) {
            return time.toLocalDate();
        }
        if (value instanceof Number number) {
            return date.getUnit() == org.apache.arrow.vector.types.DateUnit.DAY
                    ? LocalDate.ofEpochDay(number.longValue())
                    : Instant.ofEpochMilli(number.longValue())
                            .atOffset(ZoneOffset.UTC).toLocalDate();
        }
        return value;
    }

    private static long nanosPerUnit(org.apache.arrow.vector.types.TimeUnit unit) {
        return switch (unit) {
            case SECOND -> NANOS_PER_SECOND;
            case MILLISECOND -> NANOS_PER_MILLISECOND;
            case MICROSECOND -> NANOS_PER_MICROSECOND;
            case NANOSECOND -> 1L;
        };
    }
}
