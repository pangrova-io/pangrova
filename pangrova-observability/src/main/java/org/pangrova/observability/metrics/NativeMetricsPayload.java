package org.pangrova.observability.metrics;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Versioned native metrics payload contract.
 *
 * <h3>Wire format</h3>
 * <pre>
 *   [0..3]  int32 LE  schemaVersion
 *   [4..N]  fields    version-specific field data (each field 8 bytes LE)
 * </pre>
 *
 * <h3>Version history</h3>
 * <ul>
 *   <li>Version 0 (legacy): 32 bytes, no header — 4 fields at fixed offsets 0/8/16/24</li>
 *   <li>Version 1: 4-byte header + 4 fields (memoryUsedBytes, operatorRowsProcessed,
 *       ioBytesRead, cacheHitRatio) = 36 bytes total</li>
 * </ul>
 *
 * <p>Forward compatibility: unknown versions decode only the fields known to this
 * code version. Extra trailing bytes are ignored. If the payload is too short for
 * even the known fields, missing fields default to zero.</p>
 */
public final class NativeMetricsPayload {

    /** Header size in bytes (schemaVersion int32). */
    public static final int HEADER_SIZE = 4;

    /** Bytes per field (all fields are 8-byte little-endian). */
    public static final int FIELD_SIZE = 8;

    /** Legacy format: no version header, exactly 32 bytes. */
    public static final int LEGACY_VERSION = 0;

    /** First versioned format: header + 4 fields. */
    public static final int VERSION_1 = 1;

    /** Legacy payload size (version 0, no header). */
    private static final int LEGACY_PAYLOAD_SIZE = 32;

    private static final FieldDefinition[] V1_FIELDS = {
            new FieldDefinition("memoryUsedBytes", FieldType.LONG),
            new FieldDefinition("operatorRowsProcessed", FieldType.LONG),
            new FieldDefinition("ioBytesRead", FieldType.LONG),
            new FieldDefinition("cacheHitRatio", FieldType.DOUBLE)
    };

    private final int schemaVersion;
    private final Map<String, Number> fields;

    private NativeMetricsPayload(int schemaVersion, Map<String, Number> fields) {
        this.schemaVersion = schemaVersion;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /**
     * Decodes a raw byte array into a versioned metrics payload.
     *
     * <p>Detection logic:</p>
     * <ul>
     *   <li>null or empty → empty payload (version 0, all zeros)</li>
     *   <li>Exactly 32 bytes → legacy version 0 (no header)</li>
     *   <li>Otherwise → read version from first 4 bytes, decode fields after header</li>
     * </ul>
     *
     * @param raw the raw byte array from native side
     * @return decoded payload with version and field values
     */
    public static NativeMetricsPayload decode(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return empty();
        }

        if (raw.length == LEGACY_PAYLOAD_SIZE) {
            return decodeLegacy(raw);
        }

        if (raw.length < HEADER_SIZE) {
            return empty();
        }

        ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        int version = buf.getInt();

        return decodeVersioned(version, raw);
    }

    /**
     * Encodes a version-1 payload from the given metric values.
     *
     * @param memoryUsedBytes        native memory usage
     * @param operatorRowsProcessed  rows processed by operators
     * @param ioBytesRead            I/O bytes read
     * @param cacheHitRatio          cache hit ratio
     * @return encoded byte array with version header
     */
    public static byte[] encodeV1(long memoryUsedBytes, long operatorRowsProcessed,
                                  long ioBytesRead, double cacheHitRatio) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + V1_FIELDS.length * FIELD_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(VERSION_1);
        buf.putLong(memoryUsedBytes);
        buf.putLong(operatorRowsProcessed);
        buf.putLong(ioBytesRead);
        buf.putLong(Double.doubleToLongBits(cacheHitRatio));
        return buf.array();
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public Map<String, Number> fields() {
        return fields;
    }

    public long getLong(String fieldName) {
        Number value = fields.get(fieldName);
        return value != null ? value.longValue() : 0L;
    }

    public double getDouble(String fieldName) {
        Number value = fields.get(fieldName);
        return value != null ? value.doubleValue() : 0.0;
    }

    private static NativeMetricsPayload empty() {
        Map<String, Number> zeroFields = new LinkedHashMap<>();
        for (FieldDefinition fd : V1_FIELDS) {
            zeroFields.put(fd.name(), fd.type() == FieldType.DOUBLE ? 0.0 : 0L);
        }
        return new NativeMetricsPayload(LEGACY_VERSION, zeroFields);
    }

    private static NativeMetricsPayload decodeLegacy(byte[] raw) {
        return decodeFields(LEGACY_VERSION, raw, 0, V1_FIELDS);
    }

    private static NativeMetricsPayload decodeVersioned(int version, byte[] raw) {
        // For any version (known or unknown), decode the V1 fields that fit.
        // Unknown versions gracefully degrade by reading only known fields.
        return decodeFields(version, raw, HEADER_SIZE, V1_FIELDS);
    }

    private static NativeMetricsPayload decodeFields(int version, byte[] raw,
                                                     int dataOffset,
                                                     FieldDefinition[] fieldDefs) {
        Map<String, Number> result = new LinkedHashMap<>();
        ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < fieldDefs.length; i++) {
            int fieldOffset = dataOffset + i * FIELD_SIZE;
            FieldDefinition fd = fieldDefs[i];

            if (fieldOffset + FIELD_SIZE > raw.length) {
                // Payload too short for this field — default to zero
                result.put(fd.name(), fd.type() == FieldType.DOUBLE ? 0.0 : 0L);
            } else {
                long bits = buf.getLong(fieldOffset);
                if (fd.type() == FieldType.DOUBLE) {
                    result.put(fd.name(), Double.longBitsToDouble(bits));
                } else {
                    result.put(fd.name(), bits);
                }
            }
        }

        return new NativeMetricsPayload(version, result);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NativeMetricsPayload that)) return false;
        return schemaVersion == that.schemaVersion && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, fields);
    }

    @Override
    public String toString() {
        return "NativeMetricsPayload{version=" + schemaVersion + ", fields=" + fields + '}';
    }

    /** Field type in the binary payload. */
    public enum FieldType {
        LONG, DOUBLE
    }

    /** Immutable field definition: name + type. */
    public record FieldDefinition(String name, FieldType type) {
        public FieldDefinition {
            Objects.requireNonNull(name, "Field name must not be null");
            Objects.requireNonNull(type, "Field type must not be null");
        }
    }
}
