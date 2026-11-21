package org.pangrova.worker.execution.filter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.TypedValue;

/** Type-safe decoding and evaluation shared by Worker-side DPP consumers. */
final class TypedDynamicFilter {

    private TypedDynamicFilter() {}

    static RuntimeFilter parseSingle(DynamicFilterPayload payload) {
        final InjectRuntimeFilterRequest decoded;
        try {
            decoded = InjectRuntimeFilterRequest.parseFrom(payload.filterData());
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "Invalid typed dynamic filter: " + payload.filterId(), failure);
        }
        if (decoded.getFiltersCount() != 1) {
            throw new IllegalArgumentException(
                    "Dynamic filter envelope must contain exactly one entry: " + payload.filterId());
        }
        RuntimeFilter entry = decoded.getFilters(0);
        if (entry.getColumnName().isBlank()) {
            throw new IllegalArgumentException(
                    "Dynamic filter column must not be blank: " + payload.filterId());
        }
        if (payload.column() != null && !payload.column().isBlank()
                && !payload.column().equals(entry.getColumnName())) {
            throw new IllegalArgumentException(
                    "Dynamic filter column mismatch: envelope=" + payload.column()
                            + " proto=" + entry.getColumnName());
        }
        return entry;
    }

    static boolean rangeMatches(RuntimeFilter entry, String value) {
        if (value == null) {
            return true;
        }
        try {
            TypedValue lower = entry.getRange().getLower();
            TypedValue upper = entry.getRange().getUpper();
            return matchesTypedRange(lower, upper, value);
        } catch (NumberFormatException failure) {
            // Partition metadata that cannot be decoded using the declared type
            // must not be pruned. The malformed-value counter is emitted by the
            // caller's task-level validation path.
            return true;
        }
    }

    private static boolean matchesTypedRange(
            TypedValue lower, TypedValue upper, String value) {
        if (hasInt64Bounds(lower, upper)) {
            long parsed = Long.parseLong(value);
            return parsed >= lower.getInt64Value() && parsed <= upper.getInt64Value();
        }
        if (hasDoubleBounds(lower, upper)) {
            double parsed = Double.parseDouble(value);
            return !Double.isNaN(parsed)
                    && parsed >= lower.getDoubleValue() && parsed <= upper.getDoubleValue();
        }
        if (hasStringBounds(lower, upper)) {
            return compare(lower, value, lower.getStringValue()) >= 0
                    && compare(lower, value, upper.getStringValue()) <= 0;
        }
        throw new IllegalArgumentException("RANGE filter has no supported typed bounds");
    }

    static boolean rangeOverlaps(
            RuntimeFilter entry, String splitMin, String splitMax) {
        if (splitMin == null || splitMin.isEmpty() || splitMax == null || splitMax.isEmpty()) {
            return true;
        }
        TypedValue lower = entry.getRange().getLower();
        TypedValue upper = entry.getRange().getUpper();
        try {
            return overlapsTypedRange(lower, upper, splitMin, splitMax);
        } catch (NumberFormatException failure) {
            return true;
        }
    }

    private static boolean overlapsTypedRange(
            TypedValue lower, TypedValue upper, String splitMin, String splitMax) {
        if (hasInt64Bounds(lower, upper)) {
            return overlapsInt64(lower, upper, splitMin, splitMax);
        }
        if (hasDoubleBounds(lower, upper)) {
            return overlapsDouble(lower, upper, splitMin, splitMax);
        }
        if (hasStringBounds(lower, upper)) {
            return overlapsString(lower, upper, splitMin, splitMax);
        }
        throw new IllegalArgumentException("RANGE filter has no supported typed bounds");
    }

    private static boolean overlapsInt64(
            TypedValue lower, TypedValue upper, String splitMin, String splitMax) {
        return Long.parseLong(splitMax) >= lower.getInt64Value()
                && Long.parseLong(splitMin) <= upper.getInt64Value();
    }

    private static boolean overlapsDouble(
            TypedValue lower, TypedValue upper, String splitMin, String splitMax) {
        double min = Double.parseDouble(splitMin);
        double max = Double.parseDouble(splitMax);
        return !Double.isNaN(min) && !Double.isNaN(max)
                && max >= lower.getDoubleValue() && min <= upper.getDoubleValue();
    }

    private static boolean overlapsString(
            TypedValue lower, TypedValue upper, String splitMin, String splitMax) {
        return compare(lower, splitMax, lower.getStringValue()) >= 0
                && compare(lower, splitMin, upper.getStringValue()) <= 0;
    }

    private static boolean hasInt64Bounds(TypedValue lower, TypedValue upper) {
        return lower.hasInt64Value() && upper.hasInt64Value();
    }

    private static boolean hasDoubleBounds(TypedValue lower, TypedValue upper) {
        return lower.hasDoubleValue() && upper.hasDoubleValue();
    }

    private static boolean hasStringBounds(TypedValue lower, TypedValue upper) {
        return lower.hasStringValue() && upper.hasStringValue();
    }

    static SplitBlockBloomFilter decodeBloom(RuntimeFilter entry) {
        if (!entry.hasBloom() || !"SBBF".equals(entry.getBloom().getAlgorithm())
                || entry.getBloom().getSerializedFilter().isEmpty()) {
            throw new IllegalArgumentException("BLOOM filter has no SBBF payload");
        }
        return SplitBlockBloomFilter.readFrom(
                entry.getBloom().getSerializedFilter().toByteArray());
    }

    static byte[] encodeBloomProbe(RuntimeFilter entry, String value) {
        return switch (entry.getBloom().getValueEncoding()) {
            case VALUE_ENCODING_INT64_LE -> ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(Long.parseLong(value))
                    .array();
            case VALUE_ENCODING_DOUBLE_LE -> ByteBuffer.allocate(Double.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble(Double.parseDouble(value))
                    .array();
            case VALUE_ENCODING_DECIMAL_UNSCALED ->
                    new BigDecimal(value).unscaledValue().toByteArray();
            case VALUE_ENCODING_UTF8 -> value.getBytes(StandardCharsets.UTF_8);
            default -> throw new IllegalArgumentException(
                    "Unsupported BLOOM value encoding: "
                            + entry.getBloom().getValueEncoding());
        };
    }

    private static int compare(TypedValue bound, String left, String right) {
        if ("DECIMAL".equals(bound.getType().getCanonicalSql())) {
            return new BigDecimal(left).compareTo(new BigDecimal(right));
        }
        return left.compareTo(right);
    }
}
