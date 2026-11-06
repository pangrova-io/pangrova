package org.pangrova.connector.api.metadata.table;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Snapshot interval selecting only rows appended to a source.
 *
 * <p>The range is carried by a connector table handle when a materialization
 * refresh reads a proven append-only change set. It is authoritative only when
 * produced by the coordinator from a pinned source vector; client session
 * properties can never create one.
 *
 * <p>Snapshot identifiers are opaque provider values, not ordered counters:
 * Iceberg generates random 63-bit ids, so only the provider can prove that the
 * target snapshot descends from the base snapshot. The range therefore validates
 * identity only, and the connector rejects a pair that is not an append chain.
 *
 * @param fromInclusive base snapshot the change set starts after
 * @param toInclusive target snapshot the change set ends at
 */
@Experimental
public record SnapshotRange(long fromInclusive, long toInclusive) {

    public SnapshotRange {
        if (fromInclusive <= 0) {
            throw new IllegalArgumentException("fromInclusive must be positive");
        }
        if (toInclusive <= 0) {
            throw new IllegalArgumentException("toInclusive must be positive");
        }
        if (toInclusive == fromInclusive) {
            throw new IllegalArgumentException("toInclusive must differ from fromInclusive");
        }
    }

    /**
     * Parses the canonical {@code from:to} encoding.
     *
     * @param value encoded range
     * @return parsed range
     */
    public static SnapshotRange parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        int separator = value.indexOf(':');
        if (separator <= 0 || separator != value.lastIndexOf(':') || separator == value.length() - 1) {
            throw new IllegalArgumentException("range must use the from:to encoding: " + value);
        }
        long from;
        long to;
        try {
            from = Long.parseLong(value.substring(0, separator));
            to = Long.parseLong(value.substring(separator + 1));
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("range must use numeric snapshots: " + value, invalid);
        }
        return new SnapshotRange(from, to);
    }

    /**
     * Returns the canonical {@code from:to} encoding.
     *
     * @return encoded range
     */
    public String encode() {
        return fromInclusive + ":" + toInclusive;
    }

    @Override
    public String toString() {
        return encode();
    }
}
