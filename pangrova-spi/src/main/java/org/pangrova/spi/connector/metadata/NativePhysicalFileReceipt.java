package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Actual closed-object metadata, never a successful table-publication claim. */
@Experimental
public record NativePhysicalFileReceipt(
        NativePhysicalFileAllocation allocation, Metrics metrics, byte[] objectSha256,
        List<ColumnStatistics> columns) {
    private static final int MAX_COLUMN_STATISTICS = 4096;

    public NativePhysicalFileReceipt {
        Objects.requireNonNull(allocation, "allocation");
        Objects.requireNonNull(metrics, "metrics");
        objectSha256 = NativePhysicalWriteAllocation.copyBytes(objectSha256, 32, "object SHA-256");
        if (objectSha256.length != 32) throw new IllegalArgumentException("Invalid object SHA-256");
        columns = List.copyOf(columns);
        if (columns.size() > MAX_COLUMN_STATISTICS) throw new IllegalArgumentException("Too many file statistics");
    }

    @Override public byte[] objectSha256() { return objectSha256.clone(); }

    public record Metrics(long sizeBytes, long physicalRows, long schemaId, int encodingMajor,
                          int encodingMinor, long minSequence, long maxSequence) {
        public Metrics {
            if (sizeBytes <= 0 || physicalRows <= 0 || schemaId < 0 || encodingMajor < 0 || encodingMinor < 0
                    || minSequence < 0 || maxSequence < minSequence) {
                throw new IllegalArgumentException("Invalid physical file metrics");
            }
        }
    }

    /** Presence distinguishes unavailable statistics from a collected zero or NULL value. */
    public record ColumnStatistics(int fieldId, String name, OptionalLong nullCount,
                                   Optional<Comparable<?>> minimum, Optional<Comparable<?>> maximum,
                                   String collectionPolicy) {
        public ColumnStatistics {
            if (fieldId < 0) throw new IllegalArgumentException("Negative statistics field ID");
            NativePhysicalWriteAllocation.requireText(name, "statistics column");
            Objects.requireNonNull(nullCount, "nullCount");
            Objects.requireNonNull(minimum, "minimum");
            Objects.requireNonNull(maximum, "maximum");
            NativePhysicalWriteAllocation.requireText(collectionPolicy, "collectionPolicy");
            if (nullCount.isPresent() && nullCount.getAsLong() < 0) {
                throw new IllegalArgumentException("Negative null count");
            }
            if (minimum.isPresent() != maximum.isPresent()) {
                throw new IllegalArgumentException("Incomplete statistics bounds");
            }
        }
    }
}
