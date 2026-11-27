package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** The remaining stage capacity and monotonic deadline available to one split source. */
@Experimental
public record SplitPlanningBudget(long deadlineNanos, int remainingSplits, FileGranularity fileGranularity,
                                  EncodedMetadataBudget encodedMetadata) {
    /** Whether the consumer can execute byte ranges independently within one data object. */
    public enum FileGranularity { WHOLE_OBJECT, BYTE_RANGES }

    public static final long DEFAULT_TIMEOUT_MS = 30_000;
    public static final int DEFAULT_MAX_SPLITS = 100_000;
    public static final long DEFAULT_MAX_ENCODED_METADATA_BYTES = 256L * 1024 * 1024;

    public SplitPlanningBudget(long deadlineNanos, int remainingSplits) {
        this(deadlineNanos, remainingSplits, FileGranularity.WHOLE_OBJECT);
    }

    public SplitPlanningBudget(long deadlineNanos, int remainingSplits, FileGranularity fileGranularity) {
        this(deadlineNanos, remainingSplits, fileGranularity,
                new EncodedMetadataBudget(DEFAULT_MAX_ENCODED_METADATA_BYTES));
    }

    public SplitPlanningBudget {
        java.util.Objects.requireNonNull(fileGranularity, "fileGranularity");
        java.util.Objects.requireNonNull(encodedMetadata, "encodedMetadata");
        if (remainingSplits < 0) throw new IllegalArgumentException("Split capacity must not be negative");
    }

    /** Gives a source its remaining split capacity while retaining the stage's deadline and byte grant. */
    public SplitPlanningBudget forSource(int capacity, FileGranularity granularity) {
        if (capacity > remainingSplits) throw new IllegalArgumentException("Source capacity exceeds stage capacity");
        return new SplitPlanningBudget(deadlineNanos, capacity, granularity, encodedMetadata);
    }

    /** Shared by all metadata readers in a stage. Outstanding reads keep their reservations. */
    public static final class EncodedMetadataBudget {
        private final long limit;
        private final AtomicLong remaining;

        public EncodedMetadataBudget(long limit) {
            if (limit < 0) throw new IllegalArgumentException("Metadata byte grant must not be negative");
            this.limit = limit;
            this.remaining = new AtomicLong(limit);
        }

        public long remainingBytes() {
            return remaining.get();
        }

        /** Grants at most the requested bytes. A zero result means no credit is available. */
        public int reserve(int requested) {
            if (requested < 0) throw new IllegalArgumentException("Requested metadata bytes must not be negative");
            long available;
            int granted;
            do {
                available = remaining.get();
                granted = (int) Math.min(available, requested);
                if (granted == 0) return 0;
            } while (!remaining.compareAndSet(available, available - granted));
            return granted;
        }

        /** Returns the unused part of a reservation after its read exits. */
        public void refund(int unused) {
            if (unused < 0) throw new IllegalArgumentException("Metadata refund must not be negative");
            if (unused == 0) return;
            long available;
            do {
                available = remaining.get();
                if (unused > limit - available) {
                    throw new IllegalStateException("Metadata refund exceeds the outstanding grant");
                }
            } while (!remaining.compareAndSet(available, available + unused));
        }
    }

    /** Starts a new standalone enumeration with the standard stage limits. */
    public static SplitPlanningBudget standard() {
        return new SplitPlanningBudget(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS),
                DEFAULT_MAX_SPLITS);
    }

    /** Returns whether the monotonic deadline has elapsed. */
    public boolean expired() {
        return System.nanoTime() - deadlineNanos >= 0;
    }

    /** Validates a complete or partial count before the source allocates or publishes splits. */
    public void check(long splitCount) {
        if (expired()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT,
                    "Split enumeration timed out with " + splitCount + " splits planned by the source");
        }
        if (splitCount > remainingSplits) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                    "Split source needs " + splitCount + " splits with " + remainingSplits + " stage slots left");
        }
    }
}
