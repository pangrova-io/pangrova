package org.pangrova.coordinator.dynamictable.detect;

import java.util.List;
import java.util.Objects;

import org.pangrova.spi.streaming.CdcEnvelope;

/**
 * Result of a change detection pass on a source table.
 *
 * <p>Sealed hierarchy — exactly four variants:</p>
 * <ul>
 *   <li>{@link Empty} — no changes detected since last checkpoint</li>
 *   <li>{@link Incremental} — append-only data files to process</li>
 *   <li>{@link CdcBatch} — a batch of CDC change events</li>
 *   <li>{@link FullRefreshRequired} — delete files present, must fall back to full refresh</li>
 * </ul>
 */
public sealed interface ChangeResult
        permits ChangeResult.Empty,
                ChangeResult.Incremental,
                ChangeResult.CdcBatch,
                ChangeResult.FullRefreshRequired {

    /** Whether this result represents no changes. */
    default boolean isEmpty() {
        return this instanceof Empty;
    }

    /**
     * Extract the source checkpoint from this change result.
     *
     * <p>For snapshot-based results, returns the {@code toSnapshot} ID.
     * For CDC-based results, returns the last event's timestamp.
     * For empty results, returns {@code null}.</p>
     *
     * @return opaque checkpoint string, or null if no checkpoint available
     */
    default String extractCheckpoint() {
        return null;
    }

    // ── Static factories ────────────────────────────────────────────

    /** No changes detected. */
    static ChangeResult empty() {
        return Empty.INSTANCE;
    }

    /** Append-only incremental data files. */
    static ChangeResult incremental(IncrementalScanPlan plan) {
        return new Incremental(plan);
    }

    /** A batch of CDC change events. */
    static ChangeResult cdcBatch(List<CdcEnvelope> events) {
        return new CdcBatch(events);
    }

    /** Delete files detected — full refresh required. */
    static ChangeResult fullRefreshRequired(IncrementalScanPlan plan) {
        return new FullRefreshRequired(plan);
    }

    // ── Variant records ─────────────────────────────────────────────

    /** Singleton: no changes since last checkpoint. */
    record Empty() implements ChangeResult {
        static final Empty INSTANCE = new Empty();
    }

    /** Append-only incremental scan plan. */
    record Incremental(IncrementalScanPlan plan) implements ChangeResult {
        public Incremental {
            Objects.requireNonNull(plan, "plan must not be null");
        }

        @Override
        public String extractCheckpoint() {
            return String.valueOf(plan.toSnapshot());
        }
    }

    /** Batch of CDC events to process. */
    record CdcBatch(List<CdcEnvelope> events) implements ChangeResult {
        public CdcBatch {
            Objects.requireNonNull(events, "events must not be null");
            events = List.copyOf(events);
        }

        @Override
        public boolean isEmpty() {
            return events.isEmpty();
        }

        @Override
        public String extractCheckpoint() {
            if (events.isEmpty()) {
                return null;
            }
            return events.get(events.size() - 1).eventTime().toString();
        }
    }

    /** Delete files present — must fall back to full refresh. */
    record FullRefreshRequired(IncrementalScanPlan plan) implements ChangeResult {
        public FullRefreshRequired {
            Objects.requireNonNull(plan, "plan must not be null");
        }

        @Override
        public String extractCheckpoint() {
            return String.valueOf(plan.toSnapshot());
        }
    }
}
