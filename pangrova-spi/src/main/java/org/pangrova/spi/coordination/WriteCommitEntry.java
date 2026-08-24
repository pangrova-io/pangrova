package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;

import java.util.List;
import java.util.Objects;

/**
 * Represents a write commit intent recorded in the {@link WriteCommitJournal}.
 *
 * <p>Each entry tracks a single distributed write operation from intent through
 * completion. The lifecycle is:
 * <ol>
 *   <li>{@code PENDING} — recorded before {@code finishDistributedWrite()} is called</li>
 *   <li>{@code COMMITTED} — updated after the commit succeeds</li>
 * </ol>
 *
 * <p>On coordinator crash recovery, entries in {@code COMMITTED} state indicate
 * writes that already succeeded and must not be retried, providing idempotency.</p>
 *
 * @param queryId        the query identifier (unique per write operation)
 * @param catalogName    the target catalog name
 * @param tableName      the fully qualified target table name
 * @param state          the commit state (PENDING or COMMITTED)
 * @param timestampMs    epoch milliseconds when the entry was created or updated
 * @param fragments      serialized worker commit fragments needed to replay the commit
 * @param writeMode      the distributed write mode (e.g. INSERT/OVERWRITE/DELETE)
 * @param idempotencyKey stable key that dedups replayed commits (defaults to queryId)
 * @param coordinatorId  owning coordinator id for owner-scoped recovery (nullable)
 * @param term           leadership/fencing term for the owning coordinator (0 if none)
 */
@Stable(since = "1.0")
public record WriteCommitEntry(
        String queryId,
        String catalogName,
        String tableName,
        State state,
        long timestampMs,
        List<byte[]> fragments,
        String writeMode,
        String idempotencyKey,
        String coordinatorId,
        long term) {

    /**
     * Commit lifecycle states.
     */
    @Stable(since = "1.0")
    public enum State {
        /** Intent recorded; commit not yet executed. */
        PENDING,
        /** Commit succeeded; safe to skip on retry. */
        COMMITTED
    }

    public WriteCommitEntry {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(catalogName, "catalogName must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(state, "state must not be null");
        if (timestampMs <= 0) {
            throw new IllegalArgumentException("timestampMs must be positive");
        }
        // A-COMMIT-01: the entry is a REPLAYABLE redo record, not just a state marker.
        // Normalize the replay payload so a recovering coordinator can reconstruct the commit.
        fragments = fragments == null ? List.of() : List.copyOf(fragments);
        writeMode = writeMode == null ? "UNKNOWN" : writeMode;
        idempotencyKey = idempotencyKey == null ? queryId : idempotencyKey;
        // coordinatorId may be null (single-node); term may be 0 (no fencing context).
    }

    /**
     * Backward-compatible convenience constructor for identity + state only
     * (no replay payload). Prefer the canonical constructor for durable intents
     * that must be replayable after a crash.
     */
    public WriteCommitEntry(String queryId, String catalogName, String tableName,
                            State state, long timestampMs) {
        this(queryId, catalogName, tableName, state, timestampMs,
                List.of(), "UNKNOWN", queryId, null, 0L);
    }

    /** Whether this entry carries a replayable commit payload (worker fragments). */
    public boolean isReplayable() {
        return !fragments.isEmpty();
    }
}
