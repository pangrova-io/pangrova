package org.pangrova.coordinator.query.execution;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write-ahead log for multi-phase MERGE operations.
 *
 * <p>Records the intent of each MERGE phase before execution begins, so that
 * on failure the system has a complete record of what was attempted, what
 * committed, and what remains uncommitted. This enables manual or automated
 * recovery of partially-committed MERGE operations.</p>
 *
 * <p>Lifecycle: {@link #begin} before the first phase, {@link #recordPhaseStarted}
 * before each phase executes, {@link #recordPhaseCommitted} after each phase
 * commits, and {@link #complete} or {@link #recordFailure} at the end.</p>
 *
 * <p>Concurrency: thread-safe via ConcurrentHashMap for operation lookup and
 * CopyOnWriteArrayList for phase tracking within each operation.</p>
 */
@ThreadSafe
public class MergeWriteAheadLog {

    private static final Logger LOG = LoggerFactory.getLogger(MergeWriteAheadLog.class);

    /** Represents the state of a single MERGE phase. */
    public enum PhaseState {
        PENDING, EXECUTING, COMMITTED, FAILED
    }

    /** Immutable snapshot of a single MERGE phase. */
    public record PhaseRecord(
            String phaseQueryId,
            String phaseName,
            String sql,
            PhaseState state,
            Instant startedAt,
            Instant completedAt) {

        PhaseRecord withState(PhaseState newState) {
            Instant completed = (newState == PhaseState.COMMITTED || newState == PhaseState.FAILED)
                    ? Instant.now() : completedAt;
            return new PhaseRecord(phaseQueryId, phaseName, sql, newState, startedAt, completed);
        }
    }

    /**
     * Mutable tracking state for a multi-phase MERGE operation.
     * Phase list uses CopyOnWriteArrayList for thread-safe iteration
     * during failure reporting while allowing concurrent phase additions.
     */
    static final class MergeOperation {
        private final String parentQueryId;
        private final String targetTable;
        private final Instant startedAt;
        private final CopyOnWriteArrayList<PhaseRecord> phases;

        MergeOperation(String parentQueryId, String targetTable) {
            this.parentQueryId = parentQueryId;
            this.targetTable = targetTable;
            this.startedAt = Instant.now();
            this.phases = new CopyOnWriteArrayList<>();
        }

        String parentQueryId() { return parentQueryId; }
        String targetTable() { return targetTable; }
        Instant startedAt() { return startedAt; }

        void addPhase(PhaseRecord phase) { phases.add(phase); }

        /** Returns an immutable snapshot of the current phases. */
        List<PhaseRecord> snapshotPhases() { return List.copyOf(phases); }

        int phaseCount() { return phases.size(); }

        void updatePhase(String phaseQueryId, PhaseState newState) {
            for (int i = 0; i < phases.size(); i++) {
                PhaseRecord current = phases.get(i);
                if (current.phaseQueryId().equals(phaseQueryId)) {
                    phases.set(i, current.withState(newState));
                    return;
                }
            }
        }

        void failRemainingPhases() {
            for (int i = 0; i < phases.size(); i++) {
                PhaseRecord phase = phases.get(i);
                if (phase.state() == PhaseState.EXECUTING || phase.state() == PhaseState.PENDING) {
                    phases.set(i, phase.withState(PhaseState.FAILED));
                }
            }
        }
    }

    private final ConcurrentHashMap<String, MergeOperation> activeOperations = new ConcurrentHashMap<>();

    /**
     * Begins tracking a new multi-phase MERGE operation.
     *
     * @param parentQueryId the parent query identifier
     * @param targetTable   the target table being modified
     */
    public void begin(String parentQueryId, String targetTable) {
        activeOperations.put(parentQueryId, new MergeOperation(parentQueryId, targetTable));
        LOG.info("event=merge_wal_begin parentQueryId={} targetTable={}", parentQueryId, targetTable);
    }

    /**
     * Records a phase that is about to start execution.
     *
     * @param parentQueryId the parent MERGE query identifier
     * @param phaseQueryId  the sub-query identifier for this phase
     * @param phaseName     human-readable phase name (e.g., "INSERT", "DELETE", "UPDATE")
     * @param sql           the SQL being executed for this phase
     */
    public void recordPhaseStarted(String parentQueryId, String phaseQueryId,
                                   String phaseName, String sql) {
        var op = activeOperations.get(parentQueryId);
        if (op == null) {
            LOG.warn("event=merge_wal_orphan_phase parentQueryId={} phaseQueryId={}",
                    parentQueryId, phaseQueryId);
            return;
        }
        op.addPhase(new PhaseRecord(phaseQueryId, phaseName, sql,
                PhaseState.EXECUTING, Instant.now(), null));
        LOG.info("event=merge_wal_phase_started parentQueryId={} phaseQueryId={} phase={}",
                parentQueryId, phaseQueryId, phaseName);
    }

    /**
     * Records that a phase has been successfully committed.
     *
     * @param parentQueryId the parent MERGE query identifier
     * @param phaseQueryId  the sub-query identifier for the committed phase
     */
    public void recordPhaseCommitted(String parentQueryId, String phaseQueryId) {
        var op = activeOperations.get(parentQueryId);
        if (op != null) {
            op.updatePhase(phaseQueryId, PhaseState.COMMITTED);
        }
        LOG.info("event=merge_wal_phase_committed parentQueryId={} phaseQueryId={}",
                parentQueryId, phaseQueryId);
    }

    /**
     * Records that the entire MERGE operation completed successfully.
     * Removes the operation from the active tracking map.
     *
     * @param parentQueryId the parent MERGE query identifier
     */
    public void complete(String parentQueryId) {
        var op = activeOperations.remove(parentQueryId);
        if (op != null) {
            LOG.info("event=merge_wal_complete parentQueryId={} targetTable={} phases={}",
                    parentQueryId, op.targetTable(), op.phaseCount());
        }
    }

    /**
     * Records a failure in the MERGE operation and produces a structured
     * recovery report. The operation remains in the active map for inspection.
     *
     * @param parentQueryId the parent MERGE query identifier
     * @param failedPhaseId the phase that failed (may be null if failure is pre-phase)
     * @param cause         the exception that caused the failure
     */
    public void recordFailure(String parentQueryId, String failedPhaseId, Exception cause) {
        var op = activeOperations.get(parentQueryId);
        if (op == null) {
            LOG.error("event=merge_wal_failure_no_record parentQueryId={} error={}",
                    parentQueryId, cause.getMessage());
            return;
        }

        if (failedPhaseId != null) {
            op.updatePhase(failedPhaseId, PhaseState.FAILED);
        }
        op.failRemainingPhases();

        List<String> committedPhases = new ArrayList<>();
        List<String> failedPhases = new ArrayList<>();
        for (PhaseRecord phase : op.snapshotPhases()) {
            String entry = phase.phaseName() + ":" + phase.phaseQueryId();
            if (phase.state() == PhaseState.COMMITTED) {
                committedPhases.add(entry);
            } else {
                failedPhases.add(entry);
            }
        }

        LOG.error("event=merge_wal_failure parentQueryId={} targetTable={}"
                        + " committedPhases={} failedPhases={} error={}"
                        + " recovery=manual_inspection_required",
                parentQueryId, op.targetTable(), committedPhases, failedPhases,
                cause.getMessage());
    }

    /**
     * Returns an immutable snapshot of phases for a MERGE operation,
     * or an empty list if not tracked. Used for diagnostics and recovery.
     */
    public List<PhaseRecord> getPhases(String parentQueryId) {
        var op = activeOperations.get(parentQueryId);
        return op != null ? op.snapshotPhases() : List.of();
    }

    /**
     * Returns the target table for a tracked MERGE operation, or null.
     */
    public String getTargetTable(String parentQueryId) {
        var op = activeOperations.get(parentQueryId);
        return op != null ? op.targetTable() : null;
    }

    /**
     * Removes a completed or failed operation from tracking.
     * Called during periodic cleanup.
     */
    public void removeOperation(String parentQueryId) {
        activeOperations.remove(parentQueryId);
    }

    /** Returns the number of currently active MERGE operations. */
    public int activeCount() {
        return activeOperations.size();
    }
}
