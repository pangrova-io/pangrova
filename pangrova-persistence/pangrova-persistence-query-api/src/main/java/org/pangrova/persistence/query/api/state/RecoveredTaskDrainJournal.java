package org.pangrova.persistence.query.api.state;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.journal.TaskStateEvent;

/** Bounded issued-attempt recovery under an exact PostgreSQL execution claim. */
@Experimental
public interface RecoveredTaskDrainJournal {
    int PAGE_SIZE = 16;
    int MAX_RECEIPT_BYTES = 65_536;

    /** Returns candidate IDs with durable query or task evidence, without reading event payloads. */
    Set<String> observedRecoveryExecutions(Set<String> claims);

    /** Reads unproven predecessor attempts after a task-journal row ID, including terminal and orphaned queries. */
    List<IssuedAttempt> pendingTaskDrains(ExecutionAnchor claim, long afterRowId);

    /** Retains validated Worker receipts for this claim. Returns false after the exact grant expires or changes. */
    boolean retainTaskDrains(ExecutionAnchor claim, List<DrainReceipt> receipts);

    /** Ends a SQL predecessor and closes its issuance after exact drain coverage is proven. */
    DrainOutcome confirmRecoveredTaskDrains(ExecutionAnchor claim);

    /** Outcomes of an authoritative drain check under the current execution fence. */
    enum DrainOutcome {
        LOST_CLAIM, NO_SQL_EVIDENCE, MISSING_ISSUANCE, PENDING_COORDINATOR_STATEMENT,
        PENDING_RECEIPTS, PENDING_WRITE, CONFIRMED
    }

    /** Original immutable task issuance and its opaque paging key. */
    record IssuedAttempt(long rowId, TaskStateEvent event) { }

    /** Compact binary exit evidence already validated against the original seal by the Coordinator. */
    record DrainReceipt(long rowId, byte[] encoded) {
        public DrainReceipt {
            Objects.requireNonNull(encoded, "encoded");
            if (rowId <= 0 || encoded.length == 0 || encoded.length > MAX_RECEIPT_BYTES) {
                throw new IllegalArgumentException("Invalid recovered task drain receipt");
            }
            encoded = encoded.clone();
        }

        @Override
        public byte[] encoded() { return encoded.clone(); }
    }
}
