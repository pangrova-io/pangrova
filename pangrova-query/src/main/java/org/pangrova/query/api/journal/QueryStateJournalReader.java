package org.pangrova.query.api.journal;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.pangrova.common.api.annotation.Stable;

/** Read-only journal facts from the same store owner; replay alone is not Native exit evidence. */
@Stable(since = "1.0")
public interface QueryStateJournalReader {
    /** Returns ordered query facts excluding queries with a terminal fact. */
    List<QueryStateEvent> replayActiveQueries();
    /** Returns ordered task facts for active journaled queries. */
    List<TaskStateEvent> replayActiveTasks();
    /** Reads replayable facts and identifies retained evidence within the exact claim in one snapshot. */
    RecoveryBatch replayClaimedQueries(Set<String> queryIds);

    /** Ordered active facts and the IDs with any committed journal evidence, including skipped terminal facts. */
    record RecoveryBatch(List<QueryStateEvent> queries, List<TaskStateEvent> tasks, Set<String> observedExecutionIds) {
        public RecoveryBatch {
            queries = List.copyOf(queries);
            tasks = List.copyOf(tasks);
            observedExecutionIds = Set.copyOf(observedExecutionIds);
        }
    }

    /** Returns active query facts issued by the specified owner. */
    default List<QueryStateEvent> replayActiveQueriesForOwner(String coordinatorId) {
        return replayActiveQueries().stream()
                .filter(event -> coordinatorId.equals(event.context().coordinatorId())).toList();
    }
    /** Returns active task facts issued by the specified owner. */
    default List<TaskStateEvent> replayActiveTasksForOwner(String coordinatorId) {
        return replayActiveTasks().stream()
                .filter(event -> coordinatorId.equals(event.context().coordinatorId())).toList();
    }
    /** Returns the last committed query fact by journal row/insertion order. */
    Optional<QueryStateEvent> latestQueryEvent(String queryId);

    /**
     * Returns the exact persisted receipt and retirement result. An empty optional means the receipt
     * cannot be established, and a pending snapshot means there is no proven retired outcome yet.
     */
    default Optional<ReceiptRecovery> recoverReceipt(String queryId, String principal, String ticket) {
        return Optional.empty();
    }

    /** An immutable receipt is known, with a terminal state only after physical retirement. */
    record ReceiptRecovery(String terminalState, QueryTerminalOutcome outcome) {
        public ReceiptRecovery {
            if (terminalState == null ? outcome != null : outcome == null || !outcome.matchesState(terminalState)) {
                throw new IllegalArgumentException("Recovery requires a coherent terminal outcome or remains pending");
            }
        }

        public boolean pending() { return terminalState == null; }
    }

    /** Returns the latest state when the original admission belongs to this principal. */
    Optional<String> latestQueryStateForPrincipal(String queryId, String principal);
}
