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
    /** Reads ordered query and task facts for exactly the claimed executions in one store snapshot. */
    RecoveryBatch replayClaimedQueries(Set<String> queryIds);

    /** Ordered committed facts for the active queries within a single claim batch. */
    record RecoveryBatch(List<QueryStateEvent> queries, List<TaskStateEvent> tasks) {
        public RecoveryBatch {
            queries = List.copyOf(queries);
            tasks = List.copyOf(tasks);
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

    /** Returns the latest state when the original admission belongs to this principal. */
    Optional<String> latestQueryStateForPrincipal(String queryId, String principal);
}
