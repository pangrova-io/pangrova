package org.pangrova.spi.coordination;

import java.util.List;
import java.util.Optional;
import org.pangrova.spi.annotation.Stable;

/** Read-only journal facts from the same store owner; replay alone is not Native exit evidence. */
@Stable(since = "1.0")
public interface QueryStateJournalReader {
    /** Returns ordered query facts excluding queries with a terminal fact. */
    List<QueryStateEvent> replayActiveQueries();
    /** Returns ordered task facts for active journaled queries. */
    List<TaskStateEvent> replayActiveTasks();
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
}
