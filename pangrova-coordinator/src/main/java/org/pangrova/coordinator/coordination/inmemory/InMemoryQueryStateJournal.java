package org.pangrova.coordinator.coordination.inmemory;

import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;

import java.util.List;

/**
 * No-op journal implementation. Default when {@code state-journal.type: NONE}.
 * All append operations are silently discarded; replay returns empty lists.
 * Preserves current behavior (no persistence) with zero overhead.
 */
public final class InMemoryQueryStateJournal implements QueryStateJournal {

    public static final InMemoryQueryStateJournal INSTANCE = new InMemoryQueryStateJournal();

    @Override
    public void appendQueryEvent(QueryStateEvent event) {
        // no-op
    }

    @Override
    public void appendQueryEventSync(QueryStateEvent event) {
        // no-op
    }

    @Override
    public void appendTaskEvent(TaskStateEvent event) {
        // no-op
    }

    @Override
    public void appendTaskEventSync(TaskStateEvent event) {
        // no-op
    }

    @Override
    public List<QueryStateEvent> replayActiveQueries() {
        return List.of();
    }

    @Override
    public List<TaskStateEvent> replayActiveTasks() {
        return List.of();
    }

    @Override
    public void purgeQuery(String queryId) {
        // no-op
    }
}
