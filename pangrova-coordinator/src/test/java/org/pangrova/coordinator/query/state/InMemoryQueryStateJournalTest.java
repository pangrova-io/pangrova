package org.pangrova.coordinator.query.state;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.TaskStateEvent;

/**
 * Unit tests for {@link InMemoryQueryStateJournal}.
 * Verifies no-op behavior: append is silent, replay returns empty.
 */
class InMemoryQueryStateJournalTest {

    private final InMemoryQueryStateJournal journal = InMemoryQueryStateJournal.INSTANCE;

    @Test
    void appendQueryEventIsNoOp() {
        var event = new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L);
        assertDoesNotThrow(() -> journal.appendQueryEvent(event));
        assertDoesNotThrow(() -> journal.appendQueryEventSync(event));
    }

    @Test
    void appendTaskEventIsNoOp() {
        var event = new TaskStateEvent("q-1", "t-1", "w-1", "RUNNING", 0, 1L, 1000L);
        assertDoesNotThrow(() -> journal.appendTaskEvent(event));
        assertDoesNotThrow(() -> journal.appendTaskEventSync(event));
    }

    @Test
    void replayActiveQueriesReturnsEmpty() {
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        assertTrue(journal.replayActiveQueries().isEmpty());
    }

    @Test
    void replayActiveTasksReturnsEmpty() {
        journal.appendTaskEventSync(
                new TaskStateEvent("q-1", "t-1", "w-1", "RUNNING", 0, 1L, 1000L));
        assertTrue(journal.replayActiveTasks().isEmpty());
    }

    @Test
    void purgeQueryIsNoOp() {
        assertDoesNotThrow(() -> journal.purgeQuery("q-1"));
    }

    @Test
    void singletonInstance() {
        assertSame(InMemoryQueryStateJournal.INSTANCE, journal);
    }
}
