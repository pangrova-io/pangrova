package org.pangrova.persistence.query.memory.state;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.persistence.query.api.commit.JournalWriteException;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;

import static org.junit.jupiter.api.Assertions.*;

/** Actual process-local store oracles, not PostgreSQL durability or HA evidence. */
class InMemoryQueryStateJournalTest {
    private final InMemoryQueryStateJournal journal = new InMemoryQueryStateJournal();

    @Test void appendQueryEventTransfersAndRetainsOneExactFact() {
        QueryStateEvent event = query("q-1", "QUEUED", 1000);
        assertDoesNotThrow(() -> journal.appendQueryEvent(event));
        assertDoesNotThrow(() -> journal.appendQueryEventSync(event));
        assertEquals(List.of(event), journal.replayActiveQueries());
        assertEquals(event, journal.latestQueryEvent("q-1").orElseThrow());
    }

    @Test void appendTaskEventTransfersAndRetainsOneExactFact() {
        journal.appendQueryEventSync(query("q-1", "RUNNING", 900));
        TaskStateEvent event = task("q-1", 1000);
        assertDoesNotThrow(() -> journal.appendTaskEvent(event));
        assertDoesNotThrow(() -> journal.appendTaskEventSync(event));
        assertEquals(List.of(event), journal.replayActiveTasks());
    }

    @Test void replayActiveQueriesPreservesAdmissionOrderNotTimestampOrder() {
        QueryStateEvent first = query("q-1", "QUEUED", 1000);
        QueryStateEvent second = query("q-1", "RUNNING", 500);
        journal.appendQueryEventSync(first);
        journal.appendQueryEventSync(second);
        assertEquals(List.of(first, second), journal.replayActiveQueries());
        assertEquals(second, journal.latestQueryEvent("q-1").orElseThrow());
    }

    @Test void replayActiveTasksExcludesTerminalAndMissingQueryFacts() {
        journal.appendQueryEventSync(query("q-1", "QUEUED", 1));
        journal.appendTaskEventSync(task("q-1", 2));
        journal.appendTaskEventSync(task("missing", 2));
        assertEquals(1, journal.replayActiveTasks().size());
        journal.appendQueryEventSync(query("q-1", "FINISHED", 3));
        assertTrue(journal.replayActiveQueries().isEmpty());
        assertTrue(journal.replayActiveTasks().isEmpty());
    }

    @Test void purgeQueryRemovesItsFactsOnlyAndIsIdempotent() {
        QueryStateEvent other = query("q-2", "RUNNING", 1);
        TaskStateEvent otherTask = task("q-2", 2);
        journal.appendQueryEventSync(query("q-1", "QUEUED", 1));
        journal.appendTaskEventSync(task("q-1", 2));
        journal.appendQueryEventSync(other);
        journal.appendTaskEventSync(otherTask);
        assertDoesNotThrow(() -> journal.purgeQuery("q-1"));
        assertDoesNotThrow(() -> journal.purgeQuery("q-1"));
        assertEquals(List.of(other), journal.replayActiveQueries());
        assertEquals(List.of(otherTask), journal.replayActiveTasks());
        assertTrue(journal.latestQueryEvent("q-1").isEmpty());
    }

    @Test void instancesHaveIndependentFactsAndLifecycle() {
        InMemoryQueryStateJournal other = new InMemoryQueryStateJournal();
        assertNotSame(other, journal);
        journal.appendQueryEventSync(query("q-1", "RUNNING", 1));
        assertTrue(other.replayActiveQueries().isEmpty());
        other.close();
        assertEquals(1, journal.replayActiveQueries().size());
    }

    @Test void sameKindIdConflictCannotOverwriteOriginalPayload() {
        QueryStateEvent event = query("q-1", "QUEUED", 1);
        journal.appendQueryEventSync(event);
        QueryStateEvent changed = new QueryStateEvent(event.context(), "q-1", "SELECT 2", "{}", "QUEUED");
        assertThrows(JournalWriteException.class, () -> journal.appendQueryEventSync(changed));
        assertEquals(event, journal.latestQueryEvent("q-1").orElseThrow());
    }

    @Test void queryAndTaskIdsUseTheirSeparateTableNamespaces() {
        QueryStateEvent query = query("q-1", "RUNNING", 1);
        TaskStateEvent task = new TaskStateEvent(query.context(), "q-1", "t-1", "w-1", "RUNNING", 0, 1);
        journal.appendQueryEventSync(query);
        journal.appendTaskEventSync(task);
        assertEquals(List.of(query), journal.replayActiveQueries());
        assertEquals(List.of(task), journal.replayActiveTasks());
    }

    @Test void capacityCountsCommittedFactsAndPurgeActuallyReturnsCapacity() {
        InMemoryQueryStateJournal bounded = new InMemoryQueryStateJournal(2);
        QueryStateEvent event = query("q-1", "RUNNING", 1);
        bounded.appendQueryEventSync(event);
        bounded.appendTaskEventSync(task("q-1", 2));
        bounded.appendQueryEventSync(event);
        assertThrows(JournalWriteException.class, () -> bounded.appendQueryEventSync(query("q-2", "QUEUED", 3)));
        bounded.purgeQuery("q-1");
        QueryStateEvent next = query("q-2", "QUEUED", 3);
        bounded.appendQueryEventSync(next);
        assertEquals(List.of(next), bounded.replayActiveQueries());
    }

    @Test void closedStoreRejectsWritesReadsAndPurgeWithoutPretendingSuccess() {
        QueryStateEvent event = query("q-1", "QUEUED", 1);
        journal.appendQueryEventSync(event);
        journal.close();
        journal.close();
        assertThrows(JournalWriteException.class, () -> journal.appendQueryEventSync(event));
        assertThrows(JournalWriteException.class, journal::replayActiveQueries);
        assertThrows(JournalWriteException.class, () -> journal.purgeQuery("q-1"));
    }

    @Test void ownerFilteredReplayKeepsOnlyActualFrozenOwners() {
        QueryStateEvent owned = new QueryStateEvent(JournalEventContext.create(1, "coord-1"),
                "q-1", "SELECT 1", "{}", "RUNNING");
        journal.appendQueryEventSync(owned);
        journal.appendQueryEventSync(query("q-2", "RUNNING", 2));
        assertEquals(List.of(owned), journal.replayActiveQueriesForOwner("coord-1"));
    }

    private static QueryStateEvent query(String id, String state, long time) {
        return new QueryStateEvent(JournalEventContext.create(time, null), id, "SELECT 1", "{}", state);
    }
    private static TaskStateEvent task(String id, long time) {
        return new TaskStateEvent(JournalEventContext.create(time, null), id, "t-1", "w-1", "RUNNING", 0, 1);
    }
}
