package org.pangrova.persistence.query.memory.state;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
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
        assertEquals(event, journal.latestQueryEvent("q-1").orElseThrow());
    }

    @Test void appendTaskEventTransfersAndRetainsOneExactFact() {
        journal.appendQueryEventSync(query("q-1", "RUNNING", 900));
        TaskStateEvent event = task("q-1", 1000);
        assertDoesNotThrow(() -> journal.appendTaskEvent(event));
        assertDoesNotThrow(() -> journal.appendTaskEventSync(event));
        var changed = new TaskStateEvent(event.context(), "q-1", "different-task", "w-1", "RUNNING", 0, 1);
        assertThrows(JournalWriteException.class, () -> journal.appendTaskEventSync(changed));
        assertDoesNotThrow(() -> journal.appendTaskEventSync(event));
    }

    @Test void latestQueryFactUsesInsertionOrder() {
        QueryStateEvent first = query("q-1", "QUEUED", 1000);
        QueryStateEvent second = query("q-1", "RUNNING", 500);
        journal.appendQueryEventSync(first);
        journal.appendQueryEventSync(second);
        assertEquals(second, journal.latestQueryEvent("q-1").orElseThrow());
    }

    @Test void purgeQueryRemovesItsFactsOnlyAndIsIdempotent() {
        QueryStateEvent other = query("q-2", "RUNNING", 1);
        TaskStateEvent otherTask = task("q-2", 2);
        journal.appendQueryEventSync(query("q-1", "QUEUED", 1));
        var removed = task("q-1", 2);
        journal.appendTaskEventSync(removed);
        journal.appendQueryEventSync(other);
        journal.appendTaskEventSync(otherTask);
        assertDoesNotThrow(() -> journal.purgeQuery("q-1"));
        assertDoesNotThrow(() -> journal.purgeQuery("q-1"));
        assertEquals(other, journal.latestQueryEvent("q-2").orElseThrow());
        assertTrue(journal.latestQueryEvent("q-1").isEmpty());
        var changed = new TaskStateEvent(removed.context(), "q-1", "replacement", "w-1", "RUNNING", 0, 1);
        assertDoesNotThrow(() -> journal.appendTaskEventSync(changed));
        var conflicting = new TaskStateEvent(otherTask.context(), "q-2", "replacement", "w-1", "RUNNING", 0, 1);
        assertThrows(JournalWriteException.class, () -> journal.appendTaskEventSync(conflicting));
    }

    @Test void instancesHaveIndependentFactsAndLifecycle() {
        InMemoryQueryStateJournal other = new InMemoryQueryStateJournal();
        assertNotSame(other, journal);
        journal.appendQueryEventSync(query("q-1", "RUNNING", 1));
        assertTrue(other.latestQueryEvent("q-1").isEmpty());
        other.close();
        assertTrue(journal.latestQueryEvent("q-1").isPresent());
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
        assertEquals(query, journal.latestQueryEvent("q-1").orElseThrow());
        var changed = new TaskStateEvent(query.context(), "q-1", "different-task", "w-1", "RUNNING", 0, 1);
        assertThrows(JournalWriteException.class, () -> journal.appendTaskEventSync(changed));
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
        assertEquals(next, bounded.latestQueryEvent("q-2").orElseThrow());
    }

    @Test void closedStoreRejectsWritesReadsAndPurgeWithoutPretendingSuccess() {
        QueryStateEvent event = query("q-1", "QUEUED", 1);
        journal.appendQueryEventSync(event);
        journal.close();
        journal.close();
        assertThrows(JournalWriteException.class, () -> journal.appendQueryEventSync(event));
        assertThrows(JournalWriteException.class, () -> journal.latestQueryEvent("q-1"));
        assertThrows(JournalWriteException.class, () -> journal.purgeQuery("q-1"));
    }

    @Test void expiredTerminalAcceptanceAllowsNewFingerprintBeforeSweep() {
        var original = accepted("q-old", "b", 1);
        assertTrue(journal.acceptQuerySync(original).newExecution());
        assertThrows(JournalWriteException.class,
                () -> journal.acceptQuerySync(accepted("q-active-conflict", "c", 2)));
        journal.appendQueryEventSync(query("q-old", "FINISHED", 3));
        // Exact ownership reconciliation is not a new candidate replacement.
        assertEquals(original.acceptance(), journal.acceptQuerySync(original).acceptance());
        assertThrows(JournalWriteException.class,
                () -> journal.acceptQuerySync(accepted("q-old", "c", System.currentTimeMillis())));
        var replacement = accepted("q-new", "c", System.currentTimeMillis());
        var admitted = journal.acceptQuerySync(replacement);
        assertTrue(admitted.newExecution());
        assertEquals(replacement.acceptance(), admitted.acceptance());
        assertTrue(journal.latestQueryEvent("q-old").isEmpty());
        assertEquals(replacement.acceptance(), journal.findAcceptance("alice", "a".repeat(64)).orElseThrow());
        var replay = journal.acceptQuerySync(accepted("q-replay", "c", System.currentTimeMillis()));
        assertFalse(replay.newExecution());
        assertEquals(replacement.acceptance(), replay.acceptance());
    }

    private static QueryStateEvent accepted(String id, String fingerprint, long time) {
        var request = new QueryAcceptanceRequest("alice", "a".repeat(64), fingerprint.repeat(64));
        var acceptance = QueryAcceptance.pending(request, id, 60_000);
        return new QueryStateEvent(JournalEventContext.create(time, null), id, "SELECT 1", "{}", "QUEUED",
                null, acceptance);
    }

    private static QueryStateEvent query(String id, String state, long time) {
        return new QueryStateEvent(JournalEventContext.create(time, null), id, "SELECT 1", "{}", state);
    }
    private static TaskStateEvent task(String id, long time) {
        return new TaskStateEvent(JournalEventContext.create(time, null), id, "t-1", "w-1", "RUNNING", 0, 1);
    }
}
