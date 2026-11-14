package org.pangrova.coordinator.query.state;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.pangrova.query.api.journal.TaskStateEvent;

/**
 * Unit tests for {@link TaskStateEvent} record validation and immutability.
 */
class TaskStateEventTest {

    @Test
    void validEventCreation() {
        var event = new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 5L);
        assertEquals("q-1", event.queryId());
        assertEquals("t-1", event.taskId());
        assertEquals("w-1", event.workerId());
        assertEquals("RUNNING", event.state());
        assertEquals(0, event.stageId());
        assertEquals(5L, event.reportSequence());
        assertEquals(1000L, event.context().timestampMs());
    }

    @Test
    void nullWorkerIdAllowed() {
        var event = new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", null, "SCHEDULED", 0, 0L);
        assertNull(event.workerId());
    }

    @Test
    void nullQueryIdRejected() {
        assertThrows(NullPointerException.class,
                () -> new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), null, "t-1", "w-1", "RUNNING", 0, 0L));
    }

    @Test
    void nullTaskIdRejected() {
        assertThrows(NullPointerException.class,
                () -> new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", null, "w-1", "RUNNING", 0, 0L));
    }

    @Test
    void nullStateRejected() {
        assertThrows(NullPointerException.class,
                () -> new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", "w-1", null, 0, 0L));
    }

    @Test
    void negativeTimestampRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(-1L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 0L));
    }

    @Test
    void zeroTimestampAllowed() {
        var event = new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(0L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 0L);
        assertEquals(0L, event.context().timestampMs());
    }

    @Test
    void equalityAndHashCode() {
        var context = org.pangrova.query.api.journal.JournalEventContext.create(1000L, null);
        var a = new TaskStateEvent(context, "q-1", "t-1", "w-1", "RUNNING", 2, 5L);
        var b = new TaskStateEvent(context, "q-1", "t-1", "w-1", "RUNNING", 2, 5L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void inequalityOnDifferentTaskId() {
        var context = org.pangrova.query.api.journal.JournalEventContext.create(1000L, null);
        var a = new TaskStateEvent(context, "q-1", "t-1", "w-1", "RUNNING", 0, 0L);
        var b = new TaskStateEvent(context, "q-1", "t-2", "w-1", "RUNNING", 0, 0L);
        assertNotEquals(a, b);
    }
}
