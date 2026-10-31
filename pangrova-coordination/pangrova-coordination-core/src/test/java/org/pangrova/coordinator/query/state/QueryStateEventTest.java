package org.pangrova.coordinator.query.state;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.pangrova.query.api.state.QueryStateEvent;

/**
 * Unit tests for {@link QueryStateEvent} record validation and immutability.
 */
class QueryStateEventTest {

    @Test
    void validEventCreation() {
        var event = new QueryStateEvent(org.pangrova.query.api.state.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED");
        assertEquals("q-1", event.queryId());
        assertEquals("SELECT 1", event.sql());
        assertEquals("{}", event.sessionJson());
        assertEquals("QUEUED", event.state());
        assertEquals(1000L, event.context().timestampMs());
    }

    @Test
    void nullableFieldsAllowed() {
        var event = new QueryStateEvent(org.pangrova.query.api.state.JournalEventContext.create(2000L, null), "q-1", null, null, "RUNNING");
        assertNull(event.sql());
        assertNull(event.sessionJson());
    }

    @Test
    void nullQueryIdRejected() {
        assertThrows(NullPointerException.class,
                () -> new QueryStateEvent(org.pangrova.query.api.state.JournalEventContext.create(1000L, null), null, "SELECT 1", "{}", "QUEUED"));
    }

    @Test
    void nullStateRejected() {
        assertThrows(NullPointerException.class,
                () -> new QueryStateEvent(org.pangrova.query.api.state.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", null));
    }

    @Test
    void negativeTimestampRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryStateEvent(org.pangrova.query.api.state.JournalEventContext.create(-1L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
    }

    @Test
    void zeroTimestampAllowed() {
        var event = new QueryStateEvent(org.pangrova.query.api.state.JournalEventContext.create(0L, null), "q-1", null, null, "QUEUED");
        assertEquals(0L, event.context().timestampMs());
    }

    @Test
    void equalityAndHashCode() {
        var context = org.pangrova.query.api.state.JournalEventContext.create(1000L, null);
        var a = new QueryStateEvent(context, "q-1", "SELECT 1", "{}", "QUEUED");
        var b = new QueryStateEvent(context, "q-1", "SELECT 1", "{}", "QUEUED");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void inequalityOnDifferentState() {
        var context = org.pangrova.query.api.state.JournalEventContext.create(1000L, null);
        var a = new QueryStateEvent(context, "q-1", "SELECT 1", "{}", "QUEUED");
        var b = new QueryStateEvent(context, "q-1", "SELECT 1", "{}", "RUNNING");
        assertNotEquals(a, b);
    }
}
