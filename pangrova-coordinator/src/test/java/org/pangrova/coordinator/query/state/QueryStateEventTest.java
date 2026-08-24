package org.pangrova.coordinator.query.state;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.pangrova.spi.coordination.QueryStateEvent;

/**
 * Unit tests for {@link QueryStateEvent} record validation and immutability.
 */
class QueryStateEventTest {

    @Test
    void validEventCreation() {
        var event = new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L);
        assertEquals("q-1", event.queryId());
        assertEquals("SELECT 1", event.sql());
        assertEquals("{}", event.sessionJson());
        assertEquals("QUEUED", event.state());
        assertEquals(1000L, event.timestampMs());
    }

    @Test
    void nullableFieldsAllowed() {
        var event = new QueryStateEvent("q-1", null, null, "RUNNING", 2000L);
        assertNull(event.sql());
        assertNull(event.sessionJson());
    }

    @Test
    void nullQueryIdRejected() {
        assertThrows(NullPointerException.class,
                () -> new QueryStateEvent(null, "SELECT 1", "{}", "QUEUED", 1000L));
    }

    @Test
    void nullStateRejected() {
        assertThrows(NullPointerException.class,
                () -> new QueryStateEvent("q-1", "SELECT 1", "{}", null, 1000L));
    }

    @Test
    void negativeTimestampRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", -1L));
    }

    @Test
    void zeroTimestampAllowed() {
        var event = new QueryStateEvent("q-1", null, null, "QUEUED", 0L);
        assertEquals(0L, event.timestampMs());
    }

    @Test
    void equalityAndHashCode() {
        var a = new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L);
        var b = new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void inequalityOnDifferentState() {
        var a = new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L);
        var b = new QueryStateEvent("q-1", "SELECT 1", "{}", "RUNNING", 1000L);
        assertNotEquals(a, b);
    }
}
