package org.pangrova.protocol.rest.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests for the pull-mode split transport protocol. Task execution semantics
 * live exclusively in canonical v2 and are not mirrored in this REST API.
 */
class SplitProtocolTest {

    @Test
    void splitRequestValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> new SplitRequest(null, 0, "w1", List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new SplitRequest("q1", 0, null, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new SplitRequest("", 0, "w1", List.of()));
    }

    @Test
    void splitRequestNullLocalityDefaultsToEmpty() {
        SplitRequest req = new SplitRequest("q1", 0, "w1", null);
        assertEquals(List.of(), req.workerLocality());
    }

    @Test
    void splitRequestImmutableLocality() {
        SplitRequest req = new SplitRequest("q1", 0, "w1", List.of("host-a"));
        assertThrows(UnsupportedOperationException.class,
                () -> req.workerLocality().add("host-b"));
    }

    @Test
    void splitResponseNoMoreSplitsSentinel() {
        SplitResponse resp = SplitResponse.NO_MORE_SPLITS;
        assertNull(resp.splitData());
        assertEquals(-1, resp.splitIndex());
        assertFalse(resp.hasMore());
        assertFalse(resp.hasSplit());
    }

    @Test
    void splitResponseWithData() {
        SplitResponse resp = new SplitResponse("data", 5, true, List.of("host-a"));
        assertTrue(resp.hasSplit());
        assertEquals("data", resp.splitData());
        assertEquals(5, resp.splitIndex());
        assertTrue(resp.hasMore());
        assertEquals(List.of("host-a"), resp.dataLocations());
    }

}
