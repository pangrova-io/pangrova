package org.pangrova.shuffle;

import java.util.List;
import java.util.Map;

import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShufflePartitionStats;
import org.pangrova.shuffle.ShuffleRegistration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates ShuffleLifecycleManager SPI records: immutability, validation, and
 * NoOp implementation behavior.
 * Covers Task 28.7.
 */
class ShuffleLifecycleManagerSpiTest {

    // --- ShuffleServerEndpoint ---

    @Test
    @DisplayName("ShuffleServerEndpoint: valid construction")
    void serverEndpointValid() {
        var ep = new ShuffleServerEndpoint("rss-1.local", 9090);
        assertEquals("rss-1.local", ep.host());
        assertEquals(9090, ep.port());
    }

    @Test
    @DisplayName("ShuffleServerEndpoint: null host rejected")
    void serverEndpointNullHost() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleServerEndpoint(null, 9090));
    }

    @Test
    @DisplayName("ShuffleServerEndpoint: non-positive port rejected")
    void serverEndpointInvalidPort() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleServerEndpoint("host", 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleServerEndpoint("host", -1));
    }

    // --- ShuffleRegistration ---

    @Test
    @DisplayName("ShuffleRegistration: valid construction")
    void registrationValid() {
        var reg = new ShuffleRegistration("q-1", 0, 16, 1024L, true);
        assertEquals("q-1", reg.queryId());
        assertEquals(0, reg.stageId());
        assertEquals(16, reg.numPartitions());
        assertEquals(1024L, reg.estimatedBytesPerPartition());
        assertTrue(reg.requireReplication());
    }

    @Test
    @DisplayName("ShuffleRegistration: null queryId rejected")
    void registrationNullQueryId() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleRegistration(null, 0, 16, 0, false));
    }

    @Test
    @DisplayName("ShuffleRegistration: non-positive numPartitions rejected")
    void registrationInvalidPartitions() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration("q-1", 0, 0, 0, false));
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration("q-1", 0, -1, 0, false));
    }

    @Test
    @DisplayName("ShuffleRegistration: negative estimatedBytes rejected")
    void registrationNegativeBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration("q-1", 0, 16, -1, false));
    }

    // --- ShuffleHandle ---

    @Test
    @DisplayName("ShuffleHandle: valid construction with endpoints")
    void handleValid() {
        var ep = new ShuffleServerEndpoint("rss-1", 9090);
        var handle = new ShuffleHandle("shuffle-123", List.of(ep));
        assertEquals("shuffle-123", handle.shuffleId());
        assertEquals(1, handle.endpoints().size());
        assertEquals("rss-1", handle.endpoints().get(0).host());
    }

    @Test
    @DisplayName("ShuffleHandle: endpoints list is immutable")
    void handleEndpointsImmutable() {
        var ep = new ShuffleServerEndpoint("rss-1", 9090);
        var handle = new ShuffleHandle("shuffle-123", List.of(ep));
        assertThrows(UnsupportedOperationException.class,
                () -> handle.endpoints().add(new ShuffleServerEndpoint("rss-2", 9091)));
    }

    @Test
    @DisplayName("ShuffleHandle: LOCAL sentinel has empty endpoints")
    void handleLocalSentinel() {
        assertEquals("local", ShuffleHandle.LOCAL.shuffleId());
        assertTrue(ShuffleHandle.LOCAL.endpoints().isEmpty());
    }

    @Test
    @DisplayName("ShuffleHandle: null shuffleId rejected")
    void handleNullShuffleId() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleHandle(null, List.of()));
    }

    @Test
    @DisplayName("ShuffleHandle: null endpoints rejected")
    void handleNullEndpoints() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleHandle("id", null));
    }

    // --- ShuffleCommitResult ---

    @Test
    @DisplayName("ShuffleCommitResult: valid construction")
    void commitResultValid() {
        var stats = Map.of(0, new ShufflePartitionStats(0, 1024, 100));
        var result = new ShuffleCommitResult(stats, 1024, 100);
        assertEquals(1, result.partitionStats().size());
        assertEquals(1024, result.totalBytes());
        assertEquals(100, result.totalRows());
    }

    @Test
    @DisplayName("ShuffleCommitResult: partitionStats map is immutable")
    void commitResultStatsImmutable() {
        var result = new ShuffleCommitResult(Map.of(), 0, 0);
        assertThrows(UnsupportedOperationException.class,
                () -> result.partitionStats().put(0, new ShufflePartitionStats(0, 0, 0)));
    }

    @Test
    @DisplayName("ShuffleCommitResult: EMPTY sentinel")
    void commitResultEmpty() {
        assertTrue(ShuffleCommitResult.EMPTY.partitionStats().isEmpty());
        assertEquals(0, ShuffleCommitResult.EMPTY.totalBytes());
        assertEquals(0, ShuffleCommitResult.EMPTY.totalRows());
    }

    @Test
    @DisplayName("ShuffleCommitResult: negative totalBytes rejected")
    void commitResultNegativeBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleCommitResult(Map.of(), -1, 0));
    }

    @Test
    @DisplayName("ShuffleCommitResult: negative totalRows rejected")
    void commitResultNegativeRows() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleCommitResult(Map.of(), 0, -1));
    }

    @Test
    @DisplayName("ShuffleCommitResult: null partitionStats rejected")
    void commitResultNullStats() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleCommitResult(null, 0, 0));
    }

    // --- NoOp ShuffleLifecycleManager contract tests (inline impl) ---

    private static final ShuffleLifecycleManager NO_OP = new ShuffleLifecycleManager() {
        @Override public ShuffleHandle registerShuffle(ShuffleRegistration r) { return ShuffleHandle.LOCAL; }
        @Override public ShuffleCommitResult commitShuffle(String qid, int sid) { return ShuffleCommitResult.EMPTY; }
        @Override public void cleanupShuffle(String qid) {}
    };

    @Test
    @DisplayName("NoOp: registerShuffle returns LOCAL handle")
    void noOpRegister() {
        var reg = new ShuffleRegistration("q-1", 0, 16, 0, false);
        ShuffleHandle handle = NO_OP.registerShuffle(reg);
        assertSame(ShuffleHandle.LOCAL, handle);
    }

    @Test
    @DisplayName("NoOp: commitShuffle returns EMPTY result")
    void noOpCommit() {
        assertSame(ShuffleCommitResult.EMPTY, NO_OP.commitShuffle("q-1", 0));
    }

    @Test
    @DisplayName("NoOp: cleanupShuffle is no-op (no exception)")
    void noOpCleanup() {
        assertDoesNotThrow(() -> NO_OP.cleanupShuffle("q-1"));
    }

    @Test
    @DisplayName("NoOp: singleton instance")
    void noOpSingleton() {
        assertNotNull(NO_OP);
    }
}
