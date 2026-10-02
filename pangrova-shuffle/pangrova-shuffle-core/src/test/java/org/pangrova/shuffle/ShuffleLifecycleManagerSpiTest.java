package org.pangrova.shuffle;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates ShuffleLifecycleManager SPI records: immutability, validation, and
 * lifecycle implementation behavior.
 */
class ShuffleLifecycleManagerSpiTest {

    // --- ShuffleServerEndpoint ---

    @Test
    @DisplayName("ShuffleServerEndpoint: valid construction")
    void serverEndpointValid() {
        var ep = new ShuffleServerEndpoint("shuffle-1.local", 9090, 0, 1, "shuffle-1.local");
        assertEquals("shuffle-1.local", ep.host());
        assertEquals(9090, ep.port());
    }

    @Test
    @DisplayName("ShuffleServerEndpoint: null host rejected")
    void serverEndpointNullHost() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleServerEndpoint(null, 9090, 0, 1, ""));
    }

    @Test
    @DisplayName("ShuffleServerEndpoint: non-positive port rejected")
    void serverEndpointInvalidPort() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleServerEndpoint("host", 0, 0, 1, ""));
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleServerEndpoint("host", -1, 0, 1, ""));
    }

    // --- ShuffleRegistration ---

    @Test
    @DisplayName("ShuffleRegistration: valid construction")
    void registrationValid() {
        var reg = new ShuffleRegistration(
                "q-1", 0, 16, 1024L, true, 1L, 16, new byte[32]);
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
                () -> new ShuffleRegistration(
                        null, 0, 16, 0, false, 1L, 16, new byte[32]));
    }

    @Test
    @DisplayName("ShuffleRegistration: non-positive numPartitions rejected")
    void registrationInvalidPartitions() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration(
                        "q-1", 0, 0, 0, false, 1L, 1, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration(
                        "q-1", 0, -1, 0, false, 1L, 1, new byte[32]));
    }

    @Test
    @DisplayName("ShuffleRegistration: negative estimatedBytes rejected")
    void registrationNegativeBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration(
                        "q-1", 0, 16, -1, false, 1L, 16, new byte[32]));
    }

    @Test
    @DisplayName("ShuffleRegistration: non-positive planEpoch rejected")
    void registrationInvalidPlanEpoch() {
        assertThrows(IllegalArgumentException.class,
                () -> new ShuffleRegistration(
                        "q-1", 0, 16, 0, false, 0L, 16, new byte[32]));
    }

    // --- ShuffleHandle ---

    @Test
    @DisplayName("ShuffleHandle: valid construction with endpoints")
    void handleValid() {
        var ep = new ShuffleServerEndpoint("shuffle-1", 9090, 0, 1, "");
        var handle = handle("shuffle-123", List.of(ep));
        assertEquals("shuffle-123", handle.shuffleId());
        assertEquals(1, handle.endpoints().size());
        assertEquals("shuffle-1", handle.endpoints().get(0).host());
    }

    @Test
    @DisplayName("ShuffleHandle: endpoints list is immutable")
    void handleEndpointsImmutable() {
        var ep = new ShuffleServerEndpoint("shuffle-1", 9090, 0, 1, "");
        var handle = handle("shuffle-123", List.of(ep));
        assertThrows(UnsupportedOperationException.class,
                () -> handle.endpoints().add(
                        new ShuffleServerEndpoint("shuffle-2", 9091, 1, 2, "")));
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
                () -> new ShuffleHandle(
                        null, List.of(), null, null, null, null, null,
                        0, new byte[0]));
    }

    @Test
    @DisplayName("ShuffleHandle: null endpoints rejected")
    void handleNullEndpoints() {
        assertThrows(NullPointerException.class,
                () -> new ShuffleHandle(
                        "id", null, null, null, null, null, null,
                        0, new byte[0]));
    }

    @Test
    void handleValidatesLargePartitionCoverageWithoutPerPartitionScan() {
        var handle = handle("large", List.of(
                new ShuffleServerEndpoint("a", 9000, 0, 500_000, ""),
                new ShuffleServerEndpoint("b", 9001, 500_000, 1_000_000, "")));

        assertDoesNotThrow(() -> handle.validatePartitionCoverage(1_000_000));
        assertEquals("b", handle.endpointForPartition(999_999).host());
    }

    @Test
    void shuffleAccessUsesValueEqualityAndDefensiveCopies() {
        byte[] token = {1, 2, 3};
        ShuffleAccess first = new ShuffleAccess(token, 42L);
        token[0] = 9;
        byte[] returned = first.token();
        returned[1] = 9;

        assertEquals(new ShuffleAccess(new byte[] {1, 2, 3}, 42L), first);
        assertArrayEquals(new byte[] {1, 2, 3}, first.token());
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

    private static ShuffleHandle handle(
            String id, List<ShuffleServerEndpoint> endpoints) {
        long expiry = System.currentTimeMillis() + 60_000L;
        return new ShuffleHandle(
                id, endpoints,
                new ShuffleServiceAddress("commit", 9100, ""),
                new ShuffleAccess(new byte[] {1}, expiry),
                new ShuffleAccess(new byte[] {2}, expiry),
                new ShuffleAccess(new byte[] {4}, expiry),
                new ShuffleAccess(new byte[] {3}, expiry),
                1, new byte[32]);
    }

    @Test
    @DisplayName("NoOp: registerShuffle returns LOCAL handle")
    void noOpRegister() {
        var reg = new ShuffleRegistration(
                "q-1", 0, 16, 0, false, 1L, 16, new byte[32]);
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
