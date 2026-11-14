package org.pangrova.shuffle.api;

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
    @DisplayName("ShuffleHandle: empty endpoints cannot masquerade as a registration")
    void handleRejectsEmptyRegistration() {
        assertThrows(IllegalArgumentException.class, () -> handle("local", List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleHandle(
                "local", List.of(), null, null, null, null, null, 0, new byte[0]));
    }

    @Test
    void registrationRejectsBlankQueryAndNegativeStage() {
        assertThrows(IllegalArgumentException.class, () -> new ShuffleRegistration(
                " ", 0, 1, 0, false, 1, 1, new byte[32]));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleRegistration(
                "q", -1, 1, 0, false, 1, 1, new byte[32]));
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

    @Test
    void receiptBindsExactAllocationAndImmutablePartitionContents() {
        var key = new ShufflePublication("q", 0, 2, "allocation");
        var partition = new ShuffleCommitReceipt.Partition(0, 1024, 100, "ab".repeat(32));
        var partitions = new java.util.HashMap<Integer, ShuffleCommitReceipt.Partition>();
        partitions.put(0, partition);
        var receipt = new ShuffleCommitReceipt(key, 1, "cd".repeat(32), partitions);
        partitions.clear();
        assertEquals(Map.of(0, partition), receipt.partitions());
        assertThrows(UnsupportedOperationException.class, () -> receipt.partitions().clear());
        assertNotEquals(key, new ShufflePublication("q", 0, 3, "allocation"));
    }

    @Test
    void missingAndMalformedPublicationEvidenceCannotRepresentCompletion() {
        var key = new ShufflePublication("q", 0, 2, "allocation");
        String digest = "ab".repeat(32);
        var empty = new ShuffleCommitReceipt.Partition(-1, 0, 0, digest);
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt(key, 1, digest, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt(key, 0, digest, Map.of(0, empty)));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt(key, 1, digest, Map.of(1, empty)));
        assertThrows(NullPointerException.class, () -> new ShuffleCommitReceipt(key, 1, digest, null));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt.Partition(-1, 0, 1, digest));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt.Partition(0, -1, 1, digest));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt.Partition(0, 1, -1, digest));
        assertThrows(IllegalArgumentException.class, () -> new ShuffleCommitReceipt.Partition(0, 1, 1, "invalid"));
    }

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

}
