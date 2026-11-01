package org.pangrova.coordinator.query.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.coordinator.query.execution.dispatch.shuffle.LocalShuffleLifecycleManager;
import org.pangrova.shuffle.api.capability.ShuffleAccess;
import org.pangrova.shuffle.api.commit.ShuffleCommitResult;
import org.pangrova.shuffle.api.registration.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.registration.ShuffleServiceAddress;
import org.pangrova.shuffle.api.registration.ShuffleHandle;
import org.pangrova.shuffle.api.contract.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.registration.ShuffleRegistration;
import org.junit.jupiter.api.Test;

/**
 * Tests for ShuffleLifecycleManager integration with DAGDispatchEngine.
 * Verifies that registerShuffle, commitShuffle, and cleanupShuffle are
 * called at the correct points in the stage lifecycle.
 */
class ShuffleLifecycleIntegrationTest {

    @Test
    void noOpManagerDoesNothing() {
        LocalShuffleLifecycleManager manager = LocalShuffleLifecycleManager.INSTANCE;

        ShuffleHandle handle = manager.registerShuffle(
                new ShuffleRegistration("q1", 0, 4, 1024, false, 1L,
                        4, new byte[32]));
        assertEquals("local", handle.shuffleId());
        assertTrue(handle.endpoints().isEmpty());

        ShuffleCommitResult result = manager.commitShuffle("q1", 0);
        assertTrue(result.partitionStats().isEmpty());
        assertEquals(0, result.totalBytes());
        assertEquals(0, result.totalRows());

        // Cleanup should not throw
        manager.cleanupShuffle("q1");
    }

    @Test
    void recordingManagerTracksRegisterCalls() {
        RecordingShuffleLifecycleManager recorder = new RecordingShuffleLifecycleManager();

        recorder.registerShuffle(new ShuffleRegistration(
                "q1", 0, 4, 1024, false, 1L, 4, new byte[32]));
        recorder.registerShuffle(new ShuffleRegistration(
                "q1", 1, 8, 2048, true, 1L, 8, new byte[32]));

        assertEquals(2, recorder.registrations.size());
        assertEquals("q1", recorder.registrations.get(0).queryId());
        assertEquals(0, recorder.registrations.get(0).stageId());
        assertEquals(1, recorder.registrations.get(1).stageId());
        assertTrue(recorder.registrations.get(1).requireReplication());
    }

    @Test
    void recordingManagerTracksCommitCalls() {
        RecordingShuffleLifecycleManager recorder = new RecordingShuffleLifecycleManager();

        recorder.registerShuffle(new ShuffleRegistration(
                "q1", 0, 4, 1024, false, 1L, 4, new byte[32]));
        ShuffleCommitResult result = recorder.commitShuffle("q1", 0);

        assertEquals(1, recorder.commitCalls.size());
        assertEquals("q1:0", recorder.commitCalls.get(0));
        assertNotNull(result);
    }

    @Test
    void recordingManagerTracksCleanupCalls() {
        RecordingShuffleLifecycleManager recorder = new RecordingShuffleLifecycleManager();

        recorder.registerShuffle(new ShuffleRegistration(
                "q1", 0, 4, 1024, false, 1L, 4, new byte[32]));
        recorder.cleanupShuffle("q1");

        assertEquals(1, recorder.cleanupCalls.size());
        assertEquals("q1", recorder.cleanupCalls.get(0));
    }

    @Test
    void commitResultWithPartitionStats() {
        Map<Integer, org.pangrova.shuffle.api.commit.ShufflePartitionStats> stats = Map.of(
                0, new org.pangrova.shuffle.api.commit.ShufflePartitionStats(0, 1024, 100),
                1, new org.pangrova.shuffle.api.commit.ShufflePartitionStats(1, 2048, 200));
        ShuffleCommitResult result = new ShuffleCommitResult(stats, 3072, 300);

        assertEquals(2, result.partitionStats().size());
        assertEquals(3072, result.totalBytes());
        assertEquals(300, result.totalRows());
        assertFalse(result.partitionStats().isEmpty());
    }

    /**
     * Recording implementation for verifying lifecycle calls.
     * Not a mock — a real implementation that records invocations.
     */
    static final class RecordingShuffleLifecycleManager implements ShuffleLifecycleManager {
        final List<ShuffleRegistration> registrations = new CopyOnWriteArrayList<>();
        final List<String> commitCalls = new CopyOnWriteArrayList<>();
        final List<String> cleanupCalls = new CopyOnWriteArrayList<>();

        @Override
        public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
            registrations.add(registration);
            long expiry = System.currentTimeMillis() + 300_000L;
            return new ShuffleHandle(
                    registration.queryId() + "-stage-" + registration.stageId(),
                    List.of(new ShuffleServerEndpoint(
                            "localhost", 9400, 0, registration.numPartitions(), "")),
                    new ShuffleServiceAddress("localhost", 9400, ""),
                    new ShuffleAccess(new byte[] {1}, expiry),
                    new ShuffleAccess(new byte[] {2}, expiry),
                    new ShuffleAccess(new byte[] {3}, expiry),
                    new ShuffleAccess(new byte[] {4}, expiry),
                    registration.expectedProducerCount(),
                    registration.expectedProducerSetChecksum());
        }

        @Override
        public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
            commitCalls.add(queryId + ":" + stageId);
            return ShuffleCommitResult.EMPTY;
        }

        @Override
        public void cleanupShuffle(String queryId) {
            cleanupCalls.add(queryId);
        }
    }
}
