package org.pangrova.resilience.checkpoint;

import org.pangrova.resilience.checkpoint.CheckpointManager.CheckpointEntry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests CheckpointManager in-memory checkpoint storage and defensive copy behavior.
 */
class CheckpointManagerTest {

    private final Clock fixedClock = Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
    private final CheckpointManager manager = new CheckpointManager(fixedClock);

    @Test
    void checkpoint_storesAndRetrieves() {
        byte[] state = {1, 2, 3};
        manager.checkpoint("task-1", state);

        Optional<CheckpointEntry> entry = manager.getLatestCheckpoint("task-1");
        assertThat(entry).isPresent();
        assertThat(entry.get().taskId()).isEqualTo("task-1");
        assertThat(entry.get().state()).isEqualTo(new byte[]{1, 2, 3});
        assertThat(entry.get().timestamp()).isEqualTo(fixedClock.instant());
    }

    @Test
    void hasCheckpoint_returnsTrueWhenPresent() {
        manager.checkpoint("task-1", new byte[]{1});
        assertThat(manager.hasCheckpoint("task-1")).isTrue();
    }

    @Test
    void hasCheckpoint_returnsFalseWhenAbsent() {
        assertThat(manager.hasCheckpoint("unknown")).isFalse();
    }

    @Test
    void clearCheckpoints_removesEntry() {
        manager.checkpoint("task-1", new byte[]{1});
        manager.clearCheckpoints("task-1");
        assertThat(manager.hasCheckpoint("task-1")).isFalse();
    }

    @Test
    void getLatestCheckpoint_returnsEmptyWhenAbsent() {
        assertThat(manager.getLatestCheckpoint("unknown")).isEmpty();
    }

    @Test
    void checkpoint_overwritesPrevious() {
        manager.checkpoint("task-1", new byte[]{1});
        manager.checkpoint("task-1", new byte[]{2, 3});

        Optional<CheckpointEntry> entry = manager.getLatestCheckpoint("task-1");
        assertThat(entry).isPresent();
        assertThat(entry.get().state()).isEqualTo(new byte[]{2, 3});
    }

    @Test
    void defensiveCopy_inputMutationDoesNotAffectStored() {
        byte[] state = {1, 2, 3};
        manager.checkpoint("task-1", state);

        // Mutate the original array
        state[0] = 99;

        Optional<CheckpointEntry> entry = manager.getLatestCheckpoint("task-1");
        assertThat(entry.get().state()).isEqualTo(new byte[]{1, 2, 3});
    }

    @Test
    void defensiveCopy_outputMutationDoesNotAffectStored() {
        manager.checkpoint("task-1", new byte[]{1, 2, 3});

        byte[] retrieved = manager.getLatestCheckpoint("task-1").get().state();
        retrieved[0] = 99;

        // Re-retrieve should still be original
        byte[] retrievedAgain = manager.getLatestCheckpoint("task-1").get().state();
        assertThat(retrievedAgain).isEqualTo(new byte[]{1, 2, 3});
    }

    @Test
    void checkpointEntry_nullState_handledGracefully() {
        manager.checkpoint("task-null", null);
        Optional<CheckpointEntry> entry = manager.getLatestCheckpoint("task-null");
        assertThat(entry).isPresent();
        assertThat(entry.get().state()).isNull();
    }
}
