package org.pangrova.coordinator.resilience.checkpoint;

import org.pangrova.common.resilience.api.CheckpointStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

/**
 * Checkpoint manager delegating to a {@link CheckpointStore} strategy.
 * Defaults to {@link InMemoryCheckpointStore} for backward compatibility.
 */
public class CheckpointManager {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);

    public record CheckpointEntry(String taskId, byte[] state, Instant timestamp) {
        /** Defensive copy constructor. */
        public CheckpointEntry {
            state = (state == null) ? null : Arrays.copyOf(state, state.length);
        }

        /** Returns a defensive copy of the checkpoint state bytes. */
        @Override
        public byte[] state() {
            return (state == null) ? null : Arrays.copyOf(state, state.length);
        }
    }

    private final CheckpointStore store;
    private final Clock clock;

    public CheckpointManager() {
        this(new InMemoryCheckpointStore(), Clock.systemUTC());
    }

    public CheckpointManager(Clock clock) {
        this(new InMemoryCheckpointStore(), clock);
    }

    public CheckpointManager(CheckpointStore store) {
        this(store, Clock.systemUTC());
    }

    public CheckpointManager(CheckpointStore store, Clock clock) {
        this.store = store != null ? store : new InMemoryCheckpointStore();
        this.clock = clock;
    }

    public void checkpoint(String taskId, byte[] state) {
        store.saveCheckpoint(taskId, state);
        LOG.debug("Checkpoint saved for task {}", taskId);
    }

    /** Returns the most recent checkpoint for the task, or empty if none exists. */
    public Optional<CheckpointEntry> getLatestCheckpoint(String taskId) {
        if (!store.hasCheckpoint(taskId)) {
            return Optional.empty();
        }
        Optional<byte[]> data = store.loadCheckpoint(taskId);
        byte[] state = data.orElse(null);
        // Preserve null semantics: empty array from null-state save → null
        if (state != null && state.length == 0) {
            state = null;
        }
        return Optional.of(new CheckpointEntry(taskId, state, clock.instant()));
    }

    public boolean hasCheckpoint(String taskId) {
        return store.hasCheckpoint(taskId);
    }

    public void clearCheckpoints(String taskId) {
        store.clearCheckpoint(taskId);
        LOG.debug("Checkpoints cleared for task {}", taskId);
    }
}
