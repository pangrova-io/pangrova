package org.pangrova.coordinator.resilience.checkpoint;

import org.pangrova.common.resilience.api.CheckpointStore;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory checkpoint store for testing and single-node deployments.
 * State is lost on process restart.
 */
public final class InMemoryCheckpointStore implements CheckpointStore {

    /** Sentinel stored when the caller passes null state. */
    private static final byte[] NULL_MARKER = new byte[0];

    private final ConcurrentHashMap<String, byte[]> store = new ConcurrentHashMap<>();

    @Override
    public void saveCheckpoint(String componentId, byte[] state) {
        store.put(componentId, state != null ? Arrays.copyOf(state, state.length) : NULL_MARKER);
    }

    @Override
    public Optional<byte[]> loadCheckpoint(String componentId) {
        byte[] data = store.get(componentId);
        if (data == null) {
            return Optional.empty();
        }
        // Sentinel identity check — null marker means caller stored null
        if (data.length == 0) {
            return Optional.of(data);
        }
        return Optional.of(Arrays.copyOf(data, data.length));
    }

    @Override
    public boolean hasCheckpoint(String componentId) {
        return store.containsKey(componentId);
    }

    @Override
    public void clearCheckpoint(String componentId) {
        store.remove(componentId);
    }
}
