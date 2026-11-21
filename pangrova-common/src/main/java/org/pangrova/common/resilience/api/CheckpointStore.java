package org.pangrova.common.resilience.api;

import org.pangrova.common.api.annotation.Internal;

import java.util.Optional;

/**
 * SPI for persistent checkpoint storage.
 * Implementations provide durable storage for task checkpoint state,
 * enabling recovery after coordinator/worker restarts.
 */
@Internal
public interface CheckpointStore {

    /**
     * Persists a checkpoint for the given component.
     *
     * @param componentId unique identifier (e.g. taskId)
     * @param state       serialized checkpoint state
     */
    void saveCheckpoint(String componentId, byte[] state);

    /**
     * Loads the latest checkpoint for the given component.
     *
     * @param componentId unique identifier
     * @return the checkpoint state bytes, or empty if none exists
     */
    Optional<byte[]> loadCheckpoint(String componentId);

    /**
     * Checks whether a checkpoint exists for the given component.
     *
     * @param componentId unique identifier
     * @return true if a checkpoint exists
     */
    boolean hasCheckpoint(String componentId);

    /**
     * Removes the checkpoint for the given component.
     *
     * @param componentId unique identifier
     */
    void clearCheckpoint(String componentId);
}
