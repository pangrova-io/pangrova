package org.pangrova.streaming.api.checkpoint;

import org.pangrova.common.exception.StaleEpochException;

import java.util.List;
import java.util.Optional;

/**
 * Persistent store for CDC ingestion stream state.
 *
 * <p>Implementations must be thread-safe. State is keyed by
 * {@link StreamState#streamName()}.</p>
 */
public interface StreamStateStore {

    /**
     * Creates one stream without replacing an existing name or incarnation.
     *
     * @return false when the name is already occupied
     */
    boolean createState(StreamState state);

    /**
     * Updates an existing stream only while its tenant, checkpoint identity and
     * epoch still match. A deleted stream must never be recreated by an update.
     *
     * @param state the stream state to persist
     */
    void saveState(StreamState state);

    /**
     * Load the state for a stream by name.
     *
     * @param streamName unique stream identifier
     * @return the state, or empty if not found
     */
    Optional<StreamState> loadState(String streamName);

    /**
     * List all streams belonging to the given tenant.
     *
     * @param tenantId tenant identifier
     * @return unmodifiable list of stream states for the tenant
     */
    List<StreamState> listByTenant(String tenantId);

    /**
     * List all streams in the given lifecycle state.
     *
     * @param lifecycleState the lifecycle state to filter by
     * @return unmodifiable list of matching stream states
     */
    List<StreamState> listByState(StreamLifecycleState lifecycleState);

    /**
     * List all stream states regardless of state or tenant.
     *
     * @return unmodifiable list of all stream states
     */
    List<StreamState> listAll();

    /**
     * Delete the matching tenant, incarnation and epoch, including its cached offsets.
     *
     * @param state identity and epoch observed by the caller
     * @return true if the entry existed and was removed
     */
    boolean delete(StreamState state);

    /**
     * Conditionally update the same tenant and checkpoint identity only if the stored epoch matches
     * {@code expectedEpoch}. This provides optimistic concurrency control
     * for HA epoch fencing during coordinator failover.
     *
     * @param state         the new state to persist
     * @param expectedEpoch the epoch value expected in the current stored row
     * @throws StaleEpochException if the stored epoch does not match
     */
    void saveStateIfEpoch(StreamState state, long expectedEpoch);
}
