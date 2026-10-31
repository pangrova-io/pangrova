package org.pangrova.coordination.api;

import org.pangrova.common.api.annotation.Internal;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * SPI for pluggable coordinator registration and discovery.
 *
 * <p>Maintains the cluster-wide coordinator topology. Peer coordinators
 * and clients use this registry to discover the current leader endpoint,
 * eliminating the need for hardcoded addresses. Combined with
 * {@link org.pangrova.coordination.api.DistributedLease} for leader
 * election, this registry makes the election result visible to all
 * participants.</p>
 *
 * <p>Implementations must be thread-safe. Concurrent calls from different
 * threads or nodes must be serialized by the backend (e.g., database
 * row-level locking, CAS on in-memory state).</p>
 *
 * <p>Use cases: coordinator self-registration, leader discovery, role
 * transition tracking, stale coordinator expiration.</p>
 */
@Internal
public interface CoordinatorRegistry {

    /**
     * Registers a coordinator or updates an existing registration.
     *
     * <p>This operation is idempotent: calling it multiple times with the
     * same {@code coordinatorId} overwrites the previous snapshot (upsert
     * semantics). The latest snapshot wins.</p>
     *
     * @param coordinator the coordinator snapshot to register (must not be null)
     */
    void register(CoordinatorSnapshot coordinator);

    /**
     * Refreshes the heartbeat timestamp for a registered coordinator.
     *
     * <p>Only the last-heartbeat timestamp is updated; identity fields
     * (host, port, role, epoch) remain unchanged.</p>
     *
     * <p>If the given {@code coordinatorId} is not currently registered,
     * the behavior is implementation-defined (implementations may silently
     * ignore or throw).</p>
     *
     * @param coordinatorId identifier of the coordinator to update
     */
    void heartbeat(String coordinatorId);

    /**
     * Removes a coordinator from the registry.
     *
     * <p>This operation is idempotent: unregistering an unknown or
     * already-removed {@code coordinatorId} is a no-op.</p>
     *
     * @param coordinatorId identifier of the coordinator to remove
     */
    void unregister(String coordinatorId);

    /**
     * Updates the HA role for a registered coordinator.
     *
     * <p>This method is called from leader-election callbacks:
     * {@code updateRole(id, CoordinatorRole.LEADER)} on promotion,
     * {@code updateRole(id, CoordinatorRole.STANDBY)} on demotion.</p>
     *
     * <p>If the given {@code coordinatorId} is not currently registered,
     * the behavior is implementation-defined (implementations may silently
     * ignore or throw).</p>
     *
     * @param coordinatorId identifier of the coordinator to update
     * @param role          the new role (must not be null)
     */
    void updateRole(String coordinatorId, CoordinatorRole role);

    /**
     * Returns the current active leader, if one exists.
     *
     * <p>A coordinator is considered the active leader if its role is
     * {@code "LEADER"} and its last heartbeat is not stale. Returns
     * empty if no coordinator satisfies both conditions.</p>
     *
     * @return the leader snapshot, or empty if no active leader exists
     */
    Optional<CoordinatorSnapshot> getLeader();

    /**
     * Returns all coordinators whose last heartbeat is within the given
     * staleness threshold.
     *
     * <p>A coordinator is considered active if
     * {@code System.currentTimeMillis() - lastHeartbeatMs < staleThreshold}.</p>
     *
     * @param staleThreshold maximum age of the last heartbeat for a
     *                       coordinator to be considered active
     * @return an unmodifiable list of active coordinator snapshots; never null
     */
    List<CoordinatorSnapshot> getAllCoordinators(Duration staleThreshold);

    /**
     * Returns the snapshot for a specific coordinator, if registered.
     *
     * @param coordinatorId identifier of the coordinator to look up
     * @return the coordinator snapshot, or empty if not registered
     */
    Optional<CoordinatorSnapshot> getCoordinator(String coordinatorId);

    /**
     * Removes all coordinators whose last heartbeat is older than the
     * given threshold.
     *
     * <p>Implementations should call this periodically to prevent
     * unbounded growth of stale entries.</p>
     *
     * @param threshold maximum age of the last heartbeat; coordinators
     *                  older than this are expired and removed
     * @return the number of stale entries removed
     */
    int expireStale(Duration threshold);

    /**
     * Updates the last processed invalidation log ID for a coordinator.
     *
     * <p>Called periodically by {@code JdbcInvalidationPoller} after processing
     * invalidation entries. Other coordinators read this value via
     * {@link CoordinatorSnapshot#lastProcessedInvalidationId()} to determine
     * whether DDL changes have propagated cluster-wide.</p>
     *
     * <p>Default implementation is a no-op for backward compatibility with
     * registries that don't support invalidation tracking.</p>
     *
     * @param coordinatorId identifier of the coordinator
     * @param lastProcessedId highest invalidation_log ID processed
     */
    default void updateLastProcessedInvalidationId(String coordinatorId, long lastProcessedId) {
        // No-op default for backward compatibility
    }
}
