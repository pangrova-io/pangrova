package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * SPI for pluggable worker registration and discovery.
 *
 * <p>Maintains the cluster-wide worker topology. Coordinators use this
 * registry to persist worker state so that a new leader can recover the
 * full topology immediately after failover, without waiting for heartbeat
 * cycles to rebuild it.</p>
 *
 * <p>Implementations must be thread-safe. Concurrent calls from different
 * threads or nodes must be serialized by the backend (e.g., database
 * row-level locking, CAS on in-memory state).</p>
 *
 * <p>Use cases: worker registration, heartbeat persistence, stale worker
 * expiration, failover topology recovery.</p>
 */
@Stable(since = "1.0")
public interface WorkerRegistry {

    /**
     * Registers a worker or updates an existing registration.
     *
     * <p>This operation is idempotent: calling it multiple times with the
     * same {@code workerId} overwrites the previous snapshot (upsert
     * semantics). The latest snapshot wins.</p>
     *
     * @param worker the worker snapshot to register (must not be null)
     */
    void register(WorkerSnapshot worker);

    /**
     * Updates the dynamic runtime metrics for a registered worker.
     *
     * <p>Only the fields carried by {@link HeartbeatSnapshot} and the
     * last-heartbeat timestamp are updated; static identity fields
     * (host, ports, capabilities) remain unchanged.</p>
     *
     * <p>If the given {@code workerId} is not currently registered, the
     * behavior is implementation-defined (implementations may silently
     * ignore or throw).</p>
     *
     * @param workerId identifier of the worker to update
     * @param snapshot the heartbeat payload (must not be null)
     */
    void heartbeat(String workerId, HeartbeatSnapshot snapshot);

    /**
     * Removes a worker from the registry.
     *
     * <p>This operation is idempotent: unregistering an unknown or
     * already-removed {@code workerId} is a no-op.</p>
     *
     * @param workerId identifier of the worker to remove
     */
    void unregister(String workerId);

    /**
     * Returns all workers whose last heartbeat is within the given
     * staleness threshold.
     *
     * <p>A worker is considered active if
     * {@code System.currentTimeMillis() - lastHeartbeatMs < staleThreshold}.</p>
     *
     * @param staleThreshold maximum age of the last heartbeat for a
     *                       worker to be considered active
     * @return an unmodifiable list of active worker snapshots; never null
     */
    List<WorkerSnapshot> getActiveWorkers(Duration staleThreshold);

    /**
     * Returns the snapshot for a specific worker, if registered.
     *
     * @param workerId identifier of the worker to look up
     * @return the worker snapshot, or empty if not registered
     */
    Optional<WorkerSnapshot> getWorker(String workerId);

    /**
     * Removes all workers whose last heartbeat is older than the given
     * threshold.
     *
     * <p>Implementations should call this periodically to prevent
     * unbounded growth of stale entries.</p>
     *
     * @param threshold maximum age of the last heartbeat; workers older
     *                  than this are expired and removed
     * @return the number of stale entries removed
     */
    int expireStale(Duration threshold);
}
