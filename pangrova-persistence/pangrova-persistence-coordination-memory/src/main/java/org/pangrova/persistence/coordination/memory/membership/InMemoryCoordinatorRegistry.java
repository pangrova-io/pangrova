package org.pangrova.persistence.coordination.memory.membership;

import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.coordination.api.enums.CoordinatorRole;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory coordinator registry for single-node deployments and testing.
 *
 * <p>Uses a {@link ConcurrentHashMap} for thread-safe storage. Basic
 * operations (put, remove, computeIfPresent) are atomic without external
 * synchronization. This implementation is <b>not</b> suitable for
 * multi-node clusters — use a JDBC-backed implementation for HA
 * deployments where failover topology recovery is required.</p>
 *
 * <p>Follows the same concurrency pattern as
 * {@link InMemoryWorkerRegistry}.</p>
 */
public final class InMemoryCoordinatorRegistry implements CoordinatorRegistry {

    private static final String LEADER_ROLE = "LEADER";

    private final ConcurrentHashMap<String, CoordinatorSnapshot> coordinators = new ConcurrentHashMap<>();

    @Override
    public void register(CoordinatorSnapshot coordinator) {
        Objects.requireNonNull(coordinator, "coordinator must not be null");
        coordinators.merge(coordinator.coordinatorId(), coordinator, (current, registration) ->
                registration.withInvalidationProgress(current.lastProcessedInvalidationId()));
    }

    @Override
    public void heartbeat(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        coordinators.computeIfPresent(coordinatorId, (id, existing) -> new CoordinatorSnapshot(
                existing.coordinatorId(),
                existing.host(),
                existing.httpPort(),
                existing.role(),
                existing.epoch(),
                System.currentTimeMillis(),
                existing.registeredAtMs(),
                existing.lastProcessedInvalidationId(),
                existing.incarnation()
        ));
    }

    @Override
    public void unregister(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        coordinators.remove(coordinatorId);
    }

    @Override
    public void updateRole(String coordinatorId, CoordinatorRole role) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        coordinators.computeIfPresent(coordinatorId, (id, existing) -> new CoordinatorSnapshot(
                existing.coordinatorId(),
                existing.host(),
                existing.httpPort(),
                role.name(),
                existing.epoch(),
                existing.lastHeartbeatMs(),
                existing.registeredAtMs(),
                existing.lastProcessedInvalidationId(),
                existing.incarnation()
        ));
    }

    @Override
    public void updateLastProcessedInvalidationId(String coordinatorId, long lastProcessedId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        coordinators.computeIfPresent(coordinatorId, (id, existing) -> existing.withInvalidationProgress(
                lastProcessedId));
    }

    @Override
    public Optional<CoordinatorSnapshot> getLeader() {
        return coordinators.values().stream()
                .filter(c -> LEADER_ROLE.equals(c.role()))
                .findFirst();
    }

    @Override
    public List<CoordinatorSnapshot> getAllCoordinators(Duration staleThreshold) {
        Objects.requireNonNull(staleThreshold, "staleThreshold must not be null");
        long now = System.currentTimeMillis();
        long thresholdMs = staleThreshold.toMillis();
        List<CoordinatorSnapshot> active = coordinators.values().stream()
                .filter(c -> now - c.lastHeartbeatMs() < thresholdMs)
                .toList();
        return List.copyOf(active);
    }

    @Override
    public Optional<CoordinatorSnapshot> getCoordinator(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        return Optional.ofNullable(coordinators.get(coordinatorId));
    }

    @Override
    public int expireStale(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold must not be null");
        long now = System.currentTimeMillis();
        long thresholdMs = threshold.toMillis();
        AtomicInteger count = new AtomicInteger(0);
        coordinators.forEach((id, coordinator) -> {
            if (now - coordinator.lastHeartbeatMs() >= thresholdMs) {
                expireIfStale(id, now, thresholdMs, count);
            }
        });
        return count.get();
    }

    private void expireIfStale(String id, long now, long thresholdMs, AtomicInteger count) {
        coordinators.computeIfPresent(id, (k, current) -> {
            if (now - current.lastHeartbeatMs() >= thresholdMs) {
                count.incrementAndGet();
                return null;
            }
            return current;
        });
    }

    /** Returns the number of registered coordinators (for testing). */
    public int coordinatorCount() {
        return coordinators.size();
    }
}
