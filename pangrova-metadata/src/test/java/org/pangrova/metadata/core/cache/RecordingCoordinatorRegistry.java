package org.pangrova.metadata.core.cache;

import org.pangrova.spi.coordination.CoordinatorRegistry;
import org.pangrova.spi.coordination.CoordinatorRole;
import org.pangrova.spi.coordination.CoordinatorSnapshot;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link CoordinatorRegistry} for poller tests.
 *
 * <p>Records registered coordinators and their last-processed invalidation IDs so
 * {@link JdbcInvalidationPoller} cleanup fences can be asserted without depending on
 * the coordinator module's JDBC registry implementation.</p>
 */
final class RecordingCoordinatorRegistry implements CoordinatorRegistry {

    private final Map<String, CoordinatorSnapshot> coordinators = new ConcurrentHashMap<>();

    @Override
    public void register(CoordinatorSnapshot coordinator) {
        coordinators.put(coordinator.coordinatorId(), coordinator);
    }

    @Override
    public void heartbeat(String coordinatorId) {
        // Last heartbeat is irrelevant for poller cleanup tests.
    }

    @Override
    public void unregister(String coordinatorId) {
        coordinators.remove(coordinatorId);
    }

    @Override
    public void updateRole(String coordinatorId, CoordinatorRole role) {
        // Role transitions are not exercised by the poller.
    }

    @Override
    public Optional<CoordinatorSnapshot> getLeader() {
        return Optional.empty();
    }

    @Override
    public List<CoordinatorSnapshot> getAllCoordinators(Duration staleThreshold) {
        return List.copyOf(coordinators.values());
    }

    @Override
    public Optional<CoordinatorSnapshot> getCoordinator(String coordinatorId) {
        return Optional.ofNullable(coordinators.get(coordinatorId));
    }

    @Override
    public int expireStale(Duration threshold) {
        return 0;
    }

    @Override
    public void updateLastProcessedInvalidationId(String coordinatorId, long lastProcessedId) {
        CoordinatorSnapshot current = coordinators.get(coordinatorId);
        if (current != null) {
            coordinators.put(coordinatorId, new CoordinatorSnapshot(current.coordinatorId(), current.host(),
                    current.httpPort(), current.role(), current.epoch(), current.lastHeartbeatMs(),
                    current.registeredAtMs(), lastProcessedId));
        }
    }
}
