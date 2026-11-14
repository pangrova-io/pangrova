package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;

import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.pangrova.metadata.api.cache.InvalidationProgressRegistry;

/** Adapts the coordinator registry to Metadata's invalidation-progress port. */
final class CoordinatorInvalidationProgress implements InvalidationProgressRegistry {

    /** Maximum age of a query coordinator heartbeat included in cleanup decisions. */
    static final Duration DEFAULT_LIVENESS_WINDOW = Duration.ofMinutes(5);

    private final CoordinatorRegistry registry;
    private final Duration livenessWindow;

    CoordinatorInvalidationProgress(
            CoordinatorRegistry registry, Duration livenessWindow) {
        if (livenessWindow == null || livenessWindow.isZero() || livenessWindow.isNegative()) {
            throw new IllegalArgumentException("livenessWindow must be positive");
        }
        this.registry = Objects.requireNonNull(registry, "registry");
        this.livenessWindow = livenessWindow;
    }

    static PostgresInvalidationPoller createPoller(
            DataSource source, MetadataCache cache,
            CoordinatorRegistry registry, CoordinatorDeploymentRole role, String coordinatorId) {
        var progress = new CoordinatorInvalidationProgress(registry, DEFAULT_LIVENESS_WINDOW);
        return new PostgresInvalidationPoller(source, cache, progress,
                role.isQuery() ? Optional.of(coordinatorId) : Optional.empty());
    }

    @Override
    public void updateLastProcessedInvalidationId(String coordinatorId, long invalidationId) {
        registry.updateLastProcessedInvalidationId(coordinatorId, invalidationId);
    }

    @Override
    public long lastProcessedInvalidationId(String coordinatorId) {
        return registry.getCoordinator(coordinatorId)
                .map(CoordinatorSnapshot::lastProcessedInvalidationId)
                .orElse(-1L);
    }

    @Override
    public long minimumReportedInvalidationId() {
        long floor = Long.MAX_VALUE;
        // Only LIVE Coordinators may pin the cleanup floor. Asking for "every
        // Coordinator ever registered" computed a cutoff far in the past, so a
        // long-dead Coordinator's stale watermark (often 0) blocked cleanup
        // permanently. Expiry of dead rows stays owned by registry maintenance.
        for (CoordinatorSnapshot coordinator : registry.getAllCoordinators(livenessWindow)) {
            floor = Math.min(floor, coordinator.lastProcessedInvalidationId());
        }
        return floor;
    }
}
