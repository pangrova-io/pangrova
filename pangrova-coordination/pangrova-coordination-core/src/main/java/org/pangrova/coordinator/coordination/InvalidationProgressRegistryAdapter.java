package org.pangrova.coordinator.coordination;

import java.time.Duration;

import org.pangrova.persistence.api.coordination.CoordinatorRegistry;
import org.pangrova.spi.coordination.CoordinatorSnapshot;
import org.pangrova.metadata.api.cache.InvalidationProgressRegistry;

/** Adapts the coordinator registry to Metadata's invalidation-progress port. */
public final class InvalidationProgressRegistryAdapter implements InvalidationProgressRegistry {

    private final CoordinatorRegistry registry;

    public InvalidationProgressRegistryAdapter(CoordinatorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void updateLastProcessedInvalidationId(String coordinatorId, long invalidationId) {
        registry.updateLastProcessedInvalidationId(coordinatorId, invalidationId);
    }

    @Override
    public long minimumReportedInvalidationId() {
        long floor = Long.MAX_VALUE;
        // Expiration is owned by registry maintenance; never delete past a registered coordinator.
        for (CoordinatorSnapshot coordinator : registry.getAllCoordinators(Duration.ofMillis(Long.MAX_VALUE))) {
            floor = Math.min(floor, coordinator.lastProcessedInvalidationId());
        }
        return floor;
    }
}
