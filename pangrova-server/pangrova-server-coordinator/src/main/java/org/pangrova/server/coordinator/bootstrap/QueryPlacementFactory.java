package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.scheduler.api.placement.TenantPlacementResolver;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategyFactory;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.security.api.enums.TenantStatus;

import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

/** Selects the configured placement strategy and binds tenant eligibility. */
final class QueryPlacementFactory {
    private QueryPlacementFactory() {}

    static TaskScheduler create(TenantStore tenantStore, String providerId) {
        return create(tenantStore, providerId, ServiceLoader.load(WorkerSelectionStrategyFactory.class,
                QueryPlacementFactory.class.getClassLoader()));
    }

    static TaskScheduler create(TenantStore tenantStore, String providerId,
            Iterable<WorkerSelectionStrategyFactory> factories) {
        var installed = new HashMap<String, WorkerSelectionStrategyFactory>();
        for (var candidate : factories) {
            String id = candidate.providerId();
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("Placement factory providerId must not be blank");
            }
            if (candidate.apiVersion() != WorkerSelectionStrategyFactory.API_VERSION) {
                throw new IllegalArgumentException("Unsupported placement API version for " + id);
            }
            if (installed.putIfAbsent(id, candidate) != null) {
                throw new IllegalArgumentException("Duplicate placement provider: " + id);
            }
        }
        var factory = installed.get(providerId);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown placement provider: " + providerId
                    + ". Installed: " + installed.keySet());
        }
        return new TaskScheduler(Objects.requireNonNull(factory.create(), "strategy"), tenantPlacement(tenantStore));
    }

    static TenantPlacementResolver tenantPlacement(TenantStore store) {
        if (store == null) return tenantId -> Optional.empty(); // Explicit shared-only deployment.
        return tenantId -> {
            if (tenantId == null || tenantId.isBlank()) {
                throw new NoAvailableWorkerException("Tenant identity is required for placement");
            }
            var placement = store.getPlacement(tenantId)
                    .orElseThrow(() -> new NoAvailableWorkerException("Unknown tenant for placement: " + tenantId));
            if (placement.status() != TenantStatus.ACTIVE) {
                throw new NoAvailableWorkerException("Inactive tenant for placement: " + tenantId);
            }
            return Optional.ofNullable(placement.resourcePool());
        };
    }
}
