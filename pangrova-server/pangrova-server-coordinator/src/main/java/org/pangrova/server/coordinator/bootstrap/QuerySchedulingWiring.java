package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.scheduler.core.resource.WeightedFairScheduler;
import org.pangrova.scheduler.builtin.placement.LoadBalancedStrategyFactory;
import org.pangrova.scheduler.spi.placement.WorkerSelectionStrategyFactory;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.scheduler.spi.placement.TenantPlacementResolver;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;
import org.pangrova.security.api.tenant.TenantStatus;

/**
 * Assembles placement through its typed factory and owns the optional tenant executor.
 * The executor is independent of placement ranking; its presence is not evidence of
 * distributed tenant fairness. Production query admission owns separate quota checks.
 */
final class QuerySchedulingWiring implements AutoCloseable {
    private final WeightedFairScheduler tenantScheduler;
    private final TaskScheduler scheduler;

    QuerySchedulingWiring(TenantStore tenantStore) {
        this(tenantStore, new LoadBalancedStrategyFactory());
    }

    QuerySchedulingWiring(TenantStore tenantStore, WorkerSelectionStrategyFactory factory) {
        Objects.requireNonNull(factory, "factory");
        String providerId = Objects.requireNonNull(factory.providerId(), "providerId");
        if (providerId.isBlank()) throw new IllegalArgumentException("providerId must not be blank");
        if (factory.apiVersion() != WorkerSelectionStrategyFactory.API_VERSION) {
            throw new IllegalArgumentException("Unsupported placement API version for " + providerId);
        }
        tenantScheduler = tenantStore != null ? new WeightedFairScheduler() : null;
        try {
            scheduler = new TaskScheduler(Objects.requireNonNull(factory.create(), "strategy"), tenantPlacement(tenantStore));
        } catch (RuntimeException | Error failure) {
            CoordinatorResourceRelease.close(failure,
                    CoordinatorResourceRelease.present(tenantScheduler, WeightedFairScheduler::close));
            throw failure;
        }
    }

    static TenantPlacementResolver tenantPlacement(TenantStore store) {
        if (store == null) return tenantId -> Optional.empty(); // Explicit shared-only deployment.
        return tenantId -> {
            if (tenantId == null || tenantId.isBlank()) {
                throw new NoAvailableWorkerException("Tenant identity is required for placement");
            }
            var tenant = store.getTenant(tenantId)
                    .orElseThrow(() -> new NoAvailableWorkerException("Unknown tenant for placement: " + tenantId));
            if (tenant.status() != TenantStatus.ACTIVE) {
                throw new NoAvailableWorkerException("Inactive tenant for placement: " + tenantId);
            }
            return Optional.ofNullable(tenant.resourcePool());
        };
    }

    TaskScheduler scheduler() { return scheduler; }
    WeightedFairScheduler tenantScheduler() { return tenantScheduler; }

    @Override
    public void close() {
        if (tenantScheduler != null) tenantScheduler.close();
    }
}
