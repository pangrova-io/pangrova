package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.lifecycle.ResourceCloser;

import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.resource.WeightedFairScheduler;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategyFactory;
import java.util.HashMap;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Optional;
import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.scheduler.api.placement.TenantPlacementResolver;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;
import org.pangrova.security.api.enums.TenantStatus;

/**
 * Assembles placement through its typed factory and owns the optional tenant executor.
 * The executor is independent of placement ranking; its presence is not evidence of
 * distributed tenant fairness. Production query admission owns separate quota checks.
 */
final class QuerySchedulingRuntime implements AutoCloseable {
    private final WeightedFairScheduler tenantScheduler;
    private final TaskScheduler scheduler;

    QuerySchedulingRuntime(TenantStore tenantStore, String providerId) {
        this(tenantStore, providerId, ServiceLoader.load(WorkerSelectionStrategyFactory.class,
                QuerySchedulingRuntime.class.getClassLoader()));
    }

    QuerySchedulingRuntime(TenantStore tenantStore, String providerId,
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
        tenantScheduler = tenantStore != null ? new WeightedFairScheduler() : null;
        try {
            scheduler = new TaskScheduler(Objects.requireNonNull(factory.create(), "strategy"),
                    tenantPlacement(tenantStore));
        } catch (RuntimeException | Error failure) {
            ResourceCloser.closeAll(failure,
                    ResourceCloser.present(tenantScheduler, WeightedFairScheduler::close));
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
