package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.protocol.transport.TransportServerLifecycle;

/**
 * Health conditions implementation for Coordinator readiness checks.
 * Evaluates HTTP, transport, native, worker, and catalog readiness.
 */
final class CoordinatorHealthConditions implements HealthCheckEndpoint.HealthConditions {

    private final CoordinatorBootstrap bootstrap;

    CoordinatorHealthConditions(CoordinatorBootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public boolean isHttpReady() {
        return bootstrap.isStarted();
    }

    @Override
    public boolean isTransportReady() {
        TransportServerLifecycle server = bootstrap.getCoordinatorTransportServer();
        return server != null && server.isRunning();
    }

    @Override
    public boolean isNativeReady() {
        return false;
    }

    @Override
    public int activeWorkerCount() {
        WorkerManager wm = bootstrap.getWorkerManager();
        return wm != null ? wm.getActiveWorkers().size() : 0;
    }

    @Override
    public int activeCatalogCount() {
        MetadataBootstrap mb = bootstrap.getMetadataBootstrap();
        return mb != null ? mb.catalogRegistry().listCatalogNames().size() : 0;
    }
}
