package org.pangrova.worker.bootstrap.lifecycle;

import org.pangrova.common.lifecycle.HealthCheckEndpoint;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Health conditions for Worker readiness checks.
 * Evaluates HTTP, result server, and native engine availability.
 */
public final class WorkerHealthConditions implements HealthCheckEndpoint.HealthConditions {

    private final AtomicBoolean started;
    private final WorkerComponentRefs refs;

    public WorkerHealthConditions(AtomicBoolean started, WorkerComponentRefs refs) {
        this.started = started;
        this.refs = refs;
    }

    @Override
    public boolean isHttpReady() {
        return refs.httpServer() != null && started.get();
    }

    @Override
    public boolean isTransportReady() {
        return refs.resultServerPort() > 0;
    }

    @Override
    public boolean isNativeReady() {
        return refs.nativeAvailable();
    }

    @Override
    public int resultServerPort() {
        return refs.resultServerPort();
    }

    @Override
    public boolean resultServerRunning() {
        return refs.resultServerPort() > 0;
    }

    @Override
    public int activeWorkerCount() {
        return 0;
    }

    @Override
    public int activeCatalogCount() {
        return 0;
    }
}
