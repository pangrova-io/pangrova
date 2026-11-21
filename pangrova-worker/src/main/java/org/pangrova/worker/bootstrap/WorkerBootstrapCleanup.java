package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;

import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import org.pangrova.worker.bootstrap.lifecycle.WorkerCleanupSteps;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.bootstrap.phase.WorkerHeartbeatBootstrap;
import org.pangrova.worker.bootstrap.phase.WorkerHttpBootstrap;
import org.pangrova.worker.bootstrap.phase.WorkerObservabilitySetup;
import org.pangrova.worker.bootstrap.phase.WorkerResourceBootstrap;

/** One dependency-ordered cleanup path for shutdown and failed startup. */
final class WorkerBootstrapCleanup {
    private final WorkerComponentRefs refs;
    private final WorkerCleanupSteps steps = new WorkerCleanupSteps();

    WorkerBootstrapCleanup(WorkerComponentRefs refs) {
        this.refs = refs;
    }

    void close(
            WorkerBootstrapConfig config,
            WorkerObservabilitySetup observability) {
        steps.attempt(this::closeCdc);
        // Stop accepting work first, but keep heartbeating through the drain.
        // Stopping the heartbeat before draining made the Coordinator declare
        // this worker dead once coordinator.worker.heartbeat-timeout elapsed,
        // which failed every in-flight query the drain was trying to finish.
        steps.attempt(() -> WorkerHttpBootstrap.stopHttp(refs));
        steps.attempt(() -> WorkerShutdownSequence.drainTasks(refs.worker(), refs.taskManager()));
        steps.attempt(this::closeTasks);
        steps.requireSuccess();
        steps.attempt(this::closeHeartbeat);
        steps.attempt(() -> WorkerHeartbeatBootstrap.deregister(refs, config.workerId()));
        steps.requireSuccess();
        steps.attempt(this::closeRegistrationClient);
        steps.requireSuccess();
        steps.attempt(this::closeWriter);
        steps.attempt(() -> NativeFunctionBootstrap.close(refs));
        steps.requireSuccess();
        WorkerResourceBootstrap.releaseResources(refs, observability);
        if (refs.admissionIncarnation() != null) {
            refs.admissionIncarnation().closeAfterDrain(() -> {});
            refs.setAdmissionIncarnation(null);
        }
    }

    private void closeHeartbeat() {
        WorkerHeartbeatBootstrap.stopHeartbeat(refs);
    }

    private void closeRegistrationClient() {
        var client = refs.registrationClient();
        if (client == null) {
            return;
        }
        client.close();
        refs.setRegistrationClient(null);
    }

    private void closeTasks() {
        if (refs.taskOwnership() != null) {
            refs.taskOwnership().close();
        } else if (refs.taskManager() != null) {
            refs.taskManager().close();
        }
        // Task shutdown can still publish a terminal report. Retain its client if drain fails.
        if (refs.statusReporter() != null) {
            refs.statusReporter().close();
            refs.setStatusReporter(null);
        }
    }

    private void closeCdc() {
        var consumer = refs.cdcConsumer();
        if (consumer == null) {
            return;
        }
        try {
            consumer.close();
        } catch (Exception closeFailure) {
            throw new IllegalStateException("Failed to close CDC consumer", closeFailure);
        }
        refs.setCdcConsumer(null);
    }

    private void closeWriter() {
        var writer = refs.nativeWriteExecutor();
        if (writer != null) {
            writer.close();
            refs.setNativeWriteExecutor(null);
            refs.setCatalogResourceResolver(null);
        } else if (refs.catalogResourceResolver() instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception closeFailure) {
                throw new IllegalStateException(
                        "Failed to close Worker catalog runtime", closeFailure);
            }
            refs.setCatalogResourceResolver(null);
        }
    }
}
