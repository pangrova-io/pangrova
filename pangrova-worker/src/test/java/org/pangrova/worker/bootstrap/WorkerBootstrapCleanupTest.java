package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.bootstrap.phase.WorkerTelemetryInitializer;
import org.pangrova.worker.task.report.RemoteTaskStatusReporter;
import org.pangrova.testing.testkit.constants.TestObservability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual local owner/exit barriers; no simulated task, function, or JNI execution. */
class WorkerBootstrapCleanupTest {
    @Test void partialStartupRetainsStatusClientUntilItClosesSuccessfully() {
        var refs = new WorkerComponentRefs();
        var config = WorkerBootstrapConfig.withDefaults("partial-status-reporter");
        var attempts = new AtomicInteger();
        var reporter = new RemoteTaskStatusReporter("http://localhost:1", TestObservability.TRACING, TestObservability.METRICS, null) {
            @Override public void close() {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("reporter still draining");
                super.close();
            }
        };
        refs.retainStatusReporter(reporter);
        var observability = new WorkerTelemetryInitializer();
        assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config, observability))
                .hasMessageContaining("reporter still draining");
        assertThat(attempts).hasValue(1);
        new WorkerBootstrapCleanup(refs).close(config, observability);
        new WorkerBootstrapCleanup(refs).close(config, observability);
        assertThat(attempts).hasValue(2);
    }

    @Test void failedIndependentOwnersDoNotCloseTheirDependentFunctionResources() throws Exception {
        var refs = new WorkerComponentRefs();
        var config = WorkerBootstrapConfig.withDefaults("local-cleanup-boundary");
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        refs.setHeartbeatExecutor(scheduler);
        var cdcBlocked = new AtomicBoolean(true);
        var cdcAttempts = new AtomicInteger();
        AutoCloseable cdc = () -> {
            cdcAttempts.incrementAndGet();
            if (cdcBlocked.get()) throw new IOException("CDC close failed");
        };
        refs.setCdcConsumer(cdc);
        var pipe = Pipe.open();
        try (var dependent = pipe.source(); var sink = pipe.sink()) {
            refs.retainNativeFunctionResource(dependent);
            scheduler.execute(() -> holdActualOwner(entered, resume, exited));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config, new WorkerTelemetryInitializer()))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("CDC consumer");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(scheduler.isShutdown()).isFalse();
            } finally { Thread.interrupted(); resume.countDown(); }
            assertThat(dependent.isOpen()).isTrue();
            assertThat(refs.nativeFunctionResources()).containsExactly(dependent);
            assertThat(refs.cdcConsumer()).isSameAs(cdc);
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            cdcBlocked.set(false);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerTelemetryInitializer());
            assertThat(scheduler.isTerminated()).isTrue();
            assertThat(dependent.isOpen()).isFalse();
            assertThat(refs.nativeFunctionResources()).isEmpty();
            assertThat(refs.cdcConsumer()).isNull();
            assertThat(cdcAttempts).hasValue(2);
        } finally { resume.countDown(); scheduler.shutdownNow(); }
    }

    /**
     * Heartbeats must outlive the task drain. Stopping them first made the
     * Coordinator declare this worker dead after coordinator.worker.heartbeat-timeout
     * (default 30s, equal to the drain timeout), failing the very queries the drain
     * was finishing. Raising the K8s grace period alone could not fix that.
     */
    @Test void heartbeatSurvivesTaskDrainAndStopsBeforeDeregistration() throws Exception {
        var refs = new WorkerComponentRefs();
        var config = WorkerBootstrapConfig.withDefaults("drain-heartbeat");
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        refs.setHeartbeatExecutor(scheduler);
        var aliveDuringDrain = new AtomicBoolean(false);
        var drainObserved = new AtomicBoolean(false);
        var delegate = org.pangrova.worker.task.TaskManager.forHttpWiringOnly();
        try {
            var worker = new org.pangrova.worker.PangrovaWorker(
                    new org.pangrova.worker.WorkerComponents(delegate)) {
                @Override public void stop() {
                    aliveDuringDrain.set(!scheduler.isShutdown());
                    drainObserved.set(true);
                }
            };
            refs.setWorker(worker);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerTelemetryInitializer());
            assertThat(drainObserved).isTrue();
            assertThat(aliveDuringDrain).isTrue();
            assertThat(scheduler.isTerminated()).isTrue();
        } finally {
            delegate.close();
            scheduler.shutdownNow();
        }
    }

    private static void holdActualOwner(CountDownLatch entered, CountDownLatch resume, CountDownLatch exited) {
        boolean interrupted = false;
        entered.countDown();
        try {
            while (resume.getCount() != 0) {
                try { resume.await(); } catch (InterruptedException failure) { interrupted = true; }
            }
        } finally {
            exited.countDown();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
