package org.pangrova.worker.bootstrap;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.bootstrap.phase.WorkerObservabilitySetup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual local owner/exit barriers; no simulated task, function, or JNI execution. */
class WorkerBootstrapCleanupTest {
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
                assertThatThrownBy(() -> new WorkerBootstrapCleanup(refs).close(config, new WorkerObservabilitySetup(), false))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("scheduler drain interrupted")
                        .satisfies(failure -> assertThat(failure.getSuppressed()).singleElement()
                                .satisfies(other -> assertThat(other).hasMessageContaining("CDC consumer")));
            } finally { Thread.interrupted(); resume.countDown(); }
            assertThat(dependent.isOpen()).isTrue();
            assertThat(refs.nativeFunctionResources()).containsExactly(dependent);
            assertThat(refs.cdcConsumer()).isSameAs(cdc);
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            cdcBlocked.set(false);
            new WorkerBootstrapCleanup(refs).close(config, new WorkerObservabilitySetup(), false);
            assertThat(scheduler.isTerminated()).isTrue();
            assertThat(dependent.isOpen()).isFalse();
            assertThat(refs.nativeFunctionResources()).isEmpty();
            assertThat(refs.cdcConsumer()).isNull();
            assertThat(cdcAttempts).hasValue(2);
        } finally { resume.countDown(); scheduler.shutdownNow(); }
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
