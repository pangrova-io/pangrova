package org.pangrova.worker.bootstrap;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** One-shot admission state tests; no simulated Worker/JNI/business execution. */
class WorkerBootstrapLifecycleTest {
    @Test void concurrentStartsHaveExactlyOneAdmittedOwner() throws Exception {
        var lifecycle = new WorkerBootstrapLifecycle();
        var start = new CountDownLatch(1);
        var admitted = new AtomicInteger();
        var rejected = new AtomicInteger();
        var contenders = new ArrayList<CompletableFuture<Void>>();
        for (int index = 0; index < 16; index++) {
            contenders.add(CompletableFuture.runAsync(() -> {
                try { start.await(); }
                catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                try { lifecycle.beginStart(); admitted.incrementAndGet(); }
                catch (IllegalStateException expected) { rejected.incrementAndGet(); }
            }));
        }
        start.countDown();
        CompletableFuture.allOf(contenders.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
        assertThat(admitted).hasValue(1);
        assertThat(rejected).hasValue(15);
        assertThatThrownBy(() -> lifecycle.beginShutdown(new WorkerComponentRefs())).hasMessageContaining("STARTING");
        lifecycle.cleaned(true);
        assertThatThrownBy(lifecycle::beginStart).hasMessageContaining("STOPPED");
    }

    @Test void cleanupFailureSealsStartupUntilRetryAndNeverReusesStoppedInstance() {
        var lifecycle = new WorkerBootstrapLifecycle();
        lifecycle.beginStart();
        lifecycle.cleaned(false);
        assertThat(lifecycle.readyFlag()).isFalse();
        assertThatThrownBy(lifecycle::beginStart).hasMessageContaining("CLEANUP_PENDING");
        assertThat(lifecycle.beginShutdown(new WorkerComponentRefs())).isTrue();
        assertThatThrownBy(() -> lifecycle.beginShutdown(new WorkerComponentRefs())).hasMessageContaining("STOPPING");
        lifecycle.cleaned(true);
        assertThat(lifecycle.beginShutdown(new WorkerComponentRefs())).isFalse();
        assertThatThrownBy(lifecycle::beginStart).hasMessageContaining("STOPPED");
        assertThatThrownBy(lifecycle::beginConfiguration).hasMessageContaining("STOPPED");
    }

    @Test void configurationAndStartupCannotOverlap() {
        var lifecycle = new WorkerBootstrapLifecycle();
        lifecycle.beginConfiguration();
        assertThatThrownBy(lifecycle::beginStart).hasMessageContaining("CONFIGURING");
        assertThatThrownBy(() -> lifecycle.beginShutdown(new WorkerComponentRefs())).hasMessageContaining("CONFIGURING");
        lifecycle.configured();
        lifecycle.beginStart();
        assertThatThrownBy(lifecycle::beginConfiguration).hasMessageContaining("STARTING");
        lifecycle.cleaned(true);
    }

    @Test void shutdownBeforeStartupIsNotACompletedLifecycle() {
        var lifecycle = new WorkerBootstrapLifecycle();
        assertThat(lifecycle.beginShutdown(new WorkerComponentRefs())).isFalse();
        lifecycle.beginStart();
        lifecycle.started();
        assertThat(lifecycle.readyFlag()).isTrue();
        assertThat(lifecycle.beginShutdown(new WorkerComponentRefs())).isTrue();
        assertThat(lifecycle.readyFlag()).isFalse();
        lifecycle.cleaned(true);
    }
}
