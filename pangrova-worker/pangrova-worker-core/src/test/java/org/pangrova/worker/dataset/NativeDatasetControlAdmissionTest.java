package org.pangrova.worker.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class NativeDatasetControlAdmissionTest {
    @Test
    void admitsExactlyEightCommandsAndIdempotentReleaseRestoresOneSlot() {
        var admission = new NativeDatasetControlAdmission();
        var leases = new ArrayList<NativeDatasetControlAdmission.Lease>();
        try {
            for (int i = 0; i < NativeDatasetControlAdmission.MAX_CONCURRENT_COMMANDS; i++) {
                leases.add(admission.acquire());
            }
            assertThatThrownBy(admission::acquire)
                    .isInstanceOf(ControlAdmissionRejectedException.class);
            leases.get(0).close();
            leases.get(0).close();
            try (var replacement = admission.acquire()) {
                assertThat(replacement).isNotNull();
                assertThatThrownBy(admission::acquire)
                        .isInstanceOf(ControlAdmissionRejectedException.class);
            }
        } finally {
            leases.forEach(NativeDatasetControlAdmission.Lease::close);
            admission.close();
        }
        assertThatThrownBy(admission::acquire)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shutdownDrainsAnAdmittedCommandAndRejectsAllNewCommands() throws Exception {
        var admission = new NativeDatasetControlAdmission();
        var lease = admission.acquire();
        var executor =
                ManagedExecutorFactory.newSingleThreadExecutor("metadata-control-drain-test");
        var closeStarted = new CountDownLatch(1);
        try {
            var shutdown =
                    executor.submit(
                            () -> {
                                closeStarted.countDown();
                                admission.close();
                            });
            assertThat(closeStarted.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(shutdown.isDone()).isFalse();
            lease.close();
            shutdown.get(30, TimeUnit.SECONDS);
            assertThatThrownBy(admission::acquire).hasMessageContaining("closed");
            admission.close();
        } finally {
            lease.close();
            executor.shutdownNow();
            admission.close();
        }
    }

    @Test
    void interruptedCloseRetainsTheAdmittedOwnerAndRetryMustActuallyJoinIt() throws Exception {
        var admission = new NativeDatasetControlAdmission();
        var lease = admission.acquire();
        var firstFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var firstCloser =
                new Thread(
                        () -> {
                            try {
                                admission.close();
                            } catch (Throwable failure) {
                                firstFailure.set(failure);
                            }
                        },
                        "actual-control-interrupted-closer");
        var retryCloser = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var executor =
                ManagedExecutorFactory.newSingleThreadExecutor("actual-control-retry-closer");
        try {
            firstCloser.start();
            org.awaitility.Awaitility.await()
                    .atMost(java.time.Duration.ofSeconds(5))
                    .until(() -> firstCloser.getState() == Thread.State.TIMED_WAITING);
            firstCloser.interrupt();
            firstCloser.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(firstCloser.isAlive()).isFalse();
            assertThat(firstFailure.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Interrupted draining");
            assertThatThrownBy(admission::acquire).hasMessageContaining("closed");
            var retry =
                    executor.submit(
                            () -> {
                                retryCloser.set(Thread.currentThread());
                                admission.close();
                            });
            org.awaitility.Awaitility.await()
                    .atMost(java.time.Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> {
                                assertThat(retry.isDone()).isFalse();
                                assertThat(retryCloser.get()).isNotNull();
                                assertThat(retryCloser.get().getState())
                                        .isEqualTo(Thread.State.TIMED_WAITING);
                            });
            assertThat(retry.isDone()).isFalse();
            lease.close();
            retry.get(5, TimeUnit.SECONDS);
            admission.close();
            assertThatThrownBy(admission::acquire).hasMessageContaining("closed");
        } finally {
            lease.close();
            firstCloser.interrupt();
            firstCloser.join(TimeUnit.SECONDS.toMillis(5));
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            admission.close();
        }
    }
}
