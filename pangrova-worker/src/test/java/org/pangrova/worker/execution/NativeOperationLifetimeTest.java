package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual Runnable exit/ownership barriers only; these tests do not execute Native tasks. */
class NativeOperationLifetimeTest {
    @Test
    void exclusiveCleanupCannotPassAnEnteredOperation() throws Exception {
        var lifetime = new NativeOperationLifetime();
        var lease = lifetime.enter();
        var enteredCleanup = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        var cleaner = new Thread(() -> {
            try (var claim = lifetime.claim(true)) {
                claim.awaitReaders();
                enteredCleanup.countDown();
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                exited.countDown();
            }
        }, "actual-native-operation-cleanup");
        cleaner.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> lease.blocks(cleaner));
            assertThat(enteredCleanup.getCount()).isEqualTo(1);
            assertThatThrownBy(lifetime::enter).isInstanceOf(IllegalStateException.class);
        } finally {
            lease.close();
            lease.close();
        }
        assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
        cleaner.join(5_000);
        assertThat(cleaner.isAlive()).isFalse();
        assertThat(enteredCleanup.getCount()).isZero();
        assertThat(failure.get()).isNull();
    }

    @Test
    void failedExclusiveOperationExitsAndCanBeRetriedWithoutReopeningReads() {
        var lifetime = new NativeOperationLifetime();
        var actualFailure = new IllegalStateException("actual lifecycle operation failed");
        assertThatThrownBy(() -> {
            try (var claim = lifetime.claim(true)) {
                claim.awaitReaders();
                throw actualFailure;
            }
        }).isSameAs(actualFailure);
        assertThatThrownBy(lifetime::enter).isInstanceOf(IllegalStateException.class);
        try (var retry = lifetime.claim(true)) {
            retry.awaitReaders();
        }
        lifetime.sealAndAwait();
    }

    @Test
    void recoveryClaimIsExclusiveButDoesNotPermanentlySealOperationAdmission() {
        var lifetime = new NativeOperationLifetime();
        try (var claim = lifetime.claim(false)) {
            claim.awaitReaders();
            assertThatThrownBy(lifetime::enter).isInstanceOf(IllegalStateException.class);
        }
        try (var lease = lifetime.enter()) {
            assertThat(lease.blocks(Thread.currentThread())).isFalse();
        }
        lifetime.sealAndAwait();
    }

    @Test
    void selfJoinIsRejectedWithoutChangingEnteredOwnerOrCreatingAnExclusiveClaim() {
        var lifetime = new NativeOperationLifetime();
        try (var lease = lifetime.enter()) {
            assertThat(lease.blocks(Thread.currentThread())).isFalse();
            assertThatThrownBy(() -> lifetime.claim(true)).hasMessageContaining("own entered thread");
            assertThatThrownBy(lifetime::sealAndAwait).hasMessageContaining("own entered thread");
        }
        try (var claim = lifetime.claim(false)) {
            claim.awaitReaders();
            assertThatThrownBy(() -> lifetime.claim(true)).hasMessageContaining("own entered thread");
            assertThatThrownBy(lifetime::sealAndAwait).hasMessageContaining("own entered thread");
        }
        try (var lease = lifetime.enter()) {
            assertThat(lease.blocks(Thread.currentThread())).isFalse();
        }
    }

    @Test
    void globalSealWaitsForActualLastExitAndNeverUsesCancellationAsExit() throws Exception {
        var lifetime = new NativeOperationLifetime();
        var lease = lifetime.enter();
        var exited = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        var interruptedAtExit = new AtomicBoolean();
        var closer = new Thread(() -> {
            try {
                lifetime.sealAndAwait();
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                interruptedAtExit.set(Thread.currentThread().isInterrupted());
                exited.countDown();
            }
        }, "actual-native-global-join");
        closer.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> lease.blocks(closer));
            closer.interrupt();
            assertThat(exited.getCount()).isEqualTo(1);
            assertThatThrownBy(lifetime::enter).isInstanceOf(IllegalStateException.class);
        } finally {
            lease.close();
        }
        assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
        closer.join(5_000);
        assertThat(closer.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(interruptedAtExit.get()).isTrue();
    }
}
