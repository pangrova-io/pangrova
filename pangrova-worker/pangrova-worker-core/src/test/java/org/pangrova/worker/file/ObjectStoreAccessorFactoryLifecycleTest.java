package org.pangrova.worker.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.file.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.object.ObjectStoreAccessor;
import org.pangrova.file.api.object.ObjectStoreAccessorProvider;
import org.pangrova.storage.filesystem.object.LocalObjectStoreAccessor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectStoreAccessorFactoryLifecycleTest {
    @TempDir Path directory;

    @Test
    void closedFactoryCannotReopenCachedOrNewProfilesAndClosesItsProviders() {
        var provider = new CountingProvider();
        var factory = new ObjectStoreAccessorFactory(java.util.List.of(provider));
        factory.accessor(profile("first"));
        factory.close();
        factory.close();
        assertThat(provider.closes.get()).isEqualTo(1);
        assertThatThrownBy(() -> factory.accessor(profile("first"))).hasMessageContaining("closed");
        assertThatThrownBy(() -> factory.accessor(profile("new"))).hasMessageContaining("closed");
    }

    @Test
    void concurrentCloseReleasesOwnedProviderOnceAndRejectsFurtherAllocation() throws Exception {
        var provider = new CountingProvider();
        var factory = new ObjectStoreAccessorFactory(java.util.List.of(provider));
        var start = new CountDownLatch(1);
        var threads = Executors.newFixedThreadPool(8);
        try {
            var futures = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 32; i++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    factory.close();
                }, threads));
            }
            start.countDown();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
            assertThat(provider.closes.get()).isEqualTo(1);
            assertThatThrownBy(() -> factory.accessor(profile("later"))).hasMessageContaining("closed");
        } finally {
            start.countDown();
            threads.shutdownNow();
            factory.close();
        }
    }

    @Test
    void duplicateBackendInventoryIsRejectedBeforeProviderOwnershipTransfers() {
        var first = new CountingProvider();
        var second = new CountingProvider();
        assertThatThrownBy(() -> new ObjectStoreAccessorFactory(java.util.List.of(first, second)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
        assertThat(first.closes).hasValue(0);
        assertThat(second.closes).hasValue(0);
        first.close();
        second.close();
    }

    @Test
    void oneFailedProviderDoesNotPreventTheOtherOwnerClosingAndFailureIsRetained() {
        var fatal = new AssertionError("provider shutdown failed");
        var first = new CountingProvider(() -> { throw fatal; });
        var secondClosed = new AtomicInteger();
        var second = new ObjectStoreAccessorProvider() {
            @Override public StorageType storageType() { return StorageType.S3; }
            @Override public ObjectStoreAccessor accessor(StorageProfile profile) { throw new AssertionError(); }
            @Override public void close() { secondClosed.incrementAndGet(); }
        };
        var factory = new ObjectStoreAccessorFactory(java.util.List.of(first, second));
        assertThatThrownBy(factory::close).isSameAs(fatal);
        assertThatThrownBy(factory::close).isSameAs(fatal);
        assertThat(first.closes).hasValue(1);
        assertThat(secondClosed).hasValue(1);
        assertThatThrownBy(() -> factory.accessor(profile("late"))).hasMessageContaining("closed");
    }

    private StorageProfile profile(String name) {
        return new StorageProfile(name, StorageType.LOCAL, directory.toString(), "", "", "us-east-1",
                Map.of(), Map.of());
    }

    @Test
    void fatalCleanupStillReleasesEveryLaterOwnerAndPreservesItsIdentity() {
        var fatal = new AssertionError("first owner failed");
        var later = new java.io.IOException("later owner failed");
        var closed = new AtomicInteger();
        assertThatThrownBy(() -> ObjectStoreAccessorFactory.closeOwned(java.util.List.of(
                () -> { throw fatal; },
                () -> { throw later; },
                () -> { throw fatal; },
                () -> { closed.incrementAndGet(); })))
                .isSameAs(fatal);
        assertThat(closed).hasValue(1);
        assertThat(fatal.getSuppressed()).hasSize(1);
        assertThat(fatal.getSuppressed()[0].getSuppressed()).containsExactly(later);
    }

    @Test
    void laterCloseWaitsForTheSameCleanupAndRetainsItsFailure() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var expected = new IllegalStateException("credential cleanup failed");
        var provider = new CountingProvider(() -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test cleanup was not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            throw expected;
        });
        var factory = new ObjectStoreAccessorFactory(java.util.List.of(provider));
        var threads = Executors.newFixedThreadPool(2);
        try {
            var first = CompletableFuture.runAsync(factory::close, threads);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var secondEntered = new CountDownLatch(1);
            var second = CompletableFuture.runAsync(() -> {
                secondEntered.countDown();
                factory.close();
            }, threads);
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second).isNotDone();
            release.countDown();
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(factory::close).satisfies(failure ->
                    assertThat(failure.getSuppressed()).containsExactly(expected));
            assertThat(provider.closes.get()).isEqualTo(1);
            assertThatThrownBy(() -> factory.accessor(profile("late"))).hasMessageContaining("closed");
        } finally {
            release.countDown();
            threads.shutdownNow();
        }
    }

    private static final class CountingProvider implements ObjectStoreAccessorProvider {
        private final AtomicInteger closes = new AtomicInteger();
        private final Runnable cleanup;

        CountingProvider() { this(() -> { }); }
        CountingProvider(Runnable cleanup) { this.cleanup = cleanup; }

        @Override public StorageType storageType() { return StorageType.LOCAL; }
        @Override public ObjectStoreAccessor accessor(StorageProfile profile) {
            return new LocalObjectStoreAccessor(Path.of(profile.bucket()));
        }

        @Override public void close() { closes.incrementAndGet(); cleanup.run(); }
    }
}
