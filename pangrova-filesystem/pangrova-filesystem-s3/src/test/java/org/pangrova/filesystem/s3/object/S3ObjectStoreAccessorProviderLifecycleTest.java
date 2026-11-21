package org.pangrova.filesystem.s3.object;

import org.junit.jupiter.api.Test;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ObjectStoreAccessorProviderLifecycleTest {

    @Test
    void closedFactoryCannotReopenCachedOrNewProfilesAndDoesNotCloseBorrowedCredentials() {
        var credentials = new CountingCredentials();
        var factory = new S3ObjectStoreAccessorProvider(credentials);
        factory.accessor(profile("first"));
        factory.close();
        factory.close();
        assertThat(credentials.closes.get()).isZero();
        assertThatThrownBy(() -> factory.accessor(profile("first"))).hasMessageContaining("closed");
        assertThatThrownBy(() -> factory.accessor(profile("new"))).hasMessageContaining("closed");
    }

    @Test
    void concurrentCloseReleasesOwnedCredentialsOnceAndRejectsFurtherAllocation() throws Exception {
        var credentials = new CountingCredentials();
        var factory = new S3ObjectStoreAccessorProvider(credentials, true);
        factory.accessor(profile("active"));
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
            assertThat(credentials.closes.get()).isEqualTo(1);
            assertThatThrownBy(() -> factory.accessor(profile("later"))).hasMessageContaining("closed");
        } finally {
            start.countDown();
            threads.shutdownNow();
            factory.close();
        }
    }

    private StorageProfile profile(String name) {
        return new StorageProfile(name, StorageType.S3, "test-bucket", "", "", "us-east-1",
                Map.of(), Map.of());
    }

    @Test
    void fatalCleanupStillReleasesEveryLaterOwnerAndPreservesItsIdentity() {
        var fatal = new AssertionError("first owner failed");
        var later = new java.io.IOException("later owner failed");
        var closed = new AtomicInteger();
        assertThatThrownBy(() -> S3ObjectStoreAccessorProvider.closeOwned(java.util.List.of(
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
        var credentials = new CountingCredentials(() -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test cleanup was not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            throw expected;
        });
        var factory = new S3ObjectStoreAccessorProvider(credentials, true);
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
            assertThat(credentials.closes.get()).isEqualTo(1);
            assertThatThrownBy(() -> factory.accessor(profile("late"))).hasMessageContaining("closed");
        } finally {
            release.countDown();
            threads.shutdownNow();
        }
    }

    private static final class CountingCredentials implements AwsCredentialsProvider, AutoCloseable {
        private final AtomicInteger closes = new AtomicInteger();
        private final Runnable cleanup;

        CountingCredentials() { this(() -> { }); }
        CountingCredentials(Runnable cleanup) { this.cleanup = cleanup; }

        @Override public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("test-key", "test-secret");
        }

        @Override public void close() { closes.incrementAndGet(); cleanup.run(); }
    }
}
