package org.pangrova.worker.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileReadWatchdogTest {
    @Test
    void expiryAbortsAnAlreadyBlockedSdkReadWithoutDrainingOrDeliveringBytes() throws Exception {
        var alarm = new ManualFileReadAlarm();
        var entered = new CountDownLatch(1);
        var aborted = new CountDownLatch(1);
        var closes = new AtomicInteger();
        var aborts = new AtomicInteger();
        var body = new InputStream() {
            @Override public int read() throws IOException {
                entered.countDown();
                try {
                    if (!aborted.await(5, TimeUnit.SECONDS)) throw new IOException("Abort did not release read");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IOException(failure);
                }
                throw new IOException("Socket aborted");
            }
            @Override public void close() { closes.incrementAndGet(); }
        };
        var response = new ResponseInputStream<>(GetObjectResponse.builder().contentLength(1L).eTag("etag").contentRange("bytes 0-0/1").build(),
                AbortableInputStream.create(body, () -> {
                    aborts.incrementAndGet();
                    aborted.countDown();
                }));
        var client = new software.amazon.awssdk.services.s3.S3Client() {
            @Override public software.amazon.awssdk.services.s3.model.HeadObjectResponse headObject(
                    software.amazon.awssdk.services.s3.model.HeadObjectRequest request) {
                return software.amazon.awssdk.services.s3.model.HeadObjectResponse.builder()
                        .contentLength(1L).eTag("etag").lastModified(java.time.Instant.EPOCH).build();
            }
            @Override public ResponseInputStream<GetObjectResponse> getObject(
                    software.amazon.awssdk.services.s3.model.GetObjectRequest request) { return response; }
            @Override public String serviceName() { return "s3"; }
            @Override public void close() { }
        };
        var location = new org.pangrova.filesystem.api.object.ObjectLocation(
                new org.pangrova.filesystem.api.profile.StorageProfile("test", org.pangrova.common.storage.StorageType.S3,
                        "bucket", "", "", "us-east-1", java.util.Map.of(), java.util.Map.of()), "");
        var reader = Executors.newSingleThreadExecutor();
        try (var owner = new FileReadWatchdog(alarm);
                var input = owner.open(new org.pangrova.filesystem.s3.object.S3ObjectStoreAccessor(client)
                        .openRange(location, "item", 0, 1, null), 2_000, () -> 1_000)) {
            var result = reader.submit(() -> input.read());
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(alarm.delayMillis()).isEqualTo(1_000);
            alarm.fire();
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).cause()
                    .isInstanceOf(IOException.class).hasMessageContaining("expired");
            assertThat(aborts.get()).isEqualTo(1);
            assertThat(closes.get()).isZero();
        } finally {
            aborted.countDown();
            reader.shutdownNow();
            assertThat(reader.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void endpointShutdownClosesEveryReadCancelsItsAlarmAndRejectsNewReads() throws IOException {
        var alarm = new ManualFileReadAlarm();
        var closes = new AtomicInteger();
        var owner = new FileReadWatchdog(alarm);
        var input = owner.open(counted(closes), 2_000, () -> 1_000);
        owner.close();
        owner.close();
        input.close();
        assertThat(closes.get()).isEqualTo(1);
        assertThat(alarm.cancelled()).isTrue();
        assertThat(alarm.isShutdown()).isTrue();
        assertThatThrownBy(input::read).isInstanceOf(IOException.class).hasMessageContaining("closed");
        assertThatThrownBy(() -> owner.open(counted(closes), 2_000, () -> 1_000))
                .isInstanceOf(IOException.class).hasMessageContaining("endpoint is closed");
        assertThat(closes.get()).isEqualTo(2);
    }

    @Test
    void explicitCloseRetiresTheAlarmAndDoesNotCloseStorageTwice() throws IOException {
        var alarm = new ManualFileReadAlarm();
        var closes = new AtomicInteger();
        try (var owner = new FileReadWatchdog(alarm)) {
            var input = owner.open(counted(closes), 2_000, () -> 1_000);
            input.close();
            alarm.fire();
            input.close();
            assertThat(alarm.cancelled()).isTrue();
            assertThat(closes.get()).isEqualTo(1);
            assertThatThrownBy(input::read).isInstanceOf(IOException.class).hasMessageContaining("closed");
        }
    }

    @Test
    void expiryBeforeScheduleReturnsCannotAdmitAClosedReadOrLeaveAnAlarm() {
        var alarm = new ManualFileReadAlarm();
        var closes = new AtomicInteger();
        alarm.beforeReturn(alarm::fire);
        try (var owner = new FileReadWatchdog(alarm)) {
            assertThatThrownBy(() -> owner.open(counted(closes), 2_000, () -> 1_000))
                    .isInstanceOf(IOException.class).hasMessageContaining("expired");
            assertThat(closes.get()).isEqualTo(1);
            // A completed alarm need not become cancelled; it cannot run again.
            alarm.fire();
            assertThat(closes.get()).isEqualTo(1);
        }
    }

    @Test
    void endpointShutdownBeforeScheduleReturnsRetiresTheNewAlarm() {
        var alarm = new ManualFileReadAlarm();
        var closes = new AtomicInteger();
        var owner = new FileReadWatchdog(alarm);
        alarm.beforeReturn(owner::close);
        assertThatThrownBy(() -> owner.open(counted(closes), 2_000, () -> 1_000))
                .isInstanceOf(IOException.class).hasMessageContaining("closed");
        assertThat(alarm.cancelled()).isTrue();
        assertThat(closes.get()).isEqualTo(1);
        owner.close();
    }

    @Test
    void schedulerRejectionStillClosesTheNewProviderStream() {
        var alarm = new ManualFileReadAlarm();
        var closes = new AtomicInteger();
        alarm.shutdownNow();
        try (var owner = new FileReadWatchdog(alarm)) {
            assertThatThrownBy(() -> owner.open(counted(closes), 2_000, () -> 1_000))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
            assertThat(closes.get()).isEqualTo(1);
        }
    }

    @Test
    void failedExpiryCleanupRemainsVisibleToReaderAndEndpointShutdown() throws IOException {
        var alarm = new ManualFileReadAlarm();
        var owner = new FileReadWatchdog(alarm);
        var closes = new AtomicInteger();
        var input = owner.open(new ByteArrayInputStream(new byte[]{1}) {
            @Override public void close() throws IOException {
                closes.incrementAndGet();
                throw new IOException("Provider close failed");
            }
        }, 2_000, () -> 1_000);
        alarm.fire();
        assertThatThrownBy(input::read).isInstanceOfSatisfying(IOException.class, failure -> {
            assertThat(failure).hasMessageContaining("expired");
            assertThat(failure.getSuppressed()).singleElement().satisfies(suppressed ->
                    assertThat(suppressed).hasMessage("Provider close failed"));
        });
        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Provider close failed");
        }
        assertThat(closes.get()).isEqualTo(1);
    }

    @Test
    void concurrentShutdownCallersObserveTheSameCompletedCleanupFailure() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var secondEntered = new CountDownLatch(1);
        var closes = new AtomicInteger();
        var owner = new FileReadWatchdog(new ManualFileReadAlarm());
        owner.open(new ByteArrayInputStream(new byte[]{1}) {
            @Override public void close() throws IOException {
                closes.incrementAndGet();
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("Cleanup was not released");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IOException(failure);
                }
                throw new IOException("Concurrent provider close failed");
            }
        }, 2_000, () -> 1_000);
        var callers = Executors.newFixedThreadPool(2);
        try {
            var first = callers.submit(owner::close);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var second = callers.submit(() -> {
                secondEntered.countDown();
                owner.close();
            });
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(second.isDone()).isFalse();
            release.countDown();
            for (var result : java.util.List.of(first, second)) {
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasRootCauseMessage("Concurrent provider close failed");
            }
            assertThat(closes.get()).isEqualTo(1);
        } finally {
            release.countDown();
            callers.shutdownNow();
            assertThat(callers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void aLaterFatalCleanupFailureCannotBeHiddenByAnEarlierIOException() throws IOException {
        var owner = new FileReadWatchdog(new ManualFileReadAlarm());
        var first = owner.open(new ByteArrayInputStream(new byte[]{1}) {
            @Override public void close() throws IOException { throw new IOException("First close failed"); }
        }, 2_000, () -> 1_000);
        assertThatThrownBy(first::close).isInstanceOf(IOException.class);
        var fatal = new AssertionError("Fatal close failed");
        var second = owner.open(new ByteArrayInputStream(new byte[]{1}) {
            @Override public void close() { throw fatal; }
        }, 2_000, () -> 1_000);
        assertThatThrownBy(second::close).isSameAs(fatal);
        assertThatThrownBy(owner::close).isSameAs(fatal);
        assertThatThrownBy(owner::close).isSameAs(fatal);
    }

    private static InputStream counted(AtomicInteger closes) {
        return new ByteArrayInputStream(new byte[]{1}) {
            @Override public void close() { closes.incrementAndGet(); }
        };
    }
}
