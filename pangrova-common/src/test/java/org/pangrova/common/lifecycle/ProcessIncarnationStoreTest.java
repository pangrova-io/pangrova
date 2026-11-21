package org.pangrova.common.lifecycle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessIncarnationStoreTest {
    @TempDir Path directory;

    @Test
    void joinedShutdownRetiresOnlyKnownIncarnations() {
        var first = ProcessIncarnationStore.open(directory);
        var old = first.identity();
        assertThat(first.isRetired(old)).isFalse();
        first.closeAfterDrain(() -> {});
        var next = ProcessIncarnationStore.open(directory);
        try {
            assertThat(next.identity().ownershipDomainId()).isEqualTo(old.ownershipDomainId());
            assertThat(next.identity().incarnationId()).isNotEqualTo(old.incarnationId());
            assertThat(next.isRetired(old)).isTrue();
            assertThat(next.isRetired(next.identity())).isFalse();
            assertThat(next.isRetired(new ProcessIncarnationStore.Identity(
                    UUID.randomUUID().toString(), old.incarnationId()))).isFalse();
            assertThat(next.isRetired(new ProcessIncarnationStore.Identity(
                    old.ownershipDomainId(), UUID.randomUUID().toString()))).isFalse();
        } finally {
            next.closeAfterDrain(() -> {});
        }
    }

    @Test
    void failedJoinRetainsExclusiveOwnershipAndAllowsRetry() {
        var first = ProcessIncarnationStore.open(directory);
        try {
            assertThatThrownBy(() -> first.closeAfterDrain(() -> {
                throw new IllegalStateException("Native owner still running");
            })).hasMessageContaining("Native owner still running");
            assertThatThrownBy(() -> ProcessIncarnationStore.open(directory))
                    .hasMessageContaining("already owned");
            assertThat(first.isRetired(first.identity())).isFalse();
        } finally {
            first.closeAfterDrain(() -> {});
        }
    }

    @Test
    void admissionAuthorityCannotBeRecreatedUnderTheSameIncarnation() {
        var store = ProcessIncarnationStore.open(directory);
        try {
            store.claimAuthority("worker-task-admission");
            assertThatThrownBy(() -> store.claimAuthority("worker-task-admission"))
                    .hasMessageContaining("already claimed");
        } finally {
            store.closeAfterDrain(() -> {});
        }
    }

    @Test
    void newAuthorityCannotStartDuringShutdownOrAfterFailedJoin() throws Exception {
        var store = ProcessIncarnationStore.open(directory);
        var identity = store.identity();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var closing = executor.submit(() -> store.closeAfterDrain(() -> {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Join was not released");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
                throw new IllegalStateException("Owner still running");
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> store.claimAuthority("new-authority"))
                    .hasMessageContaining("sealed for shutdown");
            assertThat(store.identity()).isEqualTo(identity);
            assertThat(store.isRetired(identity)).isFalse();
            release.countDown();
            assertThatThrownBy(() -> closing.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.claimAuthority("after-failed-join"))
                    .hasMessageContaining("sealed for shutdown");
            store.closeAfterDrain(() -> {
                assertThat(store.identity()).isEqualTo(identity);
                assertThat(store.isRetired(identity)).isFalse();
            });
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void concurrentShutdownCannotReleaseOwnershipBeforeActualJoin() throws Exception {
        var store = ProcessIncarnationStore.open(directory);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var closing = executor.submit(() -> store.closeAfterDrain(() -> {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Join was not released");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> ProcessIncarnationStore.open(directory)).hasMessageContaining("already owned");
            assertThatThrownBy(() -> store.closeAfterDrain(() -> {})).hasMessageContaining("already running");
            release.countDown();
            closing.get(5, TimeUnit.SECONDS);
            var next = ProcessIncarnationStore.open(directory);
            next.closeAfterDrain(() -> {});
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void tornUnpublishedStartupTailIsRecoveredButCompleteCorruptionIsRejected() throws Exception {
        var first = ProcessIncarnationStore.open(directory);
        var old = first.identity();
        first.closeAfterDrain(() -> {});
        Path ledger = directory.resolve("incarnations.bin");
        Files.write(ledger, new byte[] {1, 2, 3, 4, 5}, StandardOpenOption.APPEND);
        var recovered = ProcessIncarnationStore.open(directory);
        assertThat(recovered.isRetired(old)).isTrue();
        recovered.closeAfterDrain(() -> {});
        byte[] bytes = Files.readAllBytes(ledger);
        bytes[bytes.length - 1] ^= 1;
        Files.write(ledger, bytes);
        assertThatThrownBy(() -> ProcessIncarnationStore.open(directory))
                .hasMessageContaining("Cannot open Process admission state")
                .hasRootCauseMessage("Corrupt Process incarnation ledger record");
    }

    @Test
    void killedIndependentJvmReleasesPhysicalOwnershipAndPreservesRetirement() throws Exception {
        Path ready = directory.resolve("ready");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), OwnerProcess.class.getName(),
                directory.toString(), ready.toString()).redirectError(directory.resolve("child.err").toFile()).start();
        try {
            var readiness = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    return child.inputReader().readLine();
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
            assertThat(readiness.get(15, TimeUnit.SECONDS)).isEqualTo("ready");
            assertThat(child.isAlive()).withFailMessage(() -> readError(directory)).isTrue();
            String[] identity = Files.readString(ready).split("\\n");
            var old = new ProcessIncarnationStore.Identity(identity[0], identity[1]);
            assertThatThrownBy(() -> ProcessIncarnationStore.open(directory))
                    .hasMessageContaining("already owned");
            child.destroyForcibly();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            var recovered = ProcessIncarnationStore.open(directory);
            try {
                assertThat(recovered.isRetired(old)).isTrue();
            } finally {
                recovered.closeAfterDrain(() -> {});
            }
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static String readError(Path directory) {
        try {
            return Files.readString(directory.resolve("child.err"));
        } catch (java.io.IOException failure) {
            return failure.toString();
        }
    }

    public static final class OwnerProcess {
        private OwnerProcess() {}
        public static void main(String[] args) throws Exception {
            var store = ProcessIncarnationStore.open(Path.of(args[0]));
            var identity = store.identity();
            Files.writeString(Path.of(args[1]), identity.ownershipDomainId() + "\n" + identity.incarnationId());
            System.out.write("ready\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            System.out.flush();
            new java.util.concurrent.CountDownLatch(1).await();
        }
    }
}
