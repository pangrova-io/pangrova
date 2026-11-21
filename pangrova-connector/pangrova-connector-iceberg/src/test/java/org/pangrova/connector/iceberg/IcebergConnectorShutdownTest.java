package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.exception.ConnectorException;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual catalog and owned executor; no substitute connector or scheduler. */
class IcebergConnectorShutdownTest {
    @TempDir Path warehouse;

    @Test
    void successfulShutdownJoinsTheOwnedSchedulerAndCanBeRepeated() throws Exception {
        var connector = connector();
        var scheduler = connector.pinEvictionExecutor();
        assertThat(scheduler.submit(() -> 42).get(10, TimeUnit.SECONDS)).isEqualTo(42);
        try {
            connector.shutdown();
            assertThat(scheduler.isTerminated()).isTrue();
            connector.shutdown();
            assertThat(scheduler.isTerminated()).isTrue();
        } finally {
            connector.shutdown();
        }
    }

    @Test
    void accessorsShareConnectorIoUntilItsOwnerCloses() throws Exception {
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var conf = new Configuration();
        var io = new org.apache.iceberg.hadoop.HadoopFileIO(conf) {
            @Override public void close() { closes.incrementAndGet(); }
        };
        var directory = java.nio.file.Files.createDirectory(warehouse.resolve("index"));
        var connector = new IcebergConnector("index", new HadoopCatalog(conf, warehouse.toString()),
                io, java.util.Set.of());
        try {
            var support = connector.getMetadata(null).getIndexSupport().orElseThrow();
            try (var first = support.getFileSystemAccessor().orElseThrow();
                 var second = support.getFileSystemAccessor().orElseThrow()) {
                try (var out = first.openOutputStream(directory.resolve("part").toString())) { out.write(42); }
                java.nio.file.Files.createDirectories(directory.resolve("child"));
                java.nio.file.Files.write(directory.resolve("child/nested"), new byte[] {10});
                assertThat(second.listFiles(directory.toString())).singleElement()
                        .asString().endsWith("/index/part");
                assertThatThrownBy(() -> second.listFiles(directory.resolve("missing").toString()))
                        .isInstanceOf(java.io.IOException.class);
            }
            assertThat(closes.get()).isZero();
        } finally {
            connector.shutdown();
        }
        connector.shutdown();
        assertThat(closes.get()).isOne();
    }

    @Test
    void interruptedJoinReportsFailureAndRetainsTheOwnerForRetry() throws Exception {
        var connector = connector();
        var scheduler = connector.pinEvictionExecutor();
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var inFlight = scheduler.submit(() -> {
            started.countDown();
            try {
                boolean released = false;
                while (!released) {
                    try {
                        released = release.await(10, TimeUnit.SECONDS);
                        if (!released) throw new IllegalStateException("Shutdown barrier timed out");
                    } catch (InterruptedException cancellation) {
                        // This actual in-flight task exits only after its owner releases the barrier.
                    }
                }
            } finally {
                exited.countDown();
            }
        });
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            assertThatThrownBy(connector::shutdown).isInstanceOf(ConnectorException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(scheduler.isTerminated()).isFalse();
        } finally {
            Thread.interrupted();
            release.countDown();
            assertThat(exited.await(10, TimeUnit.SECONDS)).isTrue();
            inFlight.get(10, TimeUnit.SECONDS);
            connector.shutdown();
        }
        assertThat(scheduler.isTerminated()).isTrue();
    }

    private IcebergConnector connector() {
        return new IcebergConnector("shutdown_test",
                new HadoopCatalog(new Configuration(), warehouse.toString()));
    }
}
