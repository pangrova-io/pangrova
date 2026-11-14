package org.pangrova.streaming.pipeline;

import org.pangrova.connector.api.enums.TableFormat;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.enums.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.connector.api.write.WriteResult;

import org.pangrova.connector.api.write.TableWriter;

import org.pangrova.streaming.maintenance.*;
import org.pangrova.persistence.streaming.memory.InMemoryWatermarkStore;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link StreamingWiring#close()} exception collection behavior.
 * Verifies one writer failure does not prevent closing other resources.
 */
class StreamingWiringResourceCloseTest {

    @Test
    void closeAllResourcesEvenWhenFirstWriterFails() throws Exception {
        RecordingWriter w1 = new RecordingWriter("w1", true);
        RecordingWriter w2 = new RecordingWriter("w2", false);
        StreamingWiring module = buildModule();
        module.registerWriter("f1", w1);
        module.registerWriter("f2", w2);

        assertThatThrownBy(module::close)
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("w1");

        assertThat(w1.closed).isTrue();
        assertThat(w2.closed).isTrue();
    }

    @Test
    void closeCollectsMultipleExceptionsAsSuppressed() throws Exception {
        RecordingWriter w1 = new RecordingWriter("w1", true);
        RecordingWriter w2 = new RecordingWriter("w2", true);
        StreamingWiring module = buildModule();
        module.registerWriter("f1", w1);
        module.registerWriter("f2", w2);

        try {
            module.close();
        } catch (Exception e) {
            assertThat(e.getSuppressed().length).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void closeSucceedsWhenNoExceptions() throws Exception {
        RecordingWriter w1 = new RecordingWriter("w1", false);
        StreamingWiring module = buildModule();
        module.registerWriter("f1", w1);
        module.close();
        assertThat(w1.closed).isTrue();
    }

    @Test
    void closeHandlesEmptyWriters() throws Exception {
        StreamingWiring module = buildModule();
        module.close();
    }

    private StreamingWiring buildModule() {
        MaintenanceScheduler scheduler = new MaintenanceScheduler(
            new NoOpSpi(), MaintenanceConfig.defaults());
        WatermarkTracker tracker = new WatermarkTracker(new InMemoryWatermarkStore());
        return new StreamingWiring(scheduler, tracker);
    }

    static final class RecordingWriter implements TableWriter {
        final String name;
        final boolean failOnClose;
        boolean closed;

        RecordingWriter(String name, boolean failOnClose) {
            this.name = name;
            this.failOnClose = failOnClose;
        }

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode mode, WriteContext context) {
            return new WriteResult(tableName, 0, 0, 0, 0, Instant.now());
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {
            closed = true;
            if (failOnClose) {
                throw new RuntimeException("Close failed for " + name);
            }
        }
    }

    static final class NoOpSpi implements TableMaintenanceSpi {
        @Override
        public CompletableFuture<CompactionResult> compactAsync(String t) {
            return CompletableFuture.completedFuture(new CompactionResult(t, 0, 0, 0));
        }

        @Override
        public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String t, Duration d) {
            return CompletableFuture.completedFuture(new OrphanCleanupResult(t, 0, 0));
        }

        @Override
        public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String t, Duration d) {
            return CompletableFuture.completedFuture(new SnapshotExpirationResult(t, 0));
        }

        @Override
        public MaintenanceStatus getMaintenanceStatus(String t) {
            return new MaintenanceStatus(t, 0, 0, 0, 0, 0, 0, 0, null, null, null);
        }
    }
}
