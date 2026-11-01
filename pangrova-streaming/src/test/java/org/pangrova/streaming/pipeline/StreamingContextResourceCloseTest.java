package org.pangrova.streaming.pipeline;

import org.pangrova.streaming.maintenance.*;
import org.pangrova.streaming.store.memory.InMemoryWatermarkStore;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link StreamingContext#close()} resource cleanup behavior.
 */
class StreamingContextResourceCloseTest {

    @Test
    void closeSucceeds() throws Exception {
        StreamingContext module = buildModule();
        module.close();
    }

    @Test
    void closeIsIdempotent() throws Exception {
        StreamingContext module = buildModule();
        module.close();
        module.close(); // second close should not throw
    }

    private StreamingContext buildModule() {
        MaintenanceScheduler scheduler = new MaintenanceScheduler(
            new NoOpSpi(), MaintenanceConfig.defaults());
        WatermarkTracker tracker = new WatermarkTracker(new InMemoryWatermarkStore());
        return new StreamingContext(scheduler, tracker);
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
