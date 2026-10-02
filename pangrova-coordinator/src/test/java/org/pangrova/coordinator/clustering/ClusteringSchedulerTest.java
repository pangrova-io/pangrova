package org.pangrova.coordinator.clustering;

import org.pangrova.spi.connector.clustering.ClusteringCapability;
import org.pangrova.spi.connector.clustering.ClusteringDescriptor;
import org.pangrova.spi.connector.clustering.ClusteringMode;
import org.pangrova.spi.connector.clustering.ClusteringRequest;
import org.pangrova.spi.connector.clustering.ClusteringResult;
import org.pangrova.spi.connector.clustering.ClusteringState;
import org.pangrova.spi.connector.clustering.ClusteringTableRef;
import org.pangrova.spi.connector.clustering.ColumnMinMax;
import org.pangrova.spi.connector.clustering.FileClusteringStats;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ClusteringScheduler} distributed lease integration (BUG-D11).
 */
class ClusteringSchedulerTest {

    private static final ClusteringTableRef TABLE =
            new ClusteringTableRef("catalog", "db", "orders");
    private static final ClusteringDescriptor DESCRIPTOR =
            new ClusteringDescriptor(TABLE, List.of("col1"), ClusteringMode.INTERLEAVE, ClusteringState.PENDING);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void reclusterAcquiresAndReleasesLease() throws Exception {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        ClusteringScheduler scheduler = new ClusteringScheduler(
                executor, null, lease, "coord-1", null);

        ClusteringResult result = scheduler.scheduleManualRecluster(
                DESCRIPTOR, new StubCapability(3)).get();

        assertThat(result.rewrittenFileCount()).isEqualTo(3);
        // Lease must be released after execution
        assertThat(lease.currentHolder("recluster:" + TABLE)).isEmpty();
    }

    @Test
    void reclusterReleasesLeaseOnFailure() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        ClusteringScheduler scheduler = new ClusteringScheduler(
                executor, null, lease, "coord-1", null);

        assertThatThrownBy(() -> scheduler.scheduleManualRecluster(
                DESCRIPTOR, new FailingCapability()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class);

        // Lease must be released even on failure
        assertThat(lease.currentHolder("recluster:" + TABLE)).isEmpty();
    }

    @Test
    void reclusterDeniedWhenLeaseHeldByAnotherCoordinator() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        // Another coordinator holds the lease
        lease.tryAcquire("recluster:" + TABLE, "coord-other",
                java.time.Duration.ofMinutes(30));

        ClusteringScheduler scheduler = new ClusteringScheduler(
                executor, null, lease, "coord-1", null);

        assertThatThrownBy(() -> scheduler.scheduleManualRecluster(
                DESCRIPTOR, new StubCapability(1)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recluster lease");
    }

    @Test
    void reclusterWithoutLeaseWorksAsBeforeD11() throws Exception {
        ClusteringScheduler scheduler = new ClusteringScheduler(executor);

        ClusteringResult result = scheduler.scheduleManualRecluster(
                DESCRIPTOR, new StubCapability(5)).get();

        assertThat(result.rewrittenFileCount()).isEqualTo(5);
    }

    @Test
    void leaseKeyUsesTableName() {
        assertThat(ClusteringScheduler.LEASE_KEY_PREFIX + TABLE)
                .isEqualTo("recluster:catalog.db.orders");
    }

    // --- Stubs (no mock frameworks per project rules) ---

    private static final class StubCapability implements ClusteringCapability {
        private final int rewriteCount;

        StubCapability(int rewriteCount) {
            this.rewriteCount = rewriteCount;
        }

        @Override
        public boolean supportsMode(ClusteringMode mode) {
            return true;
        }

        @Override
        public List<FileClusteringStats> collectFileStats(
                ClusteringTableRef tableName, List<String> columnNames) {
            // Return overlapping file stats so identifyWorstFiles selects files
            return List.of(
                    new FileClusteringStats("f1.parquet", 100,
                            List.of(new ColumnMinMax("col1", 1, 10))),
                    new FileClusteringStats("f2.parquet", 100,
                            List.of(new ColumnMinMax("col1", 5, 15))));
        }

        @Override
        public ClusteringResult recluster(ClusteringRequest request) {
            return new ClusteringResult(rewriteCount, List.of("new.parquet"),
                    List.of("f1.parquet", "f2.parquet"), 50L);
        }
    }

    private static final class FailingCapability implements ClusteringCapability {
        @Override
        public boolean supportsMode(ClusteringMode mode) {
            return true;
        }

        @Override
        public List<FileClusteringStats> collectFileStats(
                ClusteringTableRef tableName, List<String> columnNames) {
            throw new RuntimeException("simulated recluster failure");
        }

        @Override
        public ClusteringResult recluster(ClusteringRequest request) {
            throw new RuntimeException("simulated recluster failure");
        }
    }
}
