package org.pangrova.worker.clustering;

import org.pangrova.connector.spi.clustering.ClusteringCapability;
import org.pangrova.connector.api.clustering.ClusteringMode;
import org.pangrova.connector.api.clustering.ClusteringRequest;
import org.pangrova.connector.api.clustering.ClusteringResult;
import org.pangrova.connector.api.clustering.ClusteringTableRef;
import org.pangrova.connector.api.clustering.FileClusteringStats;
import org.pangrova.connector.api.clustering.ReclusterTask;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReclusterTaskExecutorTest {

    private static final ClusteringTableRef TABLE = new ClusteringTableRef("catalog", "db", "table");

    @Test
    void executeWithRegisteredCapability() {
        ReclusterTaskExecutor executor = new ReclusterTaskExecutor();
        executor.registerCapability("iceberg", new StubCapability(3));

        ReclusterTask task = new ReclusterTask("task-1", "iceberg",
                TABLE, List.of("col1"), ClusteringMode.LINEAR, List.of("f1.parquet"));
        ClusteringResult result = executor.execute(task);

        assertThat(result.rewrittenFileCount()).isEqualTo(3);
    }

    @Test
    void executeWithoutCapabilityThrows() {
        ReclusterTaskExecutor executor = new ReclusterTaskExecutor();
        ReclusterTask task = new ReclusterTask("task-2", "unknown",
                TABLE, List.of("col1"), ClusteringMode.LINEAR, List.of("f1.parquet"));

        assertThatThrownBy(() -> executor.execute(task))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown");
    }

    @Test
    void registerOverwritesPreviousCapability() {
        ReclusterTaskExecutor executor = new ReclusterTaskExecutor();
        executor.registerCapability("iceberg", new StubCapability(1));
        executor.registerCapability("iceberg", new StubCapability(5));

        ReclusterTask task = new ReclusterTask("task-3", "iceberg",
                TABLE, List.of("col1"), ClusteringMode.LINEAR, List.of("f1.parquet"));
        assertThat(executor.execute(task).rewrittenFileCount()).isEqualTo(5);
    }

    private static final class StubCapability implements ClusteringCapability {
        private final int count;
        StubCapability(int count) { this.count = count; }

        @Override
        public boolean supportsMode(ClusteringMode mode) { return true; }

        @Override
        public List<FileClusteringStats> collectFileStats(
                ClusteringTableRef tableName, List<String> columnNames) {
            return List.of();
        }

        @Override
        public ClusteringResult recluster(ClusteringRequest request) {
            return new ClusteringResult(count, List.of(), List.of(), 100L);
        }
    }
}
