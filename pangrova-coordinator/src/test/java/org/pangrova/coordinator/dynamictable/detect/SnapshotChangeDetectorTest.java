package org.pangrova.coordinator.dynamictable.detect;

import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SnapshotChangeDetector}.
 *
 * <p>Covers the three return paths: empty, incremental, fullRefreshRequired.
 * Uses a hand-written {@link ConfigurableScanPlanner} — no mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 3, 15</b></p>
 */
class SnapshotChangeDetectorTest {

    @Test
    void emptyResult_whenNoDataFilesAndNoDeleteFiles() {
        var planner = new ConfigurableScanPlanner(List.of(), List.of(), 100, 200);
        var detector = new SnapshotChangeDetector(planner);

        ChangeResult result = detector.detect(activeState("100"));

        assertThat(result).isInstanceOf(ChangeResult.Empty.class);
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void incrementalResult_whenDataFilesOnlyNoDeleteFiles() {
        var dataFiles = List.of("data/file1.parquet", "data/file2.parquet");
        var planner = new ConfigurableScanPlanner(dataFiles, List.of(), 100, 200);
        var detector = new SnapshotChangeDetector(planner);

        ChangeResult result = detector.detect(activeState("100"));

        assertThat(result).isInstanceOf(ChangeResult.Incremental.class);
        assertThat(result.isEmpty()).isFalse();
        var incremental = (ChangeResult.Incremental) result;
        assertThat(incremental.plan().dataFiles()).containsExactlyElementsOf(dataFiles);
        assertThat(incremental.plan().deleteFiles()).isEmpty();
    }

    @Test
    void fullRefreshRequired_whenDeleteFilesPresent() {
        var dataFiles = List.of("data/file1.parquet");
        var deleteFiles = List.of("delete/eq-delete-1.parquet");
        var planner = new ConfigurableScanPlanner(dataFiles, deleteFiles, 100, 200);
        var detector = new SnapshotChangeDetector(planner);

        ChangeResult result = detector.detect(activeState("100"));

        assertThat(result).isInstanceOf(ChangeResult.FullRefreshRequired.class);
        assertThat(result.isEmpty()).isFalse();
        var fullRefresh = (ChangeResult.FullRefreshRequired) result;
        assertThat(fullRefresh.plan().deleteFiles()).containsExactlyElementsOf(deleteFiles);
        assertThat(fullRefresh.plan().dataFiles()).containsExactlyElementsOf(dataFiles);
    }

    @Test
    void nullCheckpoint_treatedAsZero() {
        var dataFiles = List.of("data/file1.parquet");
        var planner = new ConfigurableScanPlanner(dataFiles, List.of(), 0, 200);
        var detector = new SnapshotChangeDetector(planner);

        ChangeResult result = detector.detect(activeState(null));

        assertThat(result).isInstanceOf(ChangeResult.Incremental.class);
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private static DynamicTableState activeState(String checkpoint) {
        Instant now = Instant.now();
        return new DynamicTableState(
                "test_dt", "tenant-1", DynamicTableLifecycleState.ACTIVE,
                DynamicTableRefreshMode.SNAPSHOT, Duration.ofMinutes(5),
                checkpoint, "SNAPSHOT",
                1, null, "admin", "coord-1", 1L,
                now, 0L, 0, now, now);
    }

    /**
     * Simple test implementation of {@link IncrementalScanPlanner}
     * that returns a pre-configured result.
     */
    static final class ConfigurableScanPlanner implements IncrementalScanPlanner {

        private final List<String> dataFiles;
        private final List<String> deleteFiles;
        private final long fromSnapshot;
        private final long toSnapshot;

        ConfigurableScanPlanner(List<String> dataFiles, List<String> deleteFiles,
                                long fromSnapshot, long toSnapshot) {
            this.dataFiles = dataFiles;
            this.deleteFiles = deleteFiles;
            this.fromSnapshot = fromSnapshot;
            this.toSnapshot = toSnapshot;
        }

        @Override
        public IncrementalScanPlan plan(String tableName, long from, long to) {
            return new IncrementalScanPlan(dataFiles, deleteFiles, fromSnapshot, toSnapshot);
        }
    }
}
