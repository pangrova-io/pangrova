package org.pangrova.connector.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.iceberg.handle.IcebergSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class IcebergSplitSourceTest {
    @Test
    void closingAnEarlyBatchClosesThePlanAndNeverCachesPartialSplits() {
        var started = new AtomicInteger();
        var converted = new AtomicInteger();
        var closed = new AtomicInteger();
        var cached = new AtomicReference<List<ConnectorSplit>>();
        var source = new IcebergSplitSource("table", () -> {
            started.incrementAndGet();
            return plan(5, closed);
        }, ignored -> split(converted.incrementAndGet()), SplitPlanningBudget.standard(), 0, 10, cached::set);

        assertThat(started).hasValue(0);
        assertThat(source.getNextBatch(2)).extracting(ConnectorSplit::dataFilePaths)
                .containsExactly(List.of("file-1"), List.of("file-2"));
        assertThat(source.isFinished()).isFalse();
        assertThat(converted).hasValue(2);
        source.close();
        source.close();
        assertThat(source.isFinished()).isTrue();
        assertThat(started).hasValue(1);
        assertThat(closed).hasValue(1);
        assertThat(cached.get()).isNull();
    }

    @Test
    void onlyACompletePlanWithinTheCacheBudgetIsPublished() {
        var closed = new AtomicInteger();
        var cached = new AtomicReference<List<ConnectorSplit>>();
        var small = new IcebergSplitSource("small", () -> plan(2, closed),
                ignored -> split(1), SplitPlanningBudget.standard(), 0, 2, cached::set);
        assertThat(small.getNextBatch(2)).hasSize(2);
        assertThat(cached.get()).isNull();
        assertThat(small.getNextBatch(1)).isEmpty();
        assertThat(small.isFinished()).isTrue();
        assertThat(cached.get()).hasSize(2);

        cached.set(null);
        var large = new IcebergSplitSource("large", () -> plan(3, closed),
                ignored -> split(1), SplitPlanningBudget.standard(), 0, 2, cached::set);
        assertThat(large.getNextBatch(4)).hasSize(3);
        assertThat(large.isFinished()).isTrue();
        assertThat(closed).hasValue(2);
        assertThat(cached.get()).isNull();
    }

    @Test
    void failedConversionClosesThePlanAndRetainsTheOriginalFailure() {
        var rejected = new IllegalStateException("split conversion failed");
        var closeFailure = new IOException("manifest reader could not close");
        var source = new IcebergSplitSource("failed", () -> CloseableIterable.combine(
                Collections.<FileScanTask>singletonList(null), () -> { throw closeFailure; }),
                ignored -> { throw rejected; }, SplitPlanningBudget.standard(), 0, 0, null);

        var observed = catchThrowable(() -> source.getNextBatch(1));
        assertThat(observed).isSameAs(rejected);
        assertThat(observed.getSuppressed()).singleElement().isInstanceOf(UncheckedIOException.class)
                .satisfies(suppressed -> assertThat(suppressed.getCause()).isSameAs(closeFailure));
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    void rangesShareOneConversionAndKeepTheWholeFileIdentityAcrossBatches() {
        var converted = new AtomicInteger();
        var closed = new AtomicInteger();
        var budget = SplitPlanningBudget.standard();
        var source = new IcebergSplitSource("table",
                () -> rangePlan(List.of(4L, 20L, 40L, 60L, 95L), closed),
                ignored -> {
                    converted.incrementAndGet();
                    return IcebergSplit.builder("file.parquet", 0, 100, "PARQUET")
                            .tableName("lake.db.table").dataFileSizeBytes(100).snapshotId(17).scanEstimate(110, 1000).build();
                }, budget, 38, 0, null);

        var first = (IcebergSplit) source.getNextBatch(1).get(0);
        var rest = source.getNextBatch(3).stream().map(IcebergSplit.class::cast).toList();
        assertThat(first.fileOffset()).isZero();
        assertThat(first.fileLength()).isEqualTo(40);
        assertThat(first.estimatedScanBytes()).isEqualTo(50);
        assertThat(first.estimatedScanRows()).isEqualTo(400);
        assertThat(rest).extracting(IcebergSplit::fileOffset).containsExactly(40L, 95L);
        assertThat(rest).extracting(IcebergSplit::fileLength).containsExactly(55L, 5L);
        assertThat(rest).allSatisfy(range -> {
            assertThat(range.dataFileSizeBytes()).isEqualTo(100);
            assertThat(range.snapshotId()).isEqualTo(17);
            assertThat(range.toDataSplit().getDataObjectId()).isEqualTo(first.toDataSplit().getDataObjectId());
            assertThat(range.toDataSplit().getSplitId()).isNotEqualTo(first.toDataSplit().getSplitId());
        });
        assertThat(converted).hasValue(1);
        assertThat(closed).hasValue(1);
    }

    @Test
    void capacityFailureClosesPendingRangesAndNeverCachesAPartialFile() {
        var closed = new AtomicInteger();
        var cached = new AtomicReference<List<ConnectorSplit>>();
        var budget = new SplitPlanningBudget(SplitPlanningBudget.standard().deadlineNanos(), 1);
        var source = new IcebergSplitSource("table", () -> rangePlan(List.of(4L, 50L), closed),
                ignored -> IcebergSplit.builder("file.parquet", 0, 100, "PARQUET").build(),
                budget, 25, 10, cached::set);

        assertThat(source.getNextBatch(1)).hasSize(1);
        assertThatThrownBy(() -> source.getNextBatch(1)).isInstanceOf(ConnectorException.class)
                .hasMessageContaining("1 stage slots left");
        assertThat(source.isFinished()).isTrue();
        assertThat(closed).hasValue(1);
        assertThat(cached.get()).isNull();
    }

    @Test
    void absentOrOutOfBoundsManifestOffsetsKeepOneWholeFile() {
        var whole = IcebergSplit.builder("file.parquet", 0, 100, "PARQUET").build();
        for (List<Long> offsets : List.of(List.<Long>of(), List.of(4L), List.of(4L, 100L), List.of(4L, 4L))) {
            var closed = new AtomicInteger();
            var source = new IcebergSplitSource("table", () -> rangePlan(offsets, closed),
                    ignored -> whole, SplitPlanningBudget.standard(), 25, 0, null);
            assertThat(source.getNextBatch(2)).containsExactly(whole);
            assertThat(closed).hasValue(1);
        }
    }

    private static CloseableIterable<FileScanTask> rangePlan(List<Long> offsets, AtomicInteger closed) {
        var data = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("file.parquet")
                .withRecordCount(1000).withFileSizeInBytes(100).withSplitOffsets(offsets).build();
        FileScanTask task = new BaseFileScanTask(data, null, "", "", null);
        return CloseableIterable.combine(List.of(task), closed::incrementAndGet);
    }

    private static CloseableIterable<FileScanTask> plan(int files, AtomicInteger closed) {
        return CloseableIterable.combine(Collections.<FileScanTask>nCopies(files, null), closed::incrementAndGet);
    }

    private static IcebergSplit split(int number) {
        return IcebergSplit.builder("file-" + number, 0, 1, "PARQUET").build();
    }
}
