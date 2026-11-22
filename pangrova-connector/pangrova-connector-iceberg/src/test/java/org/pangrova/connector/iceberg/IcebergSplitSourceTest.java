package org.pangrova.connector.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.ConnectorSplit;

import static org.assertj.core.api.Assertions.assertThat;
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
        }, ignored -> new NumberedSplit(converted.incrementAndGet()), 10, cached::set);

        assertThat(started).hasValue(0);
        assertThat(source.getNextBatch(2)).containsExactly(new NumberedSplit(1), new NumberedSplit(2));
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
                ignored -> new NumberedSplit(1), 2, cached::set);
        assertThat(small.getNextBatch(2)).hasSize(2);
        assertThat(cached.get()).isNull();
        assertThat(small.getNextBatch(1)).isEmpty();
        assertThat(small.isFinished()).isTrue();
        assertThat(cached.get()).hasSize(2);

        cached.set(null);
        var large = new IcebergSplitSource("large", () -> plan(3, closed),
                ignored -> new NumberedSplit(1), 2, cached::set);
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
                ignored -> { throw rejected; });

        var observed = catchThrowable(() -> source.getNextBatch(1));
        assertThat(observed).isSameAs(rejected);
        assertThat(observed.getSuppressed()).singleElement().isInstanceOf(UncheckedIOException.class)
                .satisfies(suppressed -> assertThat(suppressed.getCause()).isSameAs(closeFailure));
        assertThat(source.isFinished()).isTrue();
    }

    private static CloseableIterable<FileScanTask> plan(int files, AtomicInteger closed) {
        return CloseableIterable.combine(Collections.<FileScanTask>nCopies(files, null), closed::incrementAndGet);
    }

    private record NumberedSplit(int number) implements ConnectorSplit {}
}
