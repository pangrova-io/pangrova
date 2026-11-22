package org.pangrova.coordinator.query.execution.split;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Deterministic unit evidence for the production drain owner, not connector or distributed E2E. */
class SplitFetcherInterruptionTest {
    @AfterEach
    void clearTestInterruption() {
        Thread.interrupted();
    }

    @Test
    void initiallyInterruptedSourceIsClosedWithoutEnumerating() {
        var source = new ControlledSource(2, 0);
        Thread.currentThread().interrupt();

        assertInterruption(catchThrowable(() -> SplitFetcher.drainSource(source, 30_000)));

        assertThat(source.pagesRead).isZero();
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void interruptionAfterFirstPageRejectsPartialSplits() {
        var source = new ControlledSource(2, 1);

        assertInterruption(catchThrowable(() -> SplitFetcher.drainSource(source, 30_000)));

        assertThat(source.pagesRead).isEqualTo(1);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void interruptionOnFinalPageCannotEscapeAsComplete() {
        var source = new ControlledSource(2, 2);

        assertInterruption(catchThrowable(() -> SplitFetcher.drainSource(source, 30_000)));

        assertThat(source.pagesRead).isEqualTo(2);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void interruptionOnFinishedCheckCannotEscapeAsComplete() {
        var source = new ControlledSource(1, 0);
        source.interruptOnFinished = true;

        assertInterruption(catchThrowable(() -> SplitFetcher.drainSource(source, 30_000)));

        assertThat(source.pagesRead).isEqualTo(1);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void interruptionOnCloseCannotEscapeAsComplete() {
        var source = new ControlledSource(1, 0);
        source.interruptOnClose = true;

        assertInterruption(catchThrowable(() -> SplitFetcher.drainSource(source, 30_000)));

        assertThat(source.pagesRead).isEqualTo(1);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void normalCompletionReturnsEveryPageAndClosesOnce() {
        var source = new ControlledSource(2, 0);

        var result = SplitFetcher.drainSource(source, 30_000);

        assertThat(result).containsExactly(new PageSplit(1), new PageSplit(2));
        assertThat(source.pagesRead).isEqualTo(2);
        assertThat(source.closeCount).isEqualTo(1);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void closeFailureIsSuppressedOnOriginalInterruption() {
        var source = new ControlledSource(2, 1);
        source.closeFailure = new IllegalStateException("source close rejected");

        var failure = catchThrowable(() -> SplitFetcher.drainSource(source, 30_000));

        assertInterruption(failure);
        assertThat(failure.getSuppressed()).containsExactly(source.closeFailure);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void sourceCloseCannotClearTheOriginalInterruption() {
        var source = new ControlledSource(2, 1);
        source.clearInterruptOnClose = true;

        assertInterruption(catchThrowable(() -> SplitFetcher.drainSource(source, 30_000)));

        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void closeFailureIsSuppressedOnOriginalEnumerationFailure() {
        var source = new ControlledSource(2, 0);
        source.batchFailure = new IllegalStateException("batch enumeration rejected");
        source.closeFailure = new IllegalArgumentException("source close rejected");

        var failure = catchThrowable(() -> SplitFetcher.drainSource(source, 30_000));

        assertThat(failure).isSameAs(source.batchFailure);
        assertThat(failure.getSuppressed()).containsExactly(source.closeFailure);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void closeFailureCannotTurnNormalEnumerationIntoSuccess() {
        var source = new ControlledSource(1, 0);
        source.closeFailure = new IllegalStateException("source close rejected");

        var failure = catchThrowable(() -> SplitFetcher.drainSource(source, 30_000));

        assertThat(failure).isSameAs(source.closeFailure);
        assertThat(source.pagesRead).isEqualTo(1);
        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    void aFinalBatchPastTheStageDeadlineIsClosedAndNeverAdmitted() {
        var source = new ControlledSource(1, 0);
        var splits = new ArrayList<ConnectorSplit>();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
        source.beforeBatch = () -> {
            long remaining;
            while ((remaining = deadline - System.nanoTime()) > 0) LockSupport.parkNanos(remaining);
        };

        var failure = catchThrowable(() -> SplitFetcher.drainSource(source, splits, deadline, UnaryOperator.identity()));

        assertThat(failure).isInstanceOfSatisfying(PangrovaException.class, expired ->
                assertThat(expired.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT));
        assertThat(source.pagesRead).isEqualTo(1);
        assertThat(source.closeCount).isEqualTo(1);
        assertThat(splits).isEmpty();
    }

    @Test
    void distinctSourcesShareTheStageCapAndAnExactFitCanFinish() {
        var first = new ControlledSource(1, 0);
        var second = new ControlledSource(1, 0);
        var splits = new ArrayList<ConnectorSplit>(Collections.nCopies(99_999, new PageSplit(0)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        SplitFetcher.drainSource(first, splits, deadline, UnaryOperator.identity());
        var failure = catchThrowable(() -> SplitFetcher.drainSource(second, splits, deadline, UnaryOperator.identity()));

        assertThat(splits).hasSize(100_000);
        assertThat(first.closeCount).isEqualTo(1);
        assertThat(second.closeCount).isEqualTo(1);
        assertThat(failure).isInstanceOfSatisfying(PangrovaException.class, exhausted ->
                assertThat(exhausted.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED));
    }

    private static void assertInterruption(Throwable failure) {
        assertThat(failure).isInstanceOf(PangrovaException.class).hasCauseInstanceOf(InterruptedException.class);
        assertThat(((PangrovaException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    private record PageSplit(int page) implements ConnectorSplit {}

    private static final class ControlledSource implements ConnectorSplitSource {
        private final int pages;
        private final int interruptAfterPage;
        private int pagesRead;
        private int closeCount;
        private boolean interruptOnFinished;
        private boolean interruptOnClose;
        private boolean clearInterruptOnClose;
        private RuntimeException batchFailure;
        private RuntimeException closeFailure;
        private Runnable beforeBatch = () -> { };

        ControlledSource(int pages, int interruptAfterPage) {
            this.pages = pages;
            this.interruptAfterPage = interruptAfterPage;
        }

        @Override
        public List<ConnectorSplit> getNextBatch(int maxSize) {
            beforeBatch.run();
            if (batchFailure != null) throw batchFailure;
            var batch = List.<ConnectorSplit>of(new PageSplit(++pagesRead));
            if (pagesRead == interruptAfterPage) Thread.currentThread().interrupt();
            return batch;
        }

        @Override
        public boolean isFinished() {
            boolean finished = pagesRead == pages;
            if (finished && interruptOnFinished) Thread.currentThread().interrupt();
            return finished;
        }

        @Override
        public void close() {
            closeCount++;
            if (clearInterruptOnClose) Thread.interrupted();
            if (interruptOnClose) Thread.currentThread().interrupt();
            if (closeFailure != null) throw closeFailure;
        }
    }
}
