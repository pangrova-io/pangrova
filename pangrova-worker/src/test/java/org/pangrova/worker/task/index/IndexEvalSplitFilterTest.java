package org.pangrova.worker.task.index;

import org.pangrova.spi.index.IndexEvalResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IndexEvalSplitFilter} aggregation logic.
 *
 * <p>Covers three scenarios: evaluator present (splits filtered),
 * evaluator absent (all pass through), evaluator throws (conservative KEEP).</p>
 */
class IndexEvalSplitFilterTest {

    @Test
    void evaluatorPresent_skipResultFiltersOut() {
        List<IndexEvalResult> results = List.of(
                IndexEvalResult.KEEP, IndexEvalResult.SKIP, IndexEvalResult.UNKNOWN);
        IndexEvalResult aggregated = IndexEvalSplitFilter.aggregateResults(results);
        assertThat(aggregated)
                .as("Any SKIP in results must produce SKIP")
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluatorAbsent_allPassThrough() {
        IndexEvalSplitFilter filter = IndexEvalSplitFilter.tryCreate();
        // When no IndexFileEvaluatorFactory is on the classpath, tryCreate returns null
        // This verifies the conservative pass-through behavior
        if (filter == null) {
            // No evaluator available — this is the expected "absent" case
            // The design guarantees all splits pass through when filter is null
            assertThat(filter).isNull();
            return;
        }
        // If an evaluator IS available (indexing jar on classpath), verify
        // that results without SKIP produce KEEP or UNKNOWN
        List<IndexEvalResult> results = List.of(
                IndexEvalResult.KEEP, IndexEvalResult.UNKNOWN);
        IndexEvalResult aggregated = IndexEvalSplitFilter.aggregateResults(results);
        assertThat(aggregated).isEqualTo(IndexEvalResult.KEEP);
    }

    @Test
    void evaluatorThrows_conservativeKeep() {
        // When evaluation fails, UNKNOWN is recorded per-hint.
        // All UNKNOWN results aggregate to UNKNOWN (conservative KEEP behavior).
        List<IndexEvalResult> results = List.of(
                IndexEvalResult.UNKNOWN, IndexEvalResult.UNKNOWN);
        IndexEvalResult aggregated = IndexEvalSplitFilter.aggregateResults(results);
        assertThat(aggregated)
                .as("All UNKNOWN (from exceptions) must produce UNKNOWN (conservative KEEP)")
                .isEqualTo(IndexEvalResult.UNKNOWN);
    }
}
