package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WorkerRerankerExecutor (Task 196).
 */
class WorkerRerankerExecutorTest {

    @Test
    void rrfStrategyAvailableByDefault() {
        var executor = new WorkerRerankerExecutor();
        assertTrue(executor.hasStrategy("rrf"));
    }

    @Test
    void weightedStrategyAvailableByDefault() {
        var executor = new WorkerRerankerExecutor();
        assertTrue(executor.hasStrategy("weighted"));
    }

    @Test
    void cascadeStrategyAvailableByDefault() {
        var executor = new WorkerRerankerExecutor();
        assertTrue(executor.hasStrategy("cascade"));
    }

    @Test
    void executeRrfReranking() {
        var executor = new WorkerRerankerExecutor();

        List<ScoredCandidate> candidates = List.of(
                new ScoredCandidate(1L, 0.9, "vector"),
                new ScoredCandidate(2L, 0.8, "vector"),
                new ScoredCandidate(1L, 0.7, "fts"),
                new ScoredCandidate(3L, 0.95, "fts"));

        List<RankedResult> results = executor.execute(
                "rrf",
                Map.of("k", "60"),
                "test query",
                candidates,
                2);

        assertNotNull(results);
        assertEquals(2, results.size());
        // Row 1 appears in both sources, should rank highest with RRF
        assertEquals(1L, results.get(0).rowOffset());
    }

    @Test
    void unknownStrategyThrows() {
        var executor = new WorkerRerankerExecutor();

        assertThrows(IllegalStateException.class, () ->
                executor.execute(
                        "nonexistent",
                        Map.of(),
                        "query",
                        List.of(new ScoredCandidate(1L, 0.9, "vector")),
                        5));
    }

    @Test
    void invalidTopKThrows() {
        var executor = new WorkerRerankerExecutor();

        assertThrows(IllegalArgumentException.class, () ->
                executor.execute(
                        "rrf",
                        Map.of(),
                        "query",
                        List.of(),
                        0));
    }

    @Test
    void availableStrategiesContainsBuiltins() {
        var executor = new WorkerRerankerExecutor();
        var strategies = executor.availableStrategies();
        assertTrue(strategies.contains("rrf"), "missing rrf");
        assertTrue(strategies.contains("weighted"), "missing weighted");
        assertTrue(strategies.contains("cascade"), "missing cascade");
        assertTrue(strategies.contains("cross_encoder"), "missing cross_encoder");
    }
}
