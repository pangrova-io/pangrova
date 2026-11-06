package org.pangrova.planner.core.validation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeCapabilityRegistry}.
 * Covers defaults(), update(), query methods, and thread safety.
 */
class NativeCapabilityRegistryTest {

    // --- defaults() ---

    @Test
    void defaultsContainsExpectedRelTypes() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        assertThat(registry.isRelSupported("ReadRel")).isTrue();
        assertThat(registry.isRelSupported("FilterRel")).isTrue();
        assertThat(registry.isRelSupported("ProjectRel")).isTrue();
        assertThat(registry.isRelSupported("AggregateRel")).isTrue();
        assertThat(registry.isRelSupported("HashJoinRel")).isTrue();
        assertThat(registry.isRelSupported("MergeJoinRel")).isTrue();
        assertThat(registry.isRelSupported("JoinRel")).isTrue();
        assertThat(registry.isRelSupported("ExchangeRel")).isTrue();
        assertThat(registry.isRelSupported("SortRel")).isTrue();
        assertThat(registry.isRelSupported("FetchRel")).isTrue();
        assertThat(registry.isRelSupported("ConsistentPartitionWindowRel"))
                .isTrue();
        assertThat(registry.isRelSupported("SetRel")).isTrue();
    }

    @Test
    void defaultsContainsExpectedExprTypes() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        assertThat(registry.isExprSupported("FieldReference")).isTrue();
        assertThat(registry.isExprSupported("Literal")).isTrue();
        assertThat(registry.isExprSupported("ScalarFunction")).isTrue();
        assertThat(registry.isExprSupported("Cast")).isTrue();
        assertThat(registry.isExprSupported("IfThen")).isTrue();
        assertThat(registry.isExprSupported("Nested")).isTrue();
        assertThat(registry.isExprSupported("WindowFunction")).isTrue();
    }

    @Test
    void defaultsContainsExpectedFunctions() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        // Standard Substrait functions aligned with FunctionMapper mappings
        assertThat(registry.isFunctionSupported("equal")).isTrue();
        assertThat(registry.isFunctionSupported("gt")).isTrue();
        assertThat(registry.isFunctionSupported("lt")).isTrue();
        assertThat(registry.isFunctionSupported("is_distinct_from")).isTrue();
        assertThat(registry.isFunctionSupported("is_not_distinct_from")).isTrue();
        assertThat(registry.isFunctionSupported("add")).isTrue();
        assertThat(registry.isFunctionSupported("round")).isTrue();
        assertThat(registry.isFunctionSupported("year")).isTrue();
        assertThat(registry.isFunctionSupported("month")).isTrue();
        assertThat(registry.isFunctionSupported("day")).isTrue();
        assertThat(registry.isFunctionSupported("hour")).isTrue();
        assertThat(registry.isFunctionSupported("minute")).isTrue();
        assertThat(registry.isFunctionSupported("subscript")).isTrue();
        assertThat(registry.isFunctionSupported("contains")).isTrue();
        assertThat(registry.isFunctionSupported("variant_get")).isTrue();
        assertThat(registry.isFunctionSupported("array_contains")).isFalse();
        assertThat(registry.isFunctionSupported("sum")).isTrue();
        assertThat(registry.isFunctionSupported("count")).isTrue();
        assertThat(registry.isFunctionSupported("row_number")).isTrue();
        // Vector distance functions
        assertThat(registry.isFunctionSupported("l2_distance")).isTrue();
        assertThat(registry.isFunctionSupported("cosine_distance")).isTrue();
        assertThat(registry.isFunctionSupported("inner_product")).isTrue();
        assertThat(registry.isFunctionSupported("pangrova_cosine_similarity")).isTrue();
        assertThat(registry.isFunctionSupported("file_from_bytes")).isTrue();
        assertThat(registry.isFunctionSupported("file_from_url")).isTrue();
        // Unknown functions still rejected
        assertThat(registry.isFunctionSupported("nonexistent_func")).isFalse();
    }

    @Test
    void defaultsRejectsUnknownRelType() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        assertThat(registry.isRelSupported("UnknownJoinRel")).isFalse();
        assertThat(registry.isRelSupported("")).isFalse();
    }

    @Test
    void defaultsRejectsUnknownExprType() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        assertThat(registry.isExprSupported("SwitchExpression")).isFalse();
    }

    // --- update() ---

    @Test
    void updateReplacesRelTypes() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        assertThat(registry.isRelSupported("ReadRel")).isTrue();

        registry.update(Set.of("MergeJoinRel"), Set.of(), Set.of());

        assertThat(registry.isRelSupported("ReadRel")).isFalse();
        assertThat(registry.isRelSupported("MergeJoinRel")).isTrue();
    }

    @Test
    void updateReplacesExprTypes() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        assertThat(registry.isExprSupported("Cast")).isTrue();

        registry.update(Set.of(), Set.of("NewExpr"), Set.of());

        assertThat(registry.isExprSupported("Cast")).isFalse();
        assertThat(registry.isExprSupported("NewExpr")).isTrue();
    }

    @Test
    void updateReplacesFunctions() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        // defaults() now includes standard functions; verify a non-default function is absent
        assertThat(registry.isFunctionSupported("custom_func")).isFalse();

        registry.update(Set.of(), Set.of(), Set.of("custom_func", "subtract"));

        assertThat(registry.isFunctionSupported("custom_func")).isTrue();
        assertThat(registry.isFunctionSupported("subtract")).isTrue();
        // update replaces the entire set — previously-default functions are gone
        assertThat(registry.isFunctionSupported("add")).isFalse();
    }

    @Test
    void updateWithEmptySetsRemovesAllCapabilities() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        registry.update(Set.of(), Set.of(), Set.of());

        assertThat(registry.isRelSupported("ReadRel")).isFalse();
        assertThat(registry.isExprSupported("Literal")).isFalse();
    }

    // --- Thread safety ---

    @Test
    void concurrentReadsAndUpdateDoNotThrow() throws Exception {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        int threadCount = 8;
        int iterations = 1000;
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        List<Future<?>> futures = new ArrayList<>();

        // Half threads read, half threads update.
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < iterations; i++) {
                    if (threadId % 2 == 0) {
                        // Reader: query capabilities.
                        registry.isRelSupported("ReadRel");
                        registry.isExprSupported("Literal");
                        registry.isFunctionSupported("add");
                    } else {
                        // Writer: update capabilities.
                        registry.update(
                                Set.of("ReadRel", "FilterRel"),
                                Set.of("Literal", "Cast"),
                                Set.of("add"));
                    }
                }
            }));
        }

        startLatch.countDown();

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void updateIsAtomicPerField() {
        // After update, all three sets should reflect the new values.
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        registry.update(
                Set.of("NewRel"),
                Set.of("NewExpr"),
                Set.of("newFunc"));

        assertThat(registry.isRelSupported("NewRel")).isTrue();
        assertThat(registry.isExprSupported("NewExpr")).isTrue();
        assertThat(registry.isFunctionSupported("newFunc")).isTrue();
        // Old values gone.
        assertThat(registry.isRelSupported("ReadRel")).isFalse();
        assertThat(registry.isExprSupported("Literal")).isFalse();
    }
}
