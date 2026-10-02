package org.pangrova.coordinator.property.speculation;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.coordinator.speculation.DuplicateTaskResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 42: Duplicate task resolution correctness.
 *
 * <p><b>Validates: Requirements 64.3</b></p>
 *
 * <p>For any set of concurrent task completions on the same stage partition,
 * DuplicateTaskResolver selects exactly one winner deterministically.</p>
 */
@Tag("Feature: architecture-remediation")
@Tag("Property 42: Duplicate task resolution correctness")
class DuplicateTaskResolverPropertyTest {

    private static final String QUERY_ID = "property-query";

    @Property(tries = 10)
    @Label("Exactly one winner per stage partition")
    void exactlyOneWinnerPerPartition(
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 0, max = 15) int partitionId,
            @ForAll @IntRange(min = 2, max = 5) int competitorCount) {

        DuplicateTaskResolver resolver = new DuplicateTaskResolver();
        List<Boolean> results = new ArrayList<>();

        for (int i = 0; i < competitorCount; i++) {
            results.add(resolver.onTaskComplete(
                    QUERY_ID, "task-" + i, stageId, partitionId));
        }

        long winnerCount = results.stream().filter(Boolean::booleanValue).count();
        assertThat(winnerCount)
                .as("Exactly one task must win per (stageId, partitionId)")
                .isEqualTo(1);
    }

    @Property(tries = 10)
    @Label("Winner is retrievable after registration")
    void winnerIsRetrievable(
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 0, max = 15) int partitionId) {

        DuplicateTaskResolver resolver = new DuplicateTaskResolver();
        resolver.onTaskComplete(QUERY_ID, "first-task", stageId, partitionId);
        resolver.onTaskComplete(QUERY_ID, "second-task", stageId, partitionId);

        Optional<String> winner = resolver.getWinner(QUERY_ID, stageId, partitionId);
        assertThat(winner).isPresent().hasValue("first-task");
    }

    @Property(tries = 10)
    @Label("Clear removes all entries for a stage")
    void clearRemovesStageEntries(
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 1, max = 8) int partitionCount) {

        DuplicateTaskResolver resolver = new DuplicateTaskResolver();
        for (int p = 0; p < partitionCount; p++) {
            resolver.onTaskComplete(QUERY_ID, "task-" + p, stageId, p);
        }

        resolver.clearQuery(QUERY_ID);

        for (int p = 0; p < partitionCount; p++) {
            assertThat(resolver.getWinner(QUERY_ID, stageId, p))
                    .as("After clear, no winner should exist for partition %d", p)
                    .isEmpty();
        }
    }

    @Property(tries = 10)
    @Label("Concurrent completions produce exactly one winner")
    void concurrentCompletionsProduceOneWinner(
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 0, max = 7) int partitionId,
            @ForAll @IntRange(min = 2, max = 8) int threadCount) throws InterruptedException {

        DuplicateTaskResolver resolver = new DuplicateTaskResolver();
        ConcurrentHashMap<String, Boolean> results = new ConcurrentHashMap<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final String taskId = "task-" + i;
            new Thread(() -> {
                try {
                    start.await();
                    boolean won = resolver.onTaskComplete(
                            QUERY_ID, taskId, stageId, partitionId);
                    results.put(taskId, won);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        start.countDown();
        done.await();

        long winnerCount = results.values().stream().filter(Boolean::booleanValue).count();
        assertThat(winnerCount)
                .as("Under concurrency, exactly one task must win")
                .isEqualTo(1);
    }

    @Property(tries = 10)
    @Label("Different partitions have independent winners")
    void differentPartitionsHaveIndependentWinners(
            @ForAll("stageIds") String stageId) {

        DuplicateTaskResolver resolver = new DuplicateTaskResolver();
        resolver.onTaskComplete(QUERY_ID, "task-A", stageId, 0);
        resolver.onTaskComplete(QUERY_ID, "task-B", stageId, 1);

        assertThat(resolver.getWinner(QUERY_ID, stageId, 0)).hasValue("task-A");
        assertThat(resolver.getWinner(QUERY_ID, stageId, 1)).hasValue("task-B");
    }

    @Property(tries = 10)
    @Label("Identical stage partitions in different queries elect independently")
    void queryIdentityIsPartOfElectionKey(@ForAll("stageIds") String stageId) {
        DuplicateTaskResolver resolver = new DuplicateTaskResolver();
        assertThat(resolver.onTaskComplete("query-a", "task-a", stageId, 0)).isTrue();
        assertThat(resolver.onTaskComplete("query-b", "task-b", stageId, 0)).isTrue();
        resolver.clearQuery("query-a");
        assertThat(resolver.getWinner("query-a", stageId, 0)).isEmpty();
        assertThat(resolver.getWinner("query-b", stageId, 0)).hasValue("task-b");
    }

    @Provide
    Arbitrary<String> stageIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "stage-" + s);
    }
}
