package org.pangrova.server.coordinator.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.scheduler.SchedulerCancelResult;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.TaskStateEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RecoveredTaskDrainPollTest {
    @Test
    void pendingAttemptDoesNotBlockLaterPagesAndRetriesWithoutReplayingRetainedReceipts() {
        var journal = new RecordingJournal();
        var deadline = Instant.EPOCH.plusSeconds(60);
        var claim = new ExecutionAnchor("execution", "successor", "incarnation", 2, Instant.EPOCH, deadline);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var executor = Executors.newSingleThreadExecutor();
        var observed = new ConcurrentLinkedQueue<String>();
        var next = TaskReport.newBuilder()
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
        try (var poll = new RecoveredTaskDrainPoll(journal, event -> {
            boolean pending = event.taskId().equals("1") && !observed.contains("1");
            observed.add(event.taskId());
            return CompletableFuture.completedFuture(pending
                    ? SchedulerCancelResult.pending() : SchedulerCancelResult.drained(next));
        }, executor, 25)) {
            poll.start(scheduler);
            poll.track(List.of(claim));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(journal.retained).hasSize(RecoveredTaskDrainJournal.PAGE_SIZE + 1));
            assertThat(journal.cursors).containsExactly(0L, (long) RecoveredTaskDrainJournal.PAGE_SIZE, 0L);
            assertThat(journal.batchSizes).containsExactly(RecoveredTaskDrainJournal.PAGE_SIZE - 1, 1, 1);
            assertThat(observed).endsWith(Integer.toString(RecoveredTaskDrainJournal.PAGE_SIZE + 1), "1");
        } finally {
            executor.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    void failedAttemptDoesNotDiscardOtherReceiptsOrPreventLaterPages() {
        var journal = new RecordingJournal();
        var claim = new ExecutionAnchor("execution", "successor", "incarnation", 2, Instant.EPOCH,
                Instant.EPOCH.plusSeconds(60));
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var executor = Executors.newSingleThreadExecutor();
        var receipt = TaskReport.newBuilder()
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
        try (var poll = new RecoveredTaskDrainPoll(journal, event -> {
            if (event.taskId().equals("1")) throw new IllegalStateException("control request unavailable");
            if (event.taskId().equals("2")) return CompletableFuture.failedFuture(
                    new IllegalStateException("control response unavailable"));
            return CompletableFuture.completedFuture(SchedulerCancelResult.drained(receipt));
        }, executor, 25)) {
            poll.start(scheduler);
            poll.track(List.of(claim));
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(journal.retained).hasSize(RecoveredTaskDrainJournal.PAGE_SIZE - 1));
            assertThat(journal.retained).doesNotContain(1L, 2L)
                    .contains((long) RecoveredTaskDrainJournal.PAGE_SIZE + 1);
        } finally {
            executor.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    void awaitingWorkerIoLeavesOneCleanupThreadFreeForOtherQueriesAndLimitsFanout() {
        int limit = RecoveredTaskDrainPoll.MAX_IN_FLIGHT_PAGES;
        var claims = java.util.stream.IntStream.rangeClosed(0, limit)
                .mapToObj(index -> claim("query-" + index, 2)).toList();
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var executor = Executors.newSingleThreadExecutor();
        var journal = new SingleAttemptJournal();
        var slowReplies = new ConcurrentHashMap<String, CompletableFuture<SchedulerCancelResult>>();
        var receipt = SchedulerCancelResult.drained(TaskReport.newBuilder()
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build());
        var poll = new RecoveredTaskDrainPoll(journal, event -> {
            if (event.queryId().equals("query-" + limit)) return CompletableFuture.completedFuture(receipt);
            return slowReplies.computeIfAbsent(event.queryId(), ignored -> new CompletableFuture<>());
        }, executor, 25);
        try {
            poll.start(scheduler);
            poll.track(claims);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(slowReplies).hasSize(limit));
            assertThat(journal.read).hasSize(limit).doesNotContain(claims.get(limit));
            int freeSlot = limit - 1;
            slowReplies.get("query-" + freeSlot).complete(receipt);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(journal.retained).contains(claims.get(freeSlot), claims.get(limit)));
            assertThat(slowReplies.get("query-0")).isNotDone();
            assertThat(journal.retained).doesNotContain(claims.get(0));
        } finally {
            slowReplies.values().forEach(reply -> reply.complete(receipt));
            poll.close();
            executor.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    void newerClaimQueuesBehindOnlyItsOwnInFlightPage() {
        var older = claim("same-query", 2);
        var newer = claim("same-query", 3);
        var journal = new SingleAttemptJournal();
        var requested = new CompletableFuture<Void>();
        var blockedReply = new CompletableFuture<SchedulerCancelResult>();
        var receipt = SchedulerCancelResult.drained(TaskReport.newBuilder()
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build());
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var executor = Executors.newSingleThreadExecutor();
        var poll = new RecoveredTaskDrainPoll(journal, event -> {
            if (event.taskId().equals("2")) {
                requested.complete(null);
                return blockedReply;
            }
            return CompletableFuture.completedFuture(receipt);
        }, executor, 25);
        try {
            poll.start(scheduler);
            poll.track(List.of(older));
            await().atMost(Duration.ofSeconds(5)).until(requested::isDone);
            poll.track(List.of(older, newer, claim("independent", 4)));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(journal.retained).containsExactly(claim("independent", 4)));
            assertThat(journal.read).doesNotContain(newer);
            blockedReply.complete(SchedulerCancelResult.pending());
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(journal.retained).contains(newer));
            assertThat(journal.read).filteredOn(older::equals).hasSize(1);
        } finally {
            blockedReply.complete(receipt);
            poll.close();
            executor.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    private static ExecutionAnchor claim(String executionId, long token) {
        return new ExecutionAnchor(executionId, "successor", "incarnation", token, Instant.EPOCH,
                Instant.EPOCH.plusSeconds(60));
    }

    private static final class SingleAttemptJournal implements RecoveredTaskDrainJournal {
        private final ConcurrentLinkedQueue<ExecutionAnchor> read = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<ExecutionAnchor> retained = new ConcurrentLinkedQueue<>();

        @Override
        public List<IssuedAttempt> pendingTaskDrains(ExecutionAnchor claim, long afterRowId) {
            read.add(claim);
            return List.of(new IssuedAttempt(1, new TaskStateEvent(JournalEventContext.create(1, "owner"),
                    claim.executionId(), Long.toString(claim.fencingToken()), "worker", "SCHEDULED", 0, 0)));
        }

        @Override
        public boolean retainTaskDrains(ExecutionAnchor claim, List<DrainReceipt> receipts) {
            retained.add(claim);
            return true;
        }
    }

    private static final class RecordingJournal implements RecoveredTaskDrainJournal {
        private final Set<Long> retained = ConcurrentHashMap.newKeySet();
        private final ConcurrentLinkedQueue<Long> cursors = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();

        @Override
        public List<IssuedAttempt> pendingTaskDrains(ExecutionAnchor claim, long afterRowId) {
            cursors.add(afterRowId);
            return LongStream.rangeClosed(afterRowId + 1, PAGE_SIZE + 1)
                    .filter(id -> !retained.contains(id))
                    .limit(PAGE_SIZE)
                    .mapToObj(id -> new IssuedAttempt(id, new TaskStateEvent(JournalEventContext.create(1, "owner"),
                            claim.executionId(), Long.toString(id), "worker", "SCHEDULED", 0, 0)))
                    .toList();
        }

        @Override
        public boolean retainTaskDrains(ExecutionAnchor claim, List<DrainReceipt> receipts) {
            batchSizes.add(receipts.size());
            receipts.forEach(receipt -> retained.add(receipt.rowId()));
            return true;
        }
    }
}
