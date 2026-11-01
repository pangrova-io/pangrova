package org.pangrova.ai.worker.invocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.ai.worker.context.AiExecutionContext;

class AiInvocationBudgetManagerTest {

    @Test
    void taskRegistryHasAnExactConcurrentCapacityAndReleasesSlots() throws Exception {
        AiInvocationBudgetManager manager = new AiInvocationBudgetManager(2);
        var workers = Executors.newFixedThreadPool(16);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                String taskId = "task-" + i;
                futures.add(workers.submit(() -> {
                    start.await();
                    try {
                        manager.registerTask(context(taskId), List.of());
                        return taskId;
                    } catch (AiInvocationException failure) {
                        assertThat(failure.category())
                                .isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED);
                        return null;
                    }
                }));
            }
            start.countDown();
            List<String> accepted = new ArrayList<>();
            for (Future<String> future : futures) {
                String taskId = future.get(10, TimeUnit.SECONDS);
                if (taskId != null) {
                    accepted.add(taskId);
                }
            }
            assertThat(accepted).hasSize(2);
            AiExecutionContext existing = context(accepted.get(0));
            assertThat(manager.taskBudget(existing))
                    .isSameAs(manager.taskBudget(existing));
            assertThatThrownBy(() -> manager.registerTask(context("overflow"), List.of()))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED);

            manager.releaseTask("tenant", existing.taskAttempt());
            assertThat(manager.registerTask(context("replacement"), List.of())).isNotNull();
            assertAuthorization(() -> manager.taskBudget(existing));
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static AiExecutionContext context(String taskId) {
        return org.pangrova.ai.worker.context.AiTestTaskAuthority.context(
                "tenant", "alice", "query", taskId, "trace",
                4_102_444_800_000L, 0);
    }

    @Test
    void callbackCannotCreateAuthorityOrRegisterTheSameAttemptTwice() {
        var manager = new AiInvocationBudgetManager(2);
        var context = context("task");
        assertAuthorization(() -> manager.taskBudget(context));
        manager.registerTask(context, List.of());
        assertAuthorization(() -> manager.registerTask(context, List.of()));
        manager.releaseTask(context.tenantId(), context.taskAttempt());
        assertAuthorization(() -> manager.taskBudget(context));
    }

    @Test
    void cancelledIdleTaskRetainsAuthorityUntilActualJoinedRetirement() {
        var manager = new AiInvocationBudgetManager(1);
        var context = context("cancelled");
        var actualBudget = manager.registerTask(context, List.of());
        manager.cancelTask(context.tenantId(), context.taskAttempt());
        assertAuthorization(() -> manager.taskBudget(context));
        assertAuthorization(() -> manager.registerTask(context, List.of()));
        assertThat(actualBudget.isIdle()).isTrue();
        assertThat(actualBudget.isTerminated()).isFalse();
        assertThatThrownBy(() -> manager.registerTask(context("replacement"), List.of()))
                .isInstanceOf(AiInvocationException.class).extracting("category")
                .isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED);
        assertThat(manager.releaseTask(context.tenantId(), context.taskAttempt()))
                .isEqualTo(actualBudget.usage());
        assertThat(actualBudget.isTerminated()).isTrue();
        assertThat(manager.registerTask(context("replacement"), List.of())).isNotNull();
    }

    @Test
    void fullAttemptIdentitySeparatesFencesAndOwnerIncarnations() {
        var manager = new AiInvocationBudgetManager(3);
        var original = context("task");
        var fenced = withAttempt(original, original.taskAttempt().toBuilder().setFencingToken(8589934593L).build());
        var restarted = withAttempt(original, original.taskAttempt().toBuilder().setExecutionOwner(
                original.taskAttempt().getExecutionOwner().toBuilder().setOwnerIncarnation("restarted")).build());
        var originalBudget = manager.registerTask(original, List.of());
        var fencedBudget = manager.registerTask(fenced, List.of());
        var restartedBudget = manager.registerTask(restarted, List.of());
        assertThat(fencedBudget).isNotSameAs(originalBudget).isNotSameAs(restartedBudget);
        manager.releaseTask(original.tenantId(), original.taskAttempt());
        assertThat(manager.taskBudget(fenced)).isSameAs(fencedBudget);
        assertThat(manager.taskBudget(restarted)).isSameAs(restartedBudget);
    }

    private static AiExecutionContext withAttempt(AiExecutionContext source, org.pangrova.protocol.v2.TaskAttemptId attempt) {
        return new AiExecutionContext(source.tenantId(), source.principal(), source.queryId(), source.taskId(),
                source.traceId(), source.deadlineEpochMs(), source.maxProviderConcurrency(), source.maxProviderItems(),
                source.maxProviderAttempts(), source.maxProviderRequestBytes(), source.maxProviderResponseBytes(),
                source.maxMediaBytes(), attempt);
    }

    private static void assertAuthorization(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOf(AiInvocationException.class).extracting("category")
                .isEqualTo(AiInvocationException.Category.AUTHORIZATION);
    }
}
