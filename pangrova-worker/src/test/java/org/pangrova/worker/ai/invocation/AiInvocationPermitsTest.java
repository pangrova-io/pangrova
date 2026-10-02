package org.pangrova.worker.ai.invocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.worker.ai.context.AiTestTaskAuthority;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Exercises actual semaphore ownership and interruption, without a provider substitute. */
class AiInvocationPermitsTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void revocationRollsBackOnlyThePermitsAlreadyAcquired(int blockedPosition) throws Exception {
        Instant deadline = Instant.now().plusSeconds(30);
        var task = new AiTaskBudget(AiTestTaskAuthority.context(
                "tenant", "principal", "query", "task", "trace", deadline.toEpochMilli(), 0), List.of());
        Semaphore[] semaphores = {new Semaphore(1), new Semaphore(1), new Semaphore(1)};
        semaphores[blockedPosition].acquire();
        AtomicBoolean interrupted = new AtomicBoolean();
        var executor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "ai-partial-permit-cancellation"));
        try {
            var result = executor.submit(() -> {
                try (var permits = AiInvocationPermits.acquire(task, deadline, semaphores)) {
                    throw new AssertionError("Revoked admission must not acquire all permits");
                } catch (AiInvocationException failure) {
                    interrupted.set(Thread.currentThread().isInterrupted());
                    return failure.category();
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(semaphores[blockedPosition]::hasQueuedThreads);
            for (int index = 0; index < blockedPosition; index++) {
                assertThat(semaphores[index].availablePermits()).isZero();
            }
            task.revoke();
            assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(AiInvocationException.Category.AUTHORIZATION);
            assertThat(interrupted.get()).isTrue();
            for (int index = 0; index < semaphores.length; index++) {
                assertThat(semaphores[index].availablePermits()).isEqualTo(index == blockedPosition ? 0 : 1);
            }
        } finally {
            semaphores[blockedPosition].release();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
