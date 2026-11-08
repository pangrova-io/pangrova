package org.pangrova.ai.worker.inference.openai;

import org.pangrova.ai.provider.openai.inference.InferenceBatch;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.error.AiInvocationException;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class InferenceBatchTest {
    @Test
    void submissionRejectionWaitsForAcceptedChildAndRetainsItsUnknownOutcome() throws Exception {
        var children = Executors.newSingleThreadExecutor();
        var caller = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        var rejected = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var submissions = new AtomicInteger();
        var unknown = new AiInvocationException(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                false, "POST outcome unknown");
        Executor partial = command -> {
            if (submissions.getAndIncrement() == 0) {
                children.execute(command);
            } else {
                rejected.countDown();
                throw new RejectedExecutionException("executor closed during batch submission");
            }
        };
        try {
            var batch = caller.submit(() -> catchThrowable(() -> InferenceBatch.execute(3, partial, index -> {
                started.countDown();
                await(release);
                throw unknown;
            })));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(rejected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> batch.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            release.countDown();
            var failure = batch.get(5, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(AiInvocationException.class).hasCauseReference(unknown);
            assertThat(failure.getSuppressed()).hasSize(1);
            assertThat(failure.getSuppressed()[0]).isInstanceOf(RejectedExecutionException.class);
            assertThat(submissions).hasValue(2);
        } finally {
            release.countDown();
            caller.shutdownNow();
            children.shutdownNow();
            assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(children.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void outOfOrderCompletionRetainsInputOrdinals() throws Exception {
        var children = Executors.newFixedThreadPool(2);
        var secondFinished = new CountDownLatch(1);
        var order = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
        try {
            List<Integer> values = InferenceBatch.execute(2, children, index -> {
                if (index == 0) {
                    await(secondFinished);
                }
                order.add(index);
                if (index == 1) {
                    secondFinished.countDown();
                }
                return index;
            });
            assertThat(order).containsExactly(1, 0);
            assertThat(values).containsExactly(0, 1);
        } finally {
            secondFinished.countDown();
            children.shutdownNow();
            assertThat(children.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test interrupted", failure);
        }
    }
}
