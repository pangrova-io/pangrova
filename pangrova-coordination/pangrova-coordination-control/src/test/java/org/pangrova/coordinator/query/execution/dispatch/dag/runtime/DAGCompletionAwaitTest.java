package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.common.client.result.ResultEndpoint;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DAGCompletionAwaitTest {
    @Test
    void interruptionPreservesFlagAndSettlesTheOwnedFuture() {
        var completion = new CompletableFuture<DispatchResult>();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> DAGCompletionEvaluator.awaitResult(completion, "q", () -> completion.cancel(false)))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.COORDINATOR_DISPATCH_INTERRUPTED));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(completion.isCompletedExceptionally()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void completedResultAndOriginalFailureArePreserved() {
        var result = new DispatchResult("worker", "task", new ResultEndpoint("worker", 1, "task", "grpc"));
        assertThat(DAGCompletionEvaluator.awaitResult(CompletableFuture.completedFuture(result), "q", () -> { }))
                .isSameAs(result);
        var failure = new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED, "actual task failure");
        assertThatThrownBy(() -> DAGCompletionEvaluator.awaitResult(
                CompletableFuture.failedFuture(failure), "q", () -> { })).isSameAs(failure);
    }
}
