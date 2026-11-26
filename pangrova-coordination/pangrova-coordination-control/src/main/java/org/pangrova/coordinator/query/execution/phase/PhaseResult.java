package org.pangrova.coordinator.query.execution.phase;

import org.pangrova.execution.task.DispatchResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Result of a query execution phase.
 * Terminal short-circuits the pipeline; Continue passes updated context to the next phase.
 */
public sealed interface PhaseResult {

    /** Ends the phase pipeline with the result when its owner completes execution. */
    record Terminal(CompletionStage<DispatchResult> completion) implements PhaseResult {
        public static Terminal completed(DispatchResult result) {
            return new Terminal(CompletableFuture.completedFuture(result));
        }
    }

    /** Continue to next phase with updated context. */
    record Continue(QueryExecutionContext updated) implements PhaseResult {}
}
