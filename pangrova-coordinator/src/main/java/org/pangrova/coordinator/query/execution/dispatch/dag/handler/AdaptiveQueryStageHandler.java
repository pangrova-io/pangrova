package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveQueryEvaluator;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Evaluates adaptive query re-optimization after each stage completes.
 * Delegates to {@link AdaptiveQueryEvaluator} for partition coalescing,
 * skew detection, and dynamic parallelism adjustments.
 *
 * <p>Only acts on FINISHED events — scheduling does not require
 * adaptive evaluation since no shuffle statistics are available yet.</p>
 */
public final class AdaptiveQueryStageHandler implements StageLifecycleHandler {

    private final AdaptiveQueryEvaluator adaptiveStageHandler;

    public AdaptiveQueryStageHandler(AdaptiveQueryEvaluator adaptiveStageHandler) {
        this.adaptiveStageHandler = checkNotNull(
                adaptiveStageHandler, "adaptiveStageHandler must not be null");
    }

    /**
     * No-op: adaptive evaluation requires shuffle statistics that are
     * only available after stage completion, not at scheduling time.
     */
    @Override
    public void onScheduling(StageLifecycleEvent event) {
        // intentional no-op — shuffle stats unavailable at scheduling time
    }

    @Override
    public void onFinished(StageLifecycleEvent event) {
        adaptiveStageHandler.evaluateStageCompletion(event.stage());
    }
}
