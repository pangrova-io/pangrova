package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.coordinator.query.execution.dispatch.adaptive.AdaptiveQueryEvaluator;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/** Observes potential adaptive changes from committed shuffle statistics. */
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
        if (event.stage().shuffleStatsReport() != null) {
            adaptiveStageHandler.evaluateStageCompletion(event.stage());
        }
    }
}
