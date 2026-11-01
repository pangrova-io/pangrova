package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Evaluates straggler detection for all RUNNING stages after each
 * stage completion. Delegates to {@link SpeculativeStageHandler} for
 * the actual detection and speculative task launch.
 */
public final class SpeculationStageHandler implements StageLifecycleHandler {

    private final SpeculativeStageHandler speculativeStageHandler;

    public SpeculationStageHandler(SpeculativeStageHandler speculativeStageHandler) {
        this.speculativeStageHandler = checkNotNull(
                speculativeStageHandler, "speculativeStageHandler must not be null");
    }

    /**
     * No-op: speculation evaluation requires running stages to compare
     * against, which are not available at scheduling time.
     */
    @Override
    public void onScheduling(StageLifecycleEvent event) {
        // intentional no-op — speculation applies only after stage completion
    }

    @Override
    public void onFinished(StageLifecycleEvent event) {
        if (!speculativeStageHandler.isEnabled()) {
            return;
        }
        evaluateRunningStages(event.queryId(), event.dag());
    }

    private void evaluateRunningStages(String queryId, StageDAG dag) {
        for (Stage stage : dag.stageEntities()) {
            if (stage.stateMachine().getState() == StageStateMachine.State.RUNNING) {
                speculativeStageHandler.evaluateStragglers(
                        queryId, String.valueOf(stage.stageId()));
            }
        }
    }
}
