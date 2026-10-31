package org.pangrova.coordinator.query.stage;

/**
 * Callback interface for Stage state transitions.
 * Implementations must be thread-safe as they may be invoked from any thread.
 */
@FunctionalInterface
public interface StateChangeListener {

    /**
     * Called after a successful state transition.
     *
     * @param stageId  the stage identifier
     * @param oldState the state before transition
     * @param newState the state after transition
     */
    void onStateChange(String stageId, StageStateMachine.State oldState, StageStateMachine.State newState);
}
