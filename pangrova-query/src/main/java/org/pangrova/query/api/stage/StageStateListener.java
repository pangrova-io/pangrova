package org.pangrova.query.api.stage;

import org.pangrova.query.api.enums.StageState;

import org.pangrova.common.api.annotation.Internal;



/**
 * Callback interface for Stage state transitions.
 * Implementations must be thread-safe as they may be invoked from any thread.
 * Notifications follow successful state publication, but concurrent transitions
 * can deliver notifications out of order. A listener must not infer current state
 * from delivery order. Listener failure does not roll back the published state.
 * The state owner borrows the listener and does not close resources it captures.
 */
@FunctionalInterface
@Internal
public interface StageStateListener {

    /**
     * Called after a successful state transition.
     *
     * @param stageId  the stage identifier
     * @param oldState the state before transition
     * @param newState the state after transition
     */
    void onStateChange(String stageId, StageState oldState, StageState newState);
}
