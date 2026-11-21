package org.pangrova.shuffle.api.coordinator;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;

/** Installs the authoritative journal for a durable execution and detaches its volatile pass on failure. */
@Experimental(reason = "Durable exchange recovery")
public interface RecoverableShuffleControl {
    /** Installs the execution journal before the first registration or recovery RPC. */
    void bindDurableEvidence(String executionId, ShuffleLifecycleEvidence journal);

    /** Discards local state for the specified owner while preserving the recoverable backend allocation. */
    void detachDurableControl(String executionId, ShuffleExecutionOwner expectedOwner);
}
