package org.pangrova.coordinator.query.execution.dispatch.shuffle.databuild;

import org.pangrova.protocol.v2.shuffle.FinalizeShuffleRequest;
import org.pangrova.protocol.v2.shuffle.FinalizeShuffleResponse;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleRegistration;
import org.pangrova.shuffle.api.ShuffleServiceAddress;

import java.util.Optional;

/** Durable control observation of the same external-shuffle RPCs used by ordinary DAG execution. */
@org.pangrova.common.api.annotation.Internal
public interface ShuffleLifecycleEvidence {
    Optional<Allocation> restoredAllocation(ShuffleRegistration registration);

    void registered(ShuffleRegistration registration, ShuffleHandle handle, ShuffleExecutionOwner owner);

    void finalized(ShuffleRegistration registration, ShuffleHandle handle,
            FinalizeShuffleRequest request, FinalizeShuffleResponse response);

    /** Capability-free immutable server allocation needed to recover the original generation. */
    record Allocation(String shuffleId, ShuffleServiceAddress service) {}
}
