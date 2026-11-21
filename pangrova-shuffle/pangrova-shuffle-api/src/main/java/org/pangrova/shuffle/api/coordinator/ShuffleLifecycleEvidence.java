package org.pangrova.shuffle.api.coordinator;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;
import org.pangrova.shuffle.api.ShuffleRegistration;

import java.util.Objects;
import java.util.Optional;

/** Records allocation and publication for a durable execution under its current owner. */
@Experimental(reason = "Durable exchange recovery")
public interface ShuffleLifecycleEvidence {
    /** Finds the original allocation for this stage and generation during recovery. */
    Optional<Allocation> restoredAllocation(ShuffleRegistration registration);

    /** Persists or confirms the registered allocation before the owner admits producers. */
    void registered(ShuffleRegistration registration, Allocation allocation, ShuffleExecutionOwner owner);

    /**
     * Records the validated receipt under the exact request owner. The journal receives its own copy of
     * the credential-free provider finalization response for replay and audit before publication becomes visible.
     */
    void finalized(ShuffleRegistration registration, Allocation allocation, ShuffleExecutionOwner requestOwner,
            ShuffleCommitReceipt receipt, byte[] providerResponse);

    /** Capability-free immutable server allocation needed to recover the original generation. */
    record Allocation(String shuffleId, ShuffleProviderDescriptor descriptor) {
        public Allocation {
            if (shuffleId == null || shuffleId.isBlank()) throw new IllegalArgumentException(
                    "Allocation ID is required");
            Objects.requireNonNull(descriptor, "descriptor");
        }
    }
}
