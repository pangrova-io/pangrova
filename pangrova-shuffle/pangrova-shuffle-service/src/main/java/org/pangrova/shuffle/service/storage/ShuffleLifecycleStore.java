package org.pangrova.shuffle.service.storage;

import java.util.Optional;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.shuffle.service.model.ShuffleServiceModel;

import static org.pangrova.shuffle.service.model.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.FinalizeCommand;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.FinalizeResult;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.RegisterCommand;

/** Durable allocation, finalization, and cleanup transaction boundary. */
public interface ShuffleLifecycleStore extends ShuffleCleanupStore, AutoCloseable {

    /** Requires the exact owner to still hold the authoritative execution anchor. */
    void requireLiveOwner(String queryId, ExecutionOwnerIdentity owner);

    /** Fenced Coordinator recovery of an ACTIVE query cleanup scope. */
    void recoverCleanupAccess(
            String queryId, ExecutionOwnerIdentity owner, long expiresAtMs);

    /** Fenced Coordinator recovery of a registered or finalized shuffle. */
    void recoverFinalizeAccess(
            String shuffleId, ExecutionOwnerIdentity owner, long expiresAtMs);

    /** Fenced monotonic renewal of durable shuffle and query-scope expiry. */
    void renewAccess(
            String queryId,
            String shuffleId,
            ExecutionOwnerIdentity owner,
            long expiresAtMs);

    Allocation register(RegisterCommand command);

    Optional<Allocation> findAllocation(String shuffleId);

    FinalizeResult finalizeShuffle(FinalizeCommand command);

    Optional<FinalizeResult> findFinalization(String shuffleId);

    @Override
    void close();
}
