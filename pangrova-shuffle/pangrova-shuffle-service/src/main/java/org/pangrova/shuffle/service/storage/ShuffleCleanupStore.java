package org.pangrova.shuffle.service.storage;

import java.util.Optional;

import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupShufflePage;

/** Durable cleanup claim, paging, lease-renewal, and completion boundary. */
public interface ShuffleCleanupStore {

    /** Claims one durable prefix tombstone for a bounded compensation sweep. */
    Optional<String> claimUncertainSweep(String owner, long leaseMillis);

    /** Extends the exact sweep owner's lease before the next object-storage page. */
    void renewUncertainSweep(String shuffleId, String owner, long leaseMillis);

    /** Schedules the next sweep; an uncertain prefix is retained indefinitely. */
    void completeUncertainSweep(String shuffleId, String owner, long intervalMillis);

    Optional<CleanupPlan> beginCleanup(CleanupCommand command, CleanupLease lease);

    Optional<CleanupPlan> beginExpiredCleanup(CleanupLease lease);

    CleanupObjectPage cleanupObjects(
            CleanupPlan plan, CleanupCursor after, int limit, CleanupLease lease);

    CleanupShufflePage cleanupShuffles(
            CleanupPlan plan, String after, int limit, CleanupLease lease);

    void renewCleanup(CleanupPlan plan, CleanupLease lease);

    void completeCleanup(CleanupPlan plan);
}
