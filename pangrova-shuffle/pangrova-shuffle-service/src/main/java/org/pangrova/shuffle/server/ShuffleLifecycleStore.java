package org.pangrova.shuffle.server;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.security.enums.CapabilityOperation;

import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupReceipt;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupShufflePage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupStart;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PublishedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.RegisterCommand;

/** Durable allocation, finalization, and cleanup transaction boundary. */
interface ShuffleLifecycleStore extends ShuffleCleanupStore, AutoCloseable {

    /** Opens the exact finalized partition and durably pins its objects in one transaction. */
    ReadAdmission openRead(String queryId, ExecutionOwnerIdentity owner, String shuffleId, int partitionId,
            long startSequence, long readUntilMs, long drainMillis);

    /** Renews frame and object-read admission under the exact live owner and pin. */
    long grantReadWindow(String queryId, ExecutionOwnerIdentity owner, UUID id);

    record ReadPin(UUID id, long protectedForMillis) {}

    record ReadAdmission(PublishedPartition published, ReadPin pin) {}

    void releaseRead(UUID id);

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

    /** Validates an exact task against the locked allocation and renews both durable expiries. */
    void renewTaskAccess(String shuffleId, ExecutionOwnerIdentity owner, long expiresAtMs,
            TaskAttemptId attempt, CapabilityOperation operation);

    Allocation register(RegisterCommand command);

    Optional<Allocation> findAllocation(String shuffleId);

    FinalizeResult finalizeShuffle(FinalizeCommand command);

    String beginManifestWrite(Allocation allocation, String objectKey, long recoveryDelayMs);

    void settleManifestWrite(String reservation, String shuffleId, String objectKey);

    @Override
    void close();
}

/** Durable cleanup claim, paging, lease-renewal, and completion boundary. */
interface ShuffleCleanupStore {

    /** Claims one durable prefix tombstone for a bounded compensation sweep. */
    Optional<String> claimUncertainSweep(String owner, long leaseMillis);

    /** Extends the exact sweep owner's lease before the next object-storage page. */
    void renewUncertainSweep(String shuffleId, String owner, long leaseMillis);

    /** Schedules the next sweep; an uncertain prefix is retained indefinitely. */
    void completeUncertainSweep(String shuffleId, String owner, long intervalMillis);

    CleanupStart beginCleanup(CleanupCommand command, CleanupLease lease);

    Optional<CleanupPlan> beginExpiredCleanup(CleanupLease lease);

    CleanupObjectPage cleanupObjects(
            CleanupPlan plan, CleanupCursor after, int limit, CleanupLease lease);

    CleanupShufflePage cleanupShuffles(
            CleanupPlan plan, String after, int limit, CleanupLease lease);

    void renewCleanup(CleanupPlan plan, CleanupLease lease);

    CleanupReceipt completeCleanup(CleanupPlan plan);
}

/** Executes one cleanup under a renewable durable lease using bounded pages. */
final class ShuffleCleanupCoordinator {
    private static final int PAGE_SIZE = 1_000;
    private static final long LEASE_MILLIS = 5 * 60 * 1_000L;
    private static final long UNCERTAIN_SWEEP_INTERVAL_MILLIS = 30_000L;

    private final ShuffleCleanupStore store;
    private final ShuffleObjectStorage objects;
    private final Clock clock;

    ShuffleCleanupCoordinator(
            ShuffleCleanupStore store,
            ShuffleObjectStorage objects,
            Clock clock) {
        this.store = store;
        this.objects = objects;
        this.clock = clock;
    }

    CleanupReceipt cleanup(CleanupCommand command) {
        String owner = UUID.randomUUID().toString();
        CleanupStart start = store.beginCleanup(command, lease(owner));
        return start instanceof CleanupPlan plan ? execute(plan) : (CleanupReceipt) start;
    }

    int recoverInterrupted(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("cleanup recovery limit must be positive");
        }
        int recovered = 0;
        while (recovered < limit) {
            String owner = UUID.randomUUID().toString();
            Optional<CleanupPlan> plan = store.beginExpiredCleanup(lease(owner));
            if (plan.isEmpty()) {
                break;
            }
            execute(plan.orElseThrow());
            recovered++;
        }
        return recovered;
    }

    int sweepUncertainPrefixes(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("uncertain sweep limit must be positive");
        }
        int swept = 0;
        while (swept < limit) {
            String owner = UUID.randomUUID().toString();
            Optional<String> shuffleId = store.claimUncertainSweep(owner, LEASE_MILLIS);
            if (shuffleId.isEmpty()) {
                break;
            }
            String id = shuffleId.orElseThrow();
            String after = null;
            do {
                store.renewUncertainSweep(id, owner, LEASE_MILLIS);
                after = objects.deletePrefixPage(id + "/", after, PAGE_SIZE);
            } while (after != null);
            store.completeUncertainSweep(id, owner, UNCERTAIN_SWEEP_INTERVAL_MILLIS);
            swept++;
        }
        return swept;
    }

    private CleanupReceipt execute(CleanupPlan plan) {
        deleteTrackedObjects(plan);
        deleteObjectPrefixes(plan);
        return store.completeCleanup(plan);
    }

    private void deleteTrackedObjects(CleanupPlan plan) {
        CleanupCursor cursor = null;
        while (true) {
            CleanupObjectPage page = store.cleanupObjects(
                    plan, cursor, PAGE_SIZE, lease(plan.leaseOwner()));
            if (page.objectKeys().isEmpty()) {
                return;
            }
            objects.delete(page.objectKeys());
            cursor = page.nextCursor();
        }
    }

    private void deleteObjectPrefixes(CleanupPlan plan) {
        String shuffleAfter = null;
        while (true) {
            CleanupShufflePage page = store.cleanupShuffles(
                    plan, shuffleAfter, PAGE_SIZE, lease(plan.leaseOwner()));
            if (page.shuffleIds().isEmpty()) {
                return;
            }
            for (String shuffleId : page.shuffleIds()) {
                deleteObjectPrefix(plan, shuffleId);
            }
            shuffleAfter = page.nextShuffleId();
        }
    }

    private void deleteObjectPrefix(CleanupPlan plan, String shuffleId) {
        String objectAfter = null;
        do {
            store.renewCleanup(plan, lease(plan.leaseOwner()));
            objectAfter = objects.deletePrefixPage(
                    shuffleId + "/", objectAfter, PAGE_SIZE);
        } while (objectAfter != null);
    }

    private CleanupLease lease(String owner) {
        long now = clock.millis();
        return new CleanupLease(owner, now, Math.addExact(now, LEASE_MILLIS));
    }
}
