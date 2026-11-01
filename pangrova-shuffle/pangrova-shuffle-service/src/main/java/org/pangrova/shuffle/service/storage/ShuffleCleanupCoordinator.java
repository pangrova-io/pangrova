package org.pangrova.shuffle.service.storage;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;

import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CleanupShufflePage;

/** Executes one cleanup under a renewable durable lease using bounded pages. */
public final class ShuffleCleanupCoordinator {
    private static final int PAGE_SIZE = 1_000;
    private static final long LEASE_MILLIS = 5 * 60 * 1_000L;
    private static final long UNCERTAIN_SWEEP_INTERVAL_MILLIS = 30_000L;

    private final ShuffleCleanupStore store;
    private final ShuffleObjectStorage objects;
    private final Clock clock;

    public ShuffleCleanupCoordinator(
            ShuffleCleanupStore store,
            ShuffleObjectStorage objects,
            Clock clock) {
        this.store = store;
        this.objects = objects;
        this.clock = clock;
    }

    public Optional<CleanupPlan> cleanup(CleanupCommand command) {
        String owner = UUID.randomUUID().toString();
        Optional<CleanupPlan> plan = store.beginCleanup(command, lease(owner));
        plan.ifPresent(this::execute);
        return plan;
    }

    public int recoverInterrupted(int limit) {
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

    public int sweepUncertainPrefixes(int limit) {
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

    private void execute(CleanupPlan plan) {
        deleteTrackedObjects(plan);
        deleteObjectPrefixes(plan);
        store.completeCleanup(plan);
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
