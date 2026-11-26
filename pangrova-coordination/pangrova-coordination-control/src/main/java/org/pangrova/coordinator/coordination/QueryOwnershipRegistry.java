package org.pangrova.coordinator.coordination;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore;
import org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore.QueryAcquisition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Caches exact durable execution anchors for control-message stamping only.
 * The backend remains authoritative; no data-plane rows or buffers are held here.
 * Local entry transitions are atomic and never perform backend I/O under a lock.
 */
@ThreadSafe
final class QueryOwnershipRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(QueryOwnershipRegistry.class);
    private static final Logger ALARM_LOG = LoggerFactory.getLogger(HighAvailabilityCoordinationService.class);
    private final ConcurrentHashMap<String, Ownership> ownership = new ConcurrentHashMap<>();
    private final int maxEntries;
    private volatile Duration leaseDuration;
    private final ExecutionAnchorStore store;

    QueryOwnershipRegistry(int maxEntries, Duration leaseDuration, ExecutionAnchorStore store) {
        this.maxEntries = maxEntries;
        this.leaseDuration = leaseDuration;
        this.store = java.util.Objects.requireNonNull(store, "store");
    }

    void bindLeaseDuration(ExecutionAnchorStore backend, Duration duration) {
        if (store != backend) {
            throw new IllegalArgumentException(
                    "execution authority must use the same backend selected at construction");
        }
        if (!ownership.isEmpty()) {
            throw new IllegalStateException("execution lease duration cannot change while queries are owned");
        }
        this.leaseDuration = duration;
    }

    boolean usesStore(ExecutionAnchorStore backend) {
        return store == backend;
    }

    boolean acquire(String queryId, String coordinatorId, String incarnation) {
        return acquire(queryId, coordinatorId, incarnation, null).isPresent();
    }

    Optional<QueryAcquisition> acquire(String queryId, String coordinatorId, String incarnation, String tenantId) {
        return acquire(queryId, coordinatorId, incarnation, tenantId, OptionalInt.empty());
    }

    Optional<QueryAcquisition> acquire(String queryId, String coordinatorId, String incarnation,
            String tenantId, OptionalInt capacity) {
        Ownership cached = ownership.get(queryId);
        if (cached != null) {
            if (capacity.isPresent()) return Optional.empty();
            if (!cached.active() || !ownedBy(cached.anchor(), coordinatorId, incarnation)) {
                return Optional.empty();
            }
            var current = store.current(queryId).filter(anchor -> sameOwner(anchor, cached.anchor()));
            if (current.isPresent()) {
                Ownership latest = ownership.get(queryId);
                return latest != null && latest.active() && sameOwner(latest.anchor(), cached.anchor())
                        ? current.map(anchor -> new QueryAcquisition(anchor, OptionalLong.empty()))
                        : Optional.empty();
            }
            if (!ownership.remove(queryId, cached)) {
                return Optional.empty();
            }
        }
        if (ownership.size() >= maxEntries) {
            ALARM_LOG.error("event=coordinator_alarm severity=CRITICAL component=query_execution_anchor "
                            + "message=\"local execution-anchor cache at capacity\" size={} maxEntries={} queryId={}",
                    ownership.size(), maxEntries, queryId);
            return Optional.empty();
        }
        Ownership acquiring = new Ownership(null, false);
        if (ownership.putIfAbsent(queryId, acquiring) != null) {
            return Optional.empty();
        }
        try {
            Optional<QueryAcquisition> acquired = acquireFresh(queryId, coordinatorId, incarnation,
                    tenantId, capacity);
            if (acquired.isEmpty() && capacity.isEmpty()) {
                acquired = store.current(queryId).filter(anchor -> ownedBy(anchor, coordinatorId, incarnation))
                        .map(anchor -> new QueryAcquisition(anchor, OptionalLong.empty()));
            }
            if (acquired.isEmpty()) {
                return Optional.empty();
            }
            ExecutionAnchor anchor = acquired.orElseThrow().anchor();
            if (!ownership.replace(queryId, acquiring, new Ownership(anchor, false))) {
                return Optional.empty();
            }
            LOG.debug("event=execution_anchor_acquired queryId={} coordinatorId={} incarnation={} fencingToken={}",
                    queryId, coordinatorId, incarnation, anchor.fencingToken());
            return acquired;
        } finally {
            ownership.remove(queryId, acquiring);
        }
    }

    private Optional<QueryAcquisition> acquireFresh(String queryId, String coordinatorId, String incarnation,
            String tenantId, OptionalInt capacity) {
        if (capacity.isPresent()) {
            return store.tryAcquireLimitedQuery(queryId, coordinatorId, incarnation, leaseDuration,
                    tenantId, capacity.getAsInt());
        }
        if (tenantId != null) return store.tryAcquireQuery(queryId, coordinatorId, incarnation, leaseDuration,
                tenantId);
        return store.tryAcquire(queryId, coordinatorId, incarnation, leaseDuration)
                .map(anchor -> new QueryAcquisition(anchor, OptionalLong.empty()));
    }

    private static boolean sameOwner(ExecutionAnchor first, ExecutionAnchor second) {
        return first.executionId().equals(second.executionId())
                && ownedBy(first, second.ownerId(), second.ownerIncarnation())
                && first.fencingToken() == second.fencingToken();
    }

    private static boolean ownedBy(ExecutionAnchor anchor, String ownerId, String incarnation) {
        return anchor.ownerId().equals(ownerId) && anchor.ownerIncarnation().equals(incarnation);
    }

    boolean release(String queryId, String coordinatorId, String incarnation) {
        return retire(queryId, anchor -> ownedBy(anchor, coordinatorId, incarnation), store::release);
    }

    private boolean retire(String executionId, Predicate<ExecutionAnchor> matches,
            Predicate<ExecutionAnchor> backendOperation) {
        Ownership retiring = claimRetirement(executionId, matches);
        if (retiring == null) {
            return false;
        }
        try {
            boolean completed = backendOperation.test(retiring.anchor());
            ownership.remove(executionId, retiring);
            return completed;
        } catch (RuntimeException | Error failure) {
            ownership.replace(executionId, retiring, new Ownership(retiring.anchor(), false));
            throw failure;
        }
    }

    private Ownership claimRetirement(String executionId, Predicate<ExecutionAnchor> matches) {
        AtomicReference<Ownership> claimed = new AtomicReference<>();
        ownership.computeIfPresent(executionId, (id, current) -> {
            if (!current.active() || !matches.test(current.anchor())) {
                return current;
            }
            Ownership retiring = new Ownership(current.anchor(), true);
            claimed.set(retiring);
            return retiring;
        });
        return claimed.get();
    }

    Optional<ExecutionAnchor> localAnchor(String executionId) {
        Ownership local = ownership.get(executionId);
        return local != null && local.active() ? Optional.of(local.anchor()) : Optional.empty();
    }

    boolean isCurrentToken(String executionId, long fencingToken) {
        return localAnchor(executionId).filter(anchor -> anchor.fencingToken() == fencingToken).isPresent();
    }

    boolean isDurablyCurrent(String executionId, long fencingToken) {
        return store.current(executionId).filter(anchor -> anchor.fencingToken() == fencingToken).isPresent();
    }

    List<ExecutionAnchor> renewalSnapshot(String ownerId, String incarnation) {
        return ownership.values().stream().filter(Ownership::active).map(Ownership::anchor)
                .filter(anchor -> ownedBy(anchor, ownerId, incarnation)).toList();
    }

    boolean renewOwned(String ownerId, String incarnation) {
        List<ExecutionAnchor> expected = renewalSnapshot(ownerId, incarnation);
        if (expected.isEmpty()) {
            return true;
        }
        return applyRenewal(expected, store.renewOwned(ownerId, incarnation, leaseDuration));
    }

    boolean applyRenewal(List<ExecutionAnchor> expected, List<ExecutionAnchor> receipt) {
        var renewed = new HashMap<String, ExecutionAnchor>();
        for (ExecutionAnchor anchor : receipt) {
            if (renewed.putIfAbsent(anchor.executionId(), anchor) != null) {
                throw new IllegalStateException("Duplicate execution in authoritative renewal receipt");
            }
        }
        AtomicInteger lost = new AtomicInteger();
        for (ExecutionAnchor previous : expected) {
            ExecutionAnchor updated = renewed.get(previous.executionId());
            ownership.computeIfPresent(previous.executionId(), (id, current) -> {
                if (!current.active() || !sameOwner(current.anchor(), previous)) {
                    return current;
                }
                if (updated == null || !sameOwner(updated, previous)) {
                    lost.incrementAndGet();
                    return null;
                }
                return updated.expiresAt().isAfter(current.anchor().expiresAt())
                        ? new Ownership(updated, false) : current;
            });
        }
        if (lost.get() != 0) {
            ALARM_LOG.error("event=coordinator_alarm severity=CRITICAL component=query_execution_anchor "
                            + "message=\"execution-anchor renewal lost exact fences\" expected={} renewed={} lost={}",
                    expected.size(), receipt.size(), lost.get());
        }
        return lost.get() == 0;
    }

    List<ExecutionAnchor> claimExpired(String newOwnerId, String newOwnerIncarnation, int maxBatchSize) {
        int available = Math.max(0, maxEntries - ownership.size());
        if (available == 0) {
            return List.of();
        }
        List<ExecutionAnchor> claimed = store.claimExpired(newOwnerId, newOwnerIncarnation,
                leaseDuration, Math.min(available, maxBatchSize));
        for (ExecutionAnchor anchor : claimed) {
            ownership.put(anchor.executionId(), new Ownership(anchor, false));
        }
        return claimed;
    }

    boolean release(ExecutionAnchor anchor) {
        return retire(anchor.executionId(), current -> sameOwner(current, anchor), store::release);
    }

    boolean yield(ExecutionAnchor anchor) {
        return retire(anchor.executionId(), current -> sameOwner(current, anchor), store::yield);
    }

    int yieldOwned(String ownerId, String incarnation) {
        List<Ownership> retiring = new ArrayList<>();
        for (ExecutionAnchor anchor : renewalSnapshot(ownerId, incarnation)) {
            Ownership claimed = claimRetirement(anchor.executionId(), current -> sameOwner(current, anchor));
            if (claimed != null) {
                retiring.add(claimed);
            }
        }
        try {
            int yielded = store.yieldOwned(ownerId, incarnation);
            for (Ownership entry : retiring) {
                ownership.remove(entry.anchor().executionId(), entry);
            }
            return yielded;
        } catch (RuntimeException | Error failure) {
            for (Ownership entry : retiring) {
                ownership.replace(entry.anchor().executionId(), entry, new Ownership(entry.anchor(), false));
            }
            throw failure;
        }
    }

    void clear() {
        ownership.clear();
    }

    private static final class Ownership {
        private final ExecutionAnchor anchor;
        private final boolean retiring;

        private Ownership(ExecutionAnchor anchor, boolean retiring) {
            this.anchor = anchor;
            this.retiring = retiring;
        }

        private ExecutionAnchor anchor() {
            return anchor;
        }

        private boolean active() {
            return anchor != null && !retiring;
        }
    }
}
