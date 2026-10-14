package org.pangrova.coordinator.coordination;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.ExecutionAnchorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Caches the exact durable execution anchors owned by this Coordinator process.
 *
 * <p>The cache is never authoritative. Acquisition, renewal, completion and
 * takeover are committed by {@link ExecutionAnchorStore}; the cache only avoids
 * coordination-store reads while stamping task-control messages. No result rows,
 * vector candidates, Top-K state or data-plane buffers are held here.</p>
 */
@ThreadSafe
final class QueryOwnershipRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(QueryOwnershipRegistry.class);
    private static final Logger ALARM_LOG =
            LoggerFactory.getLogger(HighAvailabilityCoordinationService.class);

    private final ConcurrentHashMap<String, ExecutionAnchor> ownership =
            new ConcurrentHashMap<>();
    // Admission remains concurrent; the short bulk renewal excludes cache mutations
    // so its durable row count can be compared against one stable local owner set.
    private final ReentrantReadWriteLock renewalBoundary = new ReentrantReadWriteLock(true);
    private final int maxEntries;
    private final Duration leaseDuration;
    private volatile ExecutionAnchorStore store;

    QueryOwnershipRegistry(
            int maxEntries,
            Duration leaseDuration,
            ExecutionAnchorStore store) {
        this.maxEntries = maxEntries;
        this.leaseDuration = leaseDuration;
        this.store = store;
    }

    void setStore(ExecutionAnchorStore replacement) {
        renewalBoundary.writeLock().lock();
        try {
            if (!ownership.isEmpty()) {
                throw new IllegalStateException(
                        "execution-anchor store cannot change while queries are owned");
            }
            this.store = replacement;
        } finally {
            renewalBoundary.writeLock().unlock();
        }
    }

    boolean acquire(String queryId, String coordinatorId, String incarnation) {
        renewalBoundary.readLock().lock();
        try {
            ExecutionAnchor cached = ownership.get(queryId);
            if (cached != null) {
                if (!cached.ownerId().equals(coordinatorId) || !cached.ownerIncarnation().equals(incarnation)) {
                    return false;
                }
                var current = store.current(queryId);
                if (current.filter(anchor -> sameOwner(anchor, cached)).isPresent()) {
                    return true;
                }
                ownership.remove(queryId, cached);
            }
            if (ownership.size() >= maxEntries) {
                ALARM_LOG.error("event=coordinator_alarm severity=CRITICAL "
                                + "component=query_execution_anchor "
                                + "message=\"local execution-anchor cache at capacity\" "
                                + "size={} maxEntries={} queryId={}",
                        ownership.size(), maxEntries, queryId);
                return false;
            }
            Optional<ExecutionAnchor> acquired = store.tryAcquire(
                    queryId, coordinatorId, incarnation, leaseDuration);
            if (acquired.isEmpty()) {
                acquired = store.current(queryId).filter(anchor -> anchor.ownerId().equals(coordinatorId)
                        && anchor.ownerIncarnation().equals(incarnation));
                if (acquired.isEmpty()) return false;
            }
            ExecutionAnchor anchor = acquired.orElseThrow();
            ExecutionAnchor existing = ownership.putIfAbsent(queryId, anchor);
            if (existing != null) {
                if (sameOwner(existing, anchor)) {
                    return true;
                }
                return anchor.fencingToken() > existing.fencingToken()
                        && ownership.replace(queryId, existing, anchor);
            }
            LOG.debug("event=execution_anchor_acquired queryId={} coordinatorId={} "
                            + "incarnation={} fencingToken={}",
                    queryId, coordinatorId, incarnation, anchor.fencingToken());
            return true;
        } finally {
            renewalBoundary.readLock().unlock();
        }
    }

    private static boolean sameOwner(ExecutionAnchor first, ExecutionAnchor second) {
        return first.executionId().equals(second.executionId()) && first.ownerId().equals(second.ownerId())
                && first.ownerIncarnation().equals(second.ownerIncarnation())
                && first.fencingToken() == second.fencingToken();
    }

    boolean release(String queryId, String coordinatorId, String incarnation) {
        renewalBoundary.readLock().lock();
        try {
            ExecutionAnchor anchor = ownership.get(queryId);
            if (anchor == null || !anchor.ownerId().equals(coordinatorId)
                    || !anchor.ownerIncarnation().equals(incarnation)) {
                return false;
            }
            boolean released = store.release(anchor);
            ownership.remove(queryId, anchor);
            if (released) {
                LOG.debug("event=execution_anchor_released queryId={} coordinatorId={} "
                                + "fencingToken={}",
                        queryId, coordinatorId, anchor.fencingToken());
            }
            return released;
        } finally {
            renewalBoundary.readLock().unlock();
        }
    }

    Optional<ExecutionAnchor> localAnchor(String executionId) {
        return Optional.ofNullable(ownership.get(executionId));
    }

    boolean isCurrentToken(String executionId, long fencingToken) {
        ExecutionAnchor anchor = ownership.get(executionId);
        return anchor != null && anchor.fencingToken() == fencingToken;
    }

    boolean isDurablyCurrent(String executionId, long fencingToken) {
        return store.current(executionId)
                .filter(anchor -> anchor.fencingToken() == fencingToken)
                .isPresent();
    }

    boolean renewOwned(String ownerId, String ownerIncarnation) {
        renewalBoundary.writeLock().lock();
        try {
            long expected = ownership.values().stream()
                    .filter(anchor -> anchor.ownerId().equals(ownerId)
                            && anchor.ownerIncarnation().equals(ownerIncarnation))
                    .count();
            if (expected == 0) {
                return true;
            }
            int renewed = store.renewOwned(ownerId, ownerIncarnation, leaseDuration);
            if (renewed == expected) {
                return true;
            }
            boolean lostOwnership = ownership.entrySet().removeIf(entry -> {
                ExecutionAnchor anchor = entry.getValue();
                return anchor.ownerId().equals(ownerId)
                        && anchor.ownerIncarnation().equals(ownerIncarnation)
                        && store.current(anchor.executionId())
                                .filter(current -> current.fencingToken()
                                        == anchor.fencingToken())
                                .isEmpty();
            });
            if (!lostOwnership) {
                // A changed batch count alone cannot prove that an anchor was lost;
                // only a stale fenced token can justify demotion.
                LOG.debug("event=execution_anchor_renewal_count_mismatch ownerId={} "
                                + "incarnation={} expected={} renewed={}",
                        ownerId, ownerIncarnation, expected, renewed);
                return true;
            }
            ALARM_LOG.error("event=coordinator_alarm severity=CRITICAL "
                            + "component=query_execution_anchor "
                            + "message=\"execution-anchor renewal lost ownership\" "
                            + "ownerId={} incarnation={} expected={} renewed={}",
                    ownerId, ownerIncarnation, expected, renewed);
            return false;
        } finally {
            renewalBoundary.writeLock().unlock();
        }
    }

    List<ExecutionAnchor> claimExpired(
            String newOwnerId, String newOwnerIncarnation, int maxBatchSize) {
        renewalBoundary.readLock().lock();
        try {
            int available = Math.max(0, maxEntries - ownership.size());
            if (available == 0) {
                return List.of();
            }
            List<ExecutionAnchor> claimed = store.claimExpired(
                    newOwnerId, newOwnerIncarnation, leaseDuration,
                    Math.min(available, maxBatchSize));
            for (ExecutionAnchor anchor : claimed) {
                ownership.put(anchor.executionId(), anchor);
            }
            return claimed;
        } finally {
            renewalBoundary.readLock().unlock();
        }
    }

    boolean yield(ExecutionAnchor anchor) {
        renewalBoundary.readLock().lock();
        try {
            boolean yielded = store.yield(anchor);
            ownership.remove(anchor.executionId(), anchor);
            return yielded;
        } finally {
            renewalBoundary.readLock().unlock();
        }
    }

    int yieldOwned(String ownerId, String ownerIncarnation) {
        renewalBoundary.writeLock().lock();
        try {
            int yielded = store.yieldOwned(ownerId, ownerIncarnation);
            ownership.entrySet().removeIf(entry -> {
                ExecutionAnchor anchor = entry.getValue();
                return anchor.ownerId().equals(ownerId)
                        && anchor.ownerIncarnation().equals(ownerIncarnation);
            });
            return yielded;
        } finally {
            renewalBoundary.writeLock().unlock();
        }
    }

    void clear() {
        renewalBoundary.writeLock().lock();
        try {
            ownership.clear();
        } finally {
            renewalBoundary.writeLock().unlock();
        }
    }
}
