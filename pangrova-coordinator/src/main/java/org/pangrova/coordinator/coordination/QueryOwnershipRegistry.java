package org.pangrova.coordinator.coordination;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService.OwnershipEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks which coordinator owns which in-flight query, at which leader epoch.
 *
 * <p>Extracted from {@link HighAvailabilityCoordinationService} to keep that class within the
 * project class-size budget. The epoch is supplied by the caller rather than owned here, because
 * it is leader-election state: this registry only stamps and compares it.</p>
 *
 * <p>On demotion or shutdown the owned queries are persisted as orphans so the incoming leader
 * can reclaim them; the map is only cleared afterwards.</p>
 */
@ThreadSafe
final class QueryOwnershipRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(QueryOwnershipRegistry.class);

    /**
     * Operational alarms are emitted under the {@link HighAvailabilityCoordinationService} logger.
     * This registry is an internal collaborator of that service, and the alarms describe HA
     * failover health rather than registry internals — operator alert rules and tests key on the
     * owning service's logger name, so extracting this class must not move them elsewhere.
     */
    private static final Logger ALARM_LOG =
            LoggerFactory.getLogger(HighAvailabilityCoordinationService.class);

    private final ConcurrentHashMap<String, OwnershipEntry> ownership = new ConcurrentHashMap<>();
    private final int maxEntries;

    private volatile OrphanQueryReclaimer reclaimer;

    QueryOwnershipRegistry(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    void setReclaimer(OrphanQueryReclaimer reclaimer) {
        this.reclaimer = reclaimer;
    }

    /**
     * Claims ownership of a query at the current leader epoch.
     *
     * <p>BUG-D01: the epoch must be the leader term as-is, never incremented per query. Incrementing
     * on every ownership call inflated the counter and broke epoch fencing, letting a standby use
     * the inflated epoch to force-replace an active leader's lease.</p>
     *
     * @return {@code false} when the query is already owned, or the registry is at capacity
     */
    boolean acquire(String queryId, String coordinatorId, long epoch) {
        if (ownership.size() >= maxEntries) {
            // A-OWN-01: capacity exhaustion is fail-closed (reject to protect the coordinator), but it
            // is also an operational emergency — new queries cannot be tracked/recovered and it usually
            // signals an ownership leak. Raise a CRITICAL alarm instead of a quiet warning.
            ALARM_LOG.error("event=coordinator_alarm severity=CRITICAL component=ha_query_ownership "
                    + "message=\"query-ownership map at capacity; rejecting new ownership (possible "
                    + "ownership leak)\" size={} maxEntries={} queryId={}",
                    ownership.size(), maxEntries, queryId);
            return false;
        }
        boolean acquired = ownership.putIfAbsent(queryId, new OwnershipEntry(coordinatorId, epoch)) == null;
        if (acquired) {
            LOG.debug("event=query_ownership_acquired queryId={} coordinator={} epoch={}",
                    queryId, coordinatorId, epoch);
        }
        return acquired;
    }

    /** Releases ownership only if {@code coordinatorId} is the recorded owner. */
    boolean release(String queryId, String coordinatorId) {
        OwnershipEntry entry = ownership.get(queryId);
        if (entry == null || !entry.coordinatorId().equals(coordinatorId)) {
            return false;
        }
        boolean released = ownership.remove(queryId, entry);
        if (released) {
            LOG.debug("event=query_ownership_released queryId={} coordinator={}", queryId, coordinatorId);
        }
        return released;
    }

    /**
     * Re-stamps every query owned by {@code from} to {@code to} at {@code newEpoch}.
     *
     * @return the number of entries actually transferred
     */
    int transfer(String from, String to, long newEpoch) {
        int transferred = 0;
        for (Map.Entry<String, OwnershipEntry> e : ownership.entrySet()) {
            if (e.getValue().coordinatorId().equals(from)
                    && ownership.replace(e.getKey(), e.getValue(), new OwnershipEntry(to, newEpoch))) {
                transferred++;
            }
        }
        if (transferred > 0) {
            LOG.info("event=ownership_transferred from={} to={} count={} epoch={}",
                    from, to, transferred, newEpoch);
        }
        return transferred;
    }

    /** Zombie fencing: a claim is valid only at the epoch the ownership was stamped with. */
    boolean isCurrentEpoch(String queryId, long queryEpoch) {
        OwnershipEntry entry = ownership.get(queryId);
        return entry != null && queryEpoch == entry.epoch();
    }

    /**
     * Persists all currently owned queries as orphans so a new leader can reclaim them. A no-op
     * without a reclaimer or with nothing owned. Must be called before {@link #clear()}.
     */
    void persistOrphans(String coordinatorId) {
        OrphanQueryReclaimer target = this.reclaimer;
        if (target == null || ownership.isEmpty()) {
            return;
        }
        try {
            // Persist ALL owned queries — each entry carries its own coordinatorId. On graceful
            // shutdown we want everything this node owns, regardless of whether localNodeId was set.
            List<OrphanQueryReclaimer.OrphanedQuery> orphans = ownership.entrySet().stream()
                    .map(e -> new OrphanQueryReclaimer.OrphanedQuery(
                            e.getKey(), e.getValue().coordinatorId(), null, null, 0))
                    .toList();
            String effectiveId = coordinatorId != null ? coordinatorId : "unknown";
            target.releaseQueries(effectiveId, orphans);
            LOG.info("event=orphaned_queries_persisted coordinator={} count={}",
                    effectiveId, orphans.size());
        } catch (Exception e) {
            // NEW-COORD-02: persisting orphaned queries is data-safety critical — if it fails, the
            // in-flight queries this coordinator owns will NOT be reclaimed by the new leader and
            // are effectively lost. Raise a CRITICAL alarm so the failure cannot pass unnoticed.
            ALARM_LOG.error("event=coordinator_alarm severity=CRITICAL component=ha_failover "
                    + "message=\"failed to persist orphaned queries; in-flight queries owned by "
                    + "coordinator may be lost and not reclaimed on failover\" coordinator={} error={}",
                    coordinatorId, e.getMessage(), e);
        }
    }

    void clear() {
        ownership.clear();
    }
}
