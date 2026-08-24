package org.pangrova.metadata.cache;

import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.coordination.CoordinatorRegistry;
import org.pangrova.spi.coordination.CoordinatorSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Waits for DDL invalidation to propagate to all coordinators.
 *
 * <p>After a DDL operation writes to the {@code invalidation_log}, this waiter
 * polls the {@link CoordinatorRegistry} to verify that every active coordinator
 * has processed the invalidation entry (their
 * {@link CoordinatorSnapshot#lastProcessedInvalidationId()} {@code >= targetId}).
 * Times out after a configurable duration (default 5s) and returns success
 * anyway — eventual consistency is acceptable as a fallback.</p>
 *
 * <p>Each coordinator reports its {@code lastProcessedInvalidationId} via
 * heartbeat, stored in the coordinator registry. This avoids extra HTTP
 * calls — the registry already has the data.</p>
 */
public final class DdlPropagationWaiter {

    private static final Logger LOG = LoggerFactory.getLogger(DdlPropagationWaiter.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(30);

    private final CoordinatorRegistry coordinatorRegistry;
    private final JdbcInvalidationPoller localPoller;
    private final Duration timeout;

    public DdlPropagationWaiter(CoordinatorRegistry coordinatorRegistry,
                                 JdbcInvalidationPoller localPoller) {
        this(coordinatorRegistry, localPoller, DEFAULT_TIMEOUT);
    }

    public DdlPropagationWaiter(CoordinatorRegistry coordinatorRegistry,
                                 JdbcInvalidationPoller localPoller,
                                 Duration timeout) {
        this.coordinatorRegistry = coordinatorRegistry;
        this.localPoller = localPoller;
        this.timeout = timeout;
    }

    /**
     * Waits for all active coordinators to process invalidation entries up to
     * the current local {@code lastProcessedId}. Returns true if all coordinators
     * caught up, false on timeout (eventual consistency fallback).
     */
    public boolean waitForPropagation() {
        long targetId = localPoller.getLastProcessedId();
        if (targetId <= 0) {
            return true;
        }
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (allCoordinatorsCaughtUp(targetId)) {
                LOG.debug("event=ddl_propagation_complete targetId={}", targetId);
                return true;
            }
            ThreadUtils.sleepQuietly(POLL_INTERVAL);
            if (Thread.currentThread().isInterrupted()) {
                LOG.warn("event=ddl_propagation_wait_interrupted targetId={}", targetId);
                return false;
            }
        }
        LOG.info("event=ddl_propagation_timeout targetId={} timeout={}ms",
                targetId, timeout.toMillis());
        return false;
    }

    /**
     * Checks whether every active coordinator has processed invalidation entries
     * up to the target ID. Coordinators that don't report a
     * {@code lastProcessedInvalidationId} (value {@code -1}) are treated as
     * not-yet-caught-up — they may be running an older version without
     * invalidation tracking in heartbeat.
     */
    private boolean allCoordinatorsCaughtUp(long targetId) {
        List<CoordinatorSnapshot> coordinators =
                coordinatorRegistry.getAllCoordinators(STALE_THRESHOLD);
        if (coordinators.isEmpty()) {
            return true;
        }
        for (CoordinatorSnapshot coord : coordinators) {
            long processed = coord.lastProcessedInvalidationId();
            if (processed < targetId) {
                LOG.debug("event=ddl_propagation_pending coordinator={} processed={} target={}",
                        coord.coordinatorId(), processed, targetId);
                return false;
            }
        }
        return true;
    }
}
