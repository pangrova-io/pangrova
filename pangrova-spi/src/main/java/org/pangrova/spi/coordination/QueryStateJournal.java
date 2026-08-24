package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Stable;

import java.util.List;
import java.util.Optional;

/**
 * Write-ahead journal for query and task state transitions.
 * Enables Coordinator HA recovery by replaying persisted events.
 *
 * <p>Two durability tiers:
 * <ul>
 *   <li>Sync methods ({@code appendQueryEventSync}, {@code appendTaskEventSync}) —
 *       block until persisted. Used for critical transitions (QUEUED, FINISHED, FAILED).</li>
 *   <li>Async methods ({@code appendQueryEvent}, {@code appendTaskEvent}) —
 *       best-effort via bounded queue + background flush. Used for intermediate transitions.</li>
 * </ul>
 */
@Stable(since = "1.0")
public interface QueryStateJournal {

    /** Append a query state event asynchronously (best-effort, non-blocking). */
    void appendQueryEvent(QueryStateEvent event);

    /** Append a query state event synchronously (blocks until persisted). */
    void appendQueryEventSync(QueryStateEvent event);

    /** Append a task state event asynchronously (best-effort, non-blocking). */
    void appendTaskEvent(TaskStateEvent event);

    /** Append a task state event synchronously (blocks until persisted). */
    void appendTaskEventSync(TaskStateEvent event);

    /** Replay all non-terminal query events for HA recovery. */
    List<QueryStateEvent> replayActiveQueries();

    /** Replay all non-terminal task events for HA recovery. */
    List<TaskStateEvent> replayActiveTasks();

    /**
     * Replay non-terminal query events owned by a specific coordinator.
     * Used for owner-scoped recovery after failover (Task 51 / SI-6).
     * Default implementation filters {@link #replayActiveQueries()} in memory.
     */
    default List<QueryStateEvent> replayActiveQueriesForOwner(String coordinatorId) {
        return replayActiveQueries().stream()
                .filter(e -> coordinatorId.equals(e.coordinatorId()))
                .toList();
    }

    /**
     * Replay non-terminal task events owned by a specific coordinator.
     * Used for owner-scoped recovery after failover (Task 51 / SI-6).
     * Default implementation filters {@link #replayActiveTasks()} in memory.
     */
    default List<TaskStateEvent> replayActiveTasksForOwner(String coordinatorId) {
        return replayActiveTasks().stream()
                .filter(e -> coordinatorId.equals(e.coordinatorId()))
                .toList();
    }

    /**
     * Returns the most recent state event for the given query, if any journal
     * rows exist. Used as the cross-coordinator fallback for query-state lookups
     * in Active-Active deployments: a client polling a coordinator that did not
     * accept the query can still observe its current state from the shared
     * journal instead of receiving 404 (A-XNODE-01).
     *
     * <p>Default returns empty for backends without random-access lookup
     * (e.g. single-node in-memory journals, where the local state map is
     * already authoritative).</p>
     */
    default Optional<QueryStateEvent> latestQueryEvent(String queryId) {
        return Optional.empty();
    }

    /** Purge all events for a completed query (periodic cleanup). */
    void purgeQuery(String queryId);
}
