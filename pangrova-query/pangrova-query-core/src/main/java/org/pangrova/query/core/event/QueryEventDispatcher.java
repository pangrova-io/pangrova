package org.pangrova.query.core.event;

import org.pangrova.query.api.event.QueryCompletedEvent;
import org.pangrova.query.api.event.QueryCreatedEvent;
import org.pangrova.query.api.event.QueryStatistics;
import org.pangrova.query.api.event.SplitCompletedEvent;
import org.pangrova.query.spi.event.QueryEventListener;

import org.pangrova.query.api.lifecycle.QueryState;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fans query lifecycle events out to all registered {@link QueryEventListener}s.
 *
 * <p>Each callback is fault-isolated: a listener that throws is logged and skipped so it
 * can neither abort the query execution thread nor suppress other listeners. This is the
 * unified bus through which audit, lineage, and cost integrations observe queries.</p>
 */
@Experimental
public final class QueryEventDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(QueryEventDispatcher.class);

    private final List<QueryEventListener> listeners;
    private final QueryEventDispatcher parent;

    public QueryEventDispatcher(List<QueryEventListener> listeners) {
        this(listeners, null);
    }

    private QueryEventDispatcher(List<QueryEventListener> listeners, QueryEventDispatcher parent) {
        this.parent = parent;
        Objects.requireNonNull(listeners, "listeners must not be null");
        this.listeners = new CopyOnWriteArrayList<>(listeners);
    }

    /** @return a dispatcher with the given listeners */
    public static QueryEventDispatcher of(QueryEventListener... listeners) {
        return new QueryEventDispatcher(List.of(listeners));
    }

    /**
     * Attaches a listener owned by one query manager without mutating a borrowed dispatcher.
     * Parent registrations remain live; the parent never retains this owner's listener.
     */
    public QueryEventDispatcher withListener(QueryEventListener listener) {
        return new QueryEventDispatcher(List.of(Objects.requireNonNull(listener, "listener")), this);
    }

    /** Registers an additional listener after construction. */
    public void register(QueryEventListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener must not be null"));
    }

    /** @return true when no listeners are registered (callers may skip event construction) */
    public boolean isEmpty() {
        return listeners.isEmpty() && (parent == null || parent.isEmpty());
    }

    /** Fires {@link QueryEventListener#queryCreated} on every listener, fault-isolated. */
    public void fireQueryCreated(QueryCreatedEvent event) {
        if (parent != null) parent.fireQueryCreated(event);
        for (QueryEventListener listener : listeners) {
            try {
                listener.queryCreated(event);
            } catch (RuntimeException e) {
                LOG.warn("event=query_event_listener_failed phase=created listener={} queryId={} error={}",
                        listener.getClass().getName(), event.queryId(), e.getMessage(), e);
            }
        }
    }

    /** Fires {@link QueryEventListener#queryCompleted} on every listener, fault-isolated. */
    public void fireQueryCompleted(QueryCompletedEvent event) {
        if (parent != null) parent.fireQueryCompleted(event);
        for (QueryEventListener listener : listeners) {
            try {
                listener.queryCompleted(event);
            } catch (RuntimeException e) {
                LOG.warn("event=query_event_listener_failed phase=completed listener={} queryId={} error={}",
                        listener.getClass().getName(), event.queryId(), e.getMessage(), e);
            }
        }
    }

    /** Fires {@link QueryEventListener#splitCompleted} on every listener, fault-isolated. */
    public void fireSplitCompleted(SplitCompletedEvent event) {
        if (parent != null) parent.fireSplitCompleted(event);
        for (QueryEventListener listener : listeners) {
            try {
                listener.splitCompleted(event);
            } catch (RuntimeException e) {
                LOG.warn("event=query_event_listener_failed phase=split_completed listener={} "
                                + "queryId={} splitId={} error={}",
                        listener.getClass().getName(), event.queryId(), event.splitId(), e.getMessage(), e);
            }
        }
    }

    /**
     * Emits a security-denial event as a failed {@link QueryCompletedEvent} through
     * the EventListener SPI. Routes ACCESS_DENIED / TABLE_WRITE / TABLE_DELETE
     * denials so audit and lineage listeners observe them without requiring
     * separate security-specific callbacks.
     *
     * @param queryId          query that was denied (may be synthetic if no query context)
     * @param principal        principal that was denied
     * @param denialMessage    human-readable denial description
     */
    public void fireSecurityDenied(String queryId, String principal, String denialMessage) {
        QueryCompletedEvent event = new QueryCompletedEvent(
                queryId != null ? queryId : "security-denied",
                null,
                principal != null ? principal : "anonymous",
                QueryState.FAILED,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                QueryStatistics.wallTimeOnly(0),
                Optional.of(denialMessage != null ? denialMessage : "ACCESS_DENIED"));
        fireQueryCompleted(event);
    }
}
