package org.pangrova.persistence.api.query.state;

import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.spi.query.state.TaskStateEvent;

import org.pangrova.common.api.annotation.Stable;

/**
 * One owner for immutable query/task journal facts.
 * Sync methods wait for actual commit; async methods transfer an admitted event to
 * the same bounded owner and reject closed/full admission. NONE is process-local,
 * not a durable issued-attempt or HA authority.
 */
@Stable(since = "1.0")
public interface QueryStateJournal extends QueryStateJournalReader, AutoCloseable {
    /** Admits a query event or throws without accepting it; accepted failures remain owned. */
    void appendQueryEvent(QueryStateEvent event);
    /** Commits the exact frozen query event or reports an unestablished outcome for retry. */
    void appendQueryEventSync(QueryStateEvent event);
    /** Admits a task event or throws without accepting it; accepted failures remain owned. */
    void appendTaskEvent(TaskStateEvent event);
    /** Commits the exact frozen task event or reports an unestablished outcome for retry. */
    void appendTaskEventSync(TaskStateEvent event);
    /** Excludes concurrent query admission, drains admitted writes and atomically deletes its facts. */
    void purgeQuery(String queryId);
    /** Seals admission and waits for actual owned operations to exit; failure is retryable. */
    @Override void close();
    /**
     * True only after admission is sealed and every actual writer, entered operation and connection has exited.
     * This is resource evidence, not commit success; a sticky semantic failure still makes every close throw.
     */
    boolean isDrained();
}
