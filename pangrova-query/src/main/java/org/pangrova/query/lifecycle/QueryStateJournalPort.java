package org.pangrova.query.lifecycle;

import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.QueryStateJournalReader;
import org.pangrova.query.api.journal.TaskStateEvent;

/** Sole durable query/task journal owner exposed to the query lifecycle. */
public interface QueryStateJournalPort extends QueryStateJournalReader, AutoCloseable {

    void appendQueryEvent(QueryStateEvent event);

    void appendQueryEventSync(QueryStateEvent event);

    void appendTaskEvent(TaskStateEvent event);

    void appendTaskEventSync(TaskStateEvent event);

    void purgeQuery(String queryId);

    @Override
    void close();

    boolean isDrained();
}
