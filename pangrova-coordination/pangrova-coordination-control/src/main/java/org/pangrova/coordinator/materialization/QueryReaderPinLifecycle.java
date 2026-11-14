package org.pangrova.coordinator.materialization;

import java.util.Collection;
import java.util.List;
import org.pangrova.query.api.retention.ReaderPinLifecycle;
import org.pangrova.materialization.api.domain.QueryId;
import org.pangrova.materialization.usecase.ReaderPinLeases;

/** Binds query lifecycle events to durable materialization reader-pin leases. */
public final class QueryReaderPinLifecycle implements ReaderPinLifecycle {

    private final ReaderPinLeases leases;

    public QueryReaderPinLifecycle(ReaderPinLeases leases) {
        this.leases = java.util.Objects.requireNonNull(leases, "leases");
    }

    @Override
    public void release(String queryId) {
        leases.release(new QueryId(queryId));
    }

    @Override
    public void renew(Collection<String> queryIds) {
        List<QueryId> queries = queryIds.stream().map(QueryId::new).toList();
        leases.renew(queries);
    }

    @Override
    public long renewIntervalNanos() {
        return leases.renewInterval().toNanos();
    }
}
