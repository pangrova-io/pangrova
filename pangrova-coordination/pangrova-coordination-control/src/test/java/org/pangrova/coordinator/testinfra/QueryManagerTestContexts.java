package org.pangrova.coordinator.testinfra;

import org.pangrova.query.runtime.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.query.api.submission.AsyncSubmitConfig;

import java.util.concurrent.ExecutorService;

/** Test-only query assembly. Each context receives an independent in-memory journal. */
public final class QueryManagerTestContexts {

    private QueryManagerTestContexts() {}

    /** Test assembly with the default process-local journal. */
    public static QueryManagerContext minimal(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider) {
        return minimal(queryDispatcher, tracingProvider, new InMemoryQueryStateJournal());
    }

    public static QueryManagerContext minimal(QueryDispatcher queryDispatcher, TracingProvider tracingProvider,
                                               QueryStateJournal journal) {
        var audit = org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                null, null, null, queryDispatcher, null);
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null),
                audit,
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        journal, audit),
                null,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
    }

    /**
     * Test assembly with an explicit process-local journal and async configuration.
     * Used by tests to validate that query submission returns RUNNING
     * before full DAG execution completes.
     */
    public static QueryManagerContext minimalAsync(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider,
            AsyncSubmitConfig asyncSubmitConfig, ExecutorService asyncExecutor) {
        var audit = org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                null, null, null, queryDispatcher, null);
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null),
                audit,
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        new InMemoryQueryStateJournal(), audit),
                null, asyncSubmitConfig, asyncExecutor,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
    }
}
