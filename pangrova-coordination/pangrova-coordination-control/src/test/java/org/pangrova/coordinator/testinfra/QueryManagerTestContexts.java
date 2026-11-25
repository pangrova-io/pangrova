package org.pangrova.coordinator.testinfra;

import org.pangrova.query.runtime.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.lifecycle.QueryAdmissionPort;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/** Test-only query assembly. Each context receives an independent in-memory journal. */
public final class QueryManagerTestContexts {
    public static final ScheduledExecutorService SUBMIT_DEADLINES =
            org.pangrova.common.concurrent.ManagedExecutorFactory.newSingleThreadScheduledExecutor("test-submit-deadline");

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
        return minimalAsync(queryDispatcher, tracingProvider, asyncSubmitConfig, asyncExecutor,
                new InMemoryQueryStateJournal());
    }

    public static QueryManagerContext minimalAsync(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider,
            AsyncSubmitConfig asyncSubmitConfig, ExecutorService asyncExecutor,
            QueryStateJournal journal) {
        return minimalAsync(queryDispatcher, tracingProvider, asyncSubmitConfig, asyncExecutor, journal,
                org.pangrova.coordinator.query.QueryManagerCollaborators.admission(null, null));
    }

    public static QueryManagerContext minimalAsync(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider,
            AsyncSubmitConfig asyncSubmitConfig, ExecutorService asyncExecutor,
            QueryStateJournal journal, QueryAdmissionPort admission) {
        var audit = org.pangrova.coordinator.query.QueryManagerCollaborators.audit(
                null, null, null, queryDispatcher, null);
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null, admission, audit,
                org.pangrova.coordinator.query.QueryManagerCollaborators.journal(
                        journal, audit),
                null, asyncSubmitConfig, asyncExecutor, SUBMIT_DEADLINES,
                new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
    }
}
