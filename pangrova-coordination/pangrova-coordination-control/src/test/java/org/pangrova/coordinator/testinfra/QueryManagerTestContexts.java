package org.pangrova.coordinator.testinfra;

import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.submission.AsyncSubmitConfig;

import java.util.concurrent.ExecutorService;

/** Test-only query assembly. Each context receives an independent in-memory journal. */
public final class QueryManagerTestContexts {

    private QueryManagerTestContexts() {}

    /** Test assembly with an explicitly selected process-local journal. */
    public static QueryManagerContext minimal(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider) {
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null, null, null, null, null, null, null,
                new InMemoryQueryStateJournal(), null, null, null, null, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
    }

    /**
     * Test assembly with an explicit process-local journal and async configuration.
     * Used by tests to validate that query submission returns RUNNING
     * before full DAG execution completes.
     */
    public static QueryManagerContext minimalAsync(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider,
            AsyncSubmitConfig asyncSubmitConfig, ExecutorService asyncExecutor) {
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null, null, null, null, null, null, null,
                new InMemoryQueryStateJournal(), null, null,
                asyncSubmitConfig, asyncExecutor, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory());
    }
}
