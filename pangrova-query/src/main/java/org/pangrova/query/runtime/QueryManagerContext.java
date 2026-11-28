package org.pangrova.query.runtime;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.query.event.QueryEventDispatcher;
import org.pangrova.query.lifecycle.QueryAdmissionPort;
import org.pangrova.query.lifecycle.QueryAuditPort;
import org.pangrova.query.lifecycle.QueryExecutionPort;
import org.pangrova.query.lifecycle.QueryJournalLifecyclePort;
import org.pangrova.query.lifecycle.TenantAdmissionPort;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * QueryManager dependencies supplied by the process composition root.
 * The journal is mandatory: domain code must never choose a persistence backend.
 * The process owner closes the journal after query drain; QueryManager borrows it.
 */
public record QueryManagerContext(
        QueryExecutionPort queryDispatcher,
        TracingProvider tracingProvider,
        MetricsProvider metricsProvider,
        TenantAdmissionPort tenantAdmission,
        QueryAdmissionPort admission,
        QueryAuditPort audit,
        QueryJournalLifecyclePort journal,
        QueryEventDispatcher eventDispatcher,
        AsyncSubmitConfig asyncSubmitConfig,
        ExecutorService asyncExecutor,
        ScheduledExecutorService submitDeadlines,
        ExecutionTraceContextFactory traceContexts) {
    public QueryManagerContext {
        checkNotNull(traceContexts, "traceContexts must be selected explicitly");
        checkNotNull(tracingProvider, "tracingProvider must not be null");
        checkNotNull(journal, "stateJournal must be selected explicitly");
        checkNotNull(queryDispatcher, "queryDispatcher must not be null");
        if (asyncSubmitConfig != null && asyncSubmitConfig.enabled() && asyncExecutor != null) {
            checkNotNull(submitDeadlines, "async submission requires a deadline scheduler");
        }
    }

    /** Synchronous submission without async executor or event dispatcher. */
    public QueryManagerContext(
            QueryExecutionPort queryDispatcher,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            TenantAdmissionPort tenantAdmission,
            QueryAdmissionPort admission,
            QueryAuditPort audit,
            QueryJournalLifecyclePort journal,
            QueryEventDispatcher eventDispatcher,
            ExecutionTraceContextFactory traceContexts) {
        this(queryDispatcher, tracingProvider, metricsProvider, tenantAdmission,
                admission, audit, journal, eventDispatcher, null, null, null, traceContexts);
    }
}
