package org.pangrova.coordinator.query;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.query.api.submission.AsyncSubmitConfig;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.worker.ClusterResourcePressureMonitor;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.query.api.audit.QueryAuditSink;
import org.pangrova.query.event.QueryEventDispatcher;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.security.tenant.QuotaChecker;
import org.pangrova.security.tenant.TenantResolver;
import org.pangrova.planner.throttle.QueryThrottlingService;

import java.util.concurrent.ExecutorService;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * QueryManager dependencies supplied by the process composition root.
 * The journal is mandatory: domain code must never choose a persistence backend.
 * The process owner closes the journal after query drain; QueryManager borrows it.
 *
 * <p>Task 186: async submit fields ({@code asyncSubmitConfig}, {@code asyncExecutor})
 * are optional -- when present, QueryManager returns RUNNING as soon as the
 * OUTPUT stage is dispatched rather than blocking until full DAG completion.</p>
 */
public record QueryManagerContext(
        QueryDispatcher queryDispatcher,
        TracingProvider tracingProvider,
        MetricsProvider metricsProvider,
        TenantResolver tenantResolver,
        QuotaChecker quotaChecker,
        LineageEventEmitter lineageEmitter,
        AuditEventEmitter auditEmitter,
        QueryAuditSink queryAuditService,
        ResilienceRegistry resilienceRegistry,
        QueryThrottlingService throttlingService,
        QueryStateJournal stateJournal,
        ClusterResourcePressureMonitor clusterPressureMonitor,
        QueryEventDispatcher eventDispatcher,
        AsyncSubmitConfig asyncSubmitConfig,
        ExecutorService asyncExecutor,
        ExecutionTraceContextFactory traceContexts) {
    public QueryManagerContext {
        checkNotNull(traceContexts, "traceContexts must be selected explicitly");
        checkNotNull(tracingProvider, "tracingProvider must not be null");
        checkNotNull(stateJournal, "stateJournal must be selected explicitly");
    }

    /** Synchronous submission dependencies with no async executor. */
    // CHECKSTYLE.OFF: ParameterNumber -- wiring context aggregation kept stable for callers
    public QueryManagerContext(
            QueryDispatcher queryDispatcher,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            TenantResolver tenantResolver,
            QuotaChecker quotaChecker,
            LineageEventEmitter lineageEmitter,
            AuditEventEmitter auditEmitter,
            QueryAuditSink queryAuditService,
            ResilienceRegistry resilienceRegistry,
            QueryThrottlingService throttlingService,
            QueryStateJournal stateJournal,
            ClusterResourcePressureMonitor clusterPressureMonitor,
            QueryEventDispatcher eventDispatcher, ExecutionTraceContextFactory traceContexts) {
        this(queryDispatcher, tracingProvider, metricsProvider,
                tenantResolver, quotaChecker, lineageEmitter,
                auditEmitter, queryAuditService, resilienceRegistry,
                throttlingService, stateJournal,
                clusterPressureMonitor, eventDispatcher, null, null, traceContexts);
    }
    // CHECKSTYLE.ON: ParameterNumber

    /** Dependencies without an optional event dispatcher. */
    // CHECKSTYLE.OFF: ParameterNumber -- wiring context aggregation kept stable for callers
    public QueryManagerContext(
            QueryDispatcher queryDispatcher,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            TenantResolver tenantResolver,
            QuotaChecker quotaChecker,
            LineageEventEmitter lineageEmitter,
            AuditEventEmitter auditEmitter,
            QueryAuditSink queryAuditService,
            ResilienceRegistry resilienceRegistry,
            QueryThrottlingService throttlingService,
            QueryStateJournal stateJournal,
            ClusterResourcePressureMonitor clusterPressureMonitor, ExecutionTraceContextFactory traceContexts) {
        this(queryDispatcher, tracingProvider, metricsProvider,
                tenantResolver, quotaChecker, lineageEmitter,
                auditEmitter, queryAuditService, resilienceRegistry,
                throttlingService, stateJournal,
                clusterPressureMonitor, null, null, null, traceContexts);
    }
    // CHECKSTYLE.ON: ParameterNumber

    /** Dependencies without optional pressure monitoring. */
    // CHECKSTYLE.OFF: ParameterNumber -- wiring context aggregation kept stable for callers
    public QueryManagerContext(
            QueryDispatcher queryDispatcher,
            TracingProvider tracingProvider,
            MetricsProvider metricsProvider,
            TenantResolver tenantResolver,
            QuotaChecker quotaChecker,
            LineageEventEmitter lineageEmitter,
            AuditEventEmitter auditEmitter,
            QueryAuditSink queryAuditService,
            ResilienceRegistry resilienceRegistry,
            QueryThrottlingService throttlingService,
            QueryStateJournal stateJournal, ExecutionTraceContextFactory traceContexts) {
        this(queryDispatcher, tracingProvider, metricsProvider,
                tenantResolver, quotaChecker, lineageEmitter,
                auditEmitter, queryAuditService, resilienceRegistry,
                throttlingService, stateJournal, null, traceContexts);
    }
    // CHECKSTYLE.ON: ParameterNumber

}
