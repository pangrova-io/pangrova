package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.AsyncSubmitConfig;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.worker.ClusterResourcePressureMonitor;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.spi.query.audit.QueryAuditSink;
import org.pangrova.spi.observability.event.QueryEventDispatcher;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.security.tenant.QuotaChecker;
import org.pangrova.security.tenant.TenantResolver;
import org.pangrova.planner.throttle.QueryThrottlingService;

import java.util.concurrent.ExecutorService;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Groups QueryManager dependencies into a single parameter object.
 * Keeps the constructor within the manageable param governance limit.
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
        ExecutorService asyncExecutor
) {
    public QueryManagerContext {
        checkNotNull(tracingProvider, "tracingProvider must not be null");
    }

    /** Backward-compatible 13-arg constructor -- null async config and executor. */
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
            QueryEventDispatcher eventDispatcher) {
        this(queryDispatcher, tracingProvider, metricsProvider,
                tenantResolver, quotaChecker, lineageEmitter,
                auditEmitter, queryAuditService, resilienceRegistry,
                throttlingService, stateJournal,
                clusterPressureMonitor, eventDispatcher, null, null);
    }
    // CHECKSTYLE.ON: ParameterNumber

    /** Backward-compatible 12-arg constructor -- null event dispatcher. */
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
            ClusterResourcePressureMonitor clusterPressureMonitor) {
        this(queryDispatcher, tracingProvider, metricsProvider,
                tenantResolver, quotaChecker, lineageEmitter,
                auditEmitter, queryAuditService, resilienceRegistry,
                throttlingService, stateJournal,
                clusterPressureMonitor, null, null, null);
    }
    // CHECKSTYLE.ON: ParameterNumber

    /** Backward-compatible 11-arg constructor. */
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
            QueryStateJournal stateJournal) {
        this(queryDispatcher, tracingProvider, metricsProvider,
                tenantResolver, quotaChecker, lineageEmitter,
                auditEmitter, queryAuditService, resilienceRegistry,
                throttlingService, stateJournal, null);
    }
    // CHECKSTYLE.ON: ParameterNumber

    /** Minimal dependencies -- only dispatcher and tracing, all others null/default. */
    public static QueryManagerContext minimal(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider) {
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null, null, null, null, null, null, null,
                new InMemoryQueryStateJournal(), null, null, null, null);
    }

    /**
     * Task 186: Minimal context with async submit configuration.
     * Used by tests to validate that query submission returns RUNNING
     * before full DAG execution completes.
     */
    public static QueryManagerContext minimalAsync(
            QueryDispatcher queryDispatcher, TracingProvider tracingProvider,
            AsyncSubmitConfig asyncSubmitConfig, ExecutorService asyncExecutor) {
        return new QueryManagerContext(queryDispatcher, tracingProvider,
                null, null, null, null, null, null, null, null,
                new InMemoryQueryStateJournal(), null, null,
                asyncSubmitConfig, asyncExecutor);
    }
}
