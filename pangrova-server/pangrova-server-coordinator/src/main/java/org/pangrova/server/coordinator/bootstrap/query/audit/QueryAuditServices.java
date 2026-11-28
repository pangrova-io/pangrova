package org.pangrova.server.coordinator.bootstrap.query.audit;

import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.query.api.audit.QueryAuditStore;
import org.pangrova.query.audit.QueryAuditService;
import org.pangrova.query.profiling.QueryProfileService;

/** Composes audit admission and profile reads over the same Coordinator-owned store. */
public final class QueryAuditServices {
    private final QueryAuditService auditService;
    private final QueryProfileService profileService;
    private final org.pangrova.coordinator.query.task.QueryOperatorStatsRegistry operatorStats;

    /** Composes query audit, operator statistics and profile services over one audit store. */
    public QueryAuditServices(QueryAuditStore store) {
        this(store, null);
    }

    /** Registers query-scoped profile and process-wide dispatch metrics together. */
    public QueryAuditServices(QueryAuditStore store, MetricsProvider metricsProvider) {
        auditService = new QueryAuditService(
                java.util.Objects.requireNonNull(store, "store must not be null"));
        operatorStats = new org.pangrova.coordinator.query.task.QueryOperatorStatsRegistry(metricsProvider);
        profileService = new QueryProfileService(auditService, operatorStats);
    }

    public QueryAuditService auditService() { return auditService; }
    public QueryProfileService profileService() { return profileService; }

    /** Registry fed by accepted task reports; the profile service reads it. */
    public org.pangrova.coordinator.query.task.QueryOperatorStatsRegistry operatorStatsRegistry() {
        return operatorStats;
    }
}
