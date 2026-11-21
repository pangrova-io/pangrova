package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.query.api.audit.QueryAuditSink;
import org.pangrova.query.event.QueryEventDispatcher;
import org.pangrova.query.lifecycle.QueryAdmissionPort;
import org.pangrova.query.lifecycle.QueryAuditPort;
import org.pangrova.security.audit.AuditEventEmitter;

/** Process-side adapters for the query lifecycle ports. */
public final class QueryManagerCollaborators {

    private QueryManagerCollaborators() {}

    /** Binds admission and throttling to the coordinator's policy implementations. */
    public static QueryAdmissionPort admission(
            ResilienceRegistry resilience, QueryThrottlingService throttle) {
        return new QueryAdmission(resilience, throttle);
    }

    /** Binds audit, event and lineage effects to their coordinator implementations. */
    public static QueryAuditPort audit(
            AuditEventEmitter emitter, QueryAuditSink service, LineageEventEmitter lineage,
            QueryDispatcher dispatcher, QueryEventDispatcher events) {
        return new QueryAuditDispatcher(
                QueryAuditDispatcher.createAuditEmitter(emitter, service),
                lineage, dispatcher, events);
    }

    /** Wraps the process-selected durable journal for the query lifecycle. */
    public static org.pangrova.query.lifecycle.QueryJournalLifecyclePort journal(
            org.pangrova.persistence.query.api.state.QueryStateJournal journal,
            QueryAuditPort audit) {
        return new QueryJournalLifecycle(journal, audit);
    }

    /** Binds tenant resolution and its live concurrent-query quota. */
    public static org.pangrova.query.lifecycle.TenantAdmissionPort tenant(
            org.pangrova.security.tenant.TenantResolver resolver,
            org.pangrova.security.tenant.QuotaChecker checker) {
        if (resolver == null || checker == null) {
            return null;
        }
        return (user, activeQueries) -> new org.pangrova.security.tenant.TenantQueryGuard(
                resolver, checker, tenantId -> {
                    int concurrent = 0;
                    int queued = 0;
                    for (var state : activeQueries) {
                        if (state.physicalCleanupApplied() || !tenantId.equals(state.getTenantId())) {
                            continue;
                        }
                        concurrent++;
                        if (state.getState() == org.pangrova.query.api.enums.QueryState.QUEUED) {
                            queued++;
                        }
                    }
                    return new org.pangrova.security.tenant.TenantUsage(concurrent, queued, 0L, 0L);
                }).checkAndResolve(user);
    }
}
