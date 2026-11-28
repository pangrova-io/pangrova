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
        return audit(emitter, service, lineage, dispatcher, events, false);
    }

    /** Uses the journal transaction as the publisher for lineage attached to an owned query event. */
    public static QueryAuditPort audit(
            AuditEventEmitter emitter, QueryAuditSink service, LineageEventEmitter lineage,
            QueryDispatcher dispatcher, QueryEventDispatcher events, boolean journalCommitsLineage) {
        return new QueryAuditDispatcher(QueryAuditDispatcher.createAuditEmitter(emitter, service),
                lineage, dispatcher, events, journalCommitsLineage);
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
        return (user, usageByTenant) -> new org.pangrova.security.tenant.TenantQueryGuard(
                resolver, checker, tenantId -> {
                    var usage = usageByTenant.apply(tenantId);
                    return new org.pangrova.security.tenant.TenantUsage(
                            usage.concurrentQueries(), usage.queuedQueries(), 0L, 0L);
                }).checkAndResolve(user);
    }
}
