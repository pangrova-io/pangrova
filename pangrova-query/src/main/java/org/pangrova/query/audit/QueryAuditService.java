package org.pangrova.query.audit;

import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.audit.QueryAuditSink;
import org.pangrova.query.api.audit.QueryAuditStore;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import java.util.Optional;

/**
 * Manages query audit record lifecycle: persist, retrieve, and cleanup.
 * Delegates storage to a pluggable {@link QueryAuditStore} port.
 */
public final class QueryAuditService implements QueryAuditSink {

    private final QueryAuditStore auditStore;

    public QueryAuditService(QueryAuditStore auditStore) {
        this.auditStore = auditStore;
    }

    /**
     * Persists a query audit record. Wraps storage failures with OBS error code.
     */
    @Override
    public void persist(QueryAuditRecord record) {
        try {
            auditStore.save(record);
        } catch (Exception e) {
            throw new PangrovaException(
                    PangrovaErrorCode.OBS_AUDIT_PERSISTENCE_FAILED,
                    "Failed to persist audit record for query: " + record.queryId(), e);
        }
    }

    /** Retrieves an exact query audit record for its submitting principal. */
    public Optional<QueryAuditRecord> findByQueryId(String queryId, String principal) {
        return auditStore.findById(queryId, principal);
    }

    /**
     * Removes audit records older than the specified retention period.
     */
    public long cleanupExpired(int retentionDays) {
        return auditStore.deleteOlderThan(retentionDays);
    }

}
