package org.pangrova.query.core.audit;

import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.spi.audit.QueryAuditSink;
import org.pangrova.persistence.query.api.audit.QueryAuditStore;

import org.pangrova.common.exception.PangrovaErrorCode;
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

    /**
     * Retrieves an audit record by query ID.
     */
    public Optional<QueryAuditRecord> findByQueryId(String queryId) {
        return auditStore.findById(queryId);
    }

    /**
     * Removes audit records older than the specified retention period.
     */
    public long cleanupExpired(int retentionDays) {
        return auditStore.deleteOlderThan(retentionDays);
    }

}
