package org.pangrova.query.api.audit;

import org.pangrova.common.api.annotation.Internal;

import java.util.Optional;

/** Persistence port for query audit records; implementations own their storage and lifecycle. */
@Internal
public interface QueryAuditStore {
    void save(QueryAuditRecord record);
    Optional<QueryAuditRecord> findById(String queryId);
    long deleteOlderThan(int retentionDays);
}
