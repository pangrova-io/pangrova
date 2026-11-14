package org.pangrova.query.api.audit;

import org.pangrova.query.api.audit.QueryAuditRecord;

import org.pangrova.common.api.annotation.Internal;

/**
 * Admits terminal query audit records without exposing storage lookup, retention or implementation ownership.
 *
 * <p>Successful admission follows the selected runtime's persistence contract; an
 * asynchronous store does not thereby promise a durable database commit.</p>
 */
@Internal
@FunctionalInterface
public interface QueryAuditSink {
    void persist(QueryAuditRecord record);
}
