package org.pangrova.query.assembly.audit;

import org.pangrova.persistence.memory.query.audit.InMemoryQueryAuditStore;
import org.pangrova.query.core.audit.QueryAuditService;
import org.pangrova.query.core.profiling.QueryProfileService;

/** Composes audit admission and profile reads over the same Coordinator-owned store. */
public final class QueryAuditServices {
    private final QueryAuditService auditService;
    private final QueryProfileService profileService;

    public QueryAuditServices() {
        auditService = new QueryAuditService(new InMemoryQueryAuditStore());
        profileService = new QueryProfileService(auditService);
    }

    public QueryAuditService auditService() { return auditService; }
    public QueryProfileService profileService() { return profileService; }
}
