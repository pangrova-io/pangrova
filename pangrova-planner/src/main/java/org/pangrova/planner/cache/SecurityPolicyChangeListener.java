package org.pangrova.planner.cache;

import org.pangrova.security.event.SecurityPolicyChangeEvent;
import org.pangrova.security.event.SecurityPolicyChangeEvent.ColumnMaskChanged;
import org.pangrova.security.event.SecurityPolicyChangeEvent.RlsPolicyChanged;
import org.pangrova.security.event.SecurityPolicyChangeEvent.RoleAssignmentChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for security policy changes and invalidates affected cache entries.
 * RLS/column mask changes invalidate by table; role assignment changes invalidate by user.
 * Unknown event types trigger full cache invalidation as a conservative fallback.
 */
public class SecurityPolicyChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityPolicyChangeListener.class);

    private final QueryResultCache cache;

    public SecurityPolicyChangeListener(QueryResultCache cache) {
        this.cache = cache;
    }

    public void onSecurityPolicyChange(SecurityPolicyChangeEvent event) {
        if (event instanceof RlsPolicyChanged e) {
            cache.invalidateByTable(e.table());
        } else if (event instanceof ColumnMaskChanged e) {
            cache.invalidateByTable(e.table());
        } else if (event instanceof RoleAssignmentChanged e) {
            cache.invalidateByUser(e.principal());
        } else {
            LOG.warn("event=unknown_security_policy_change type={} action=invalidate_all",
                    event.getClass().getSimpleName());
            cache.invalidateAll();
        }
    }
}
