package org.pangrova.security.authz;

import java.util.Optional;
import java.util.Set;

import org.pangrova.spi.security.UserIdentity;

/**
 * Read-only view of current row-level filters used by authorization checks.
 *
 * <p>Keeps the authorization package independent of the filter implementation;
 * filters must always be resolved from the caller's current authorization state.</p>
 */
public interface RowFilterSource {

    /** Returns the safe SQL filter for the table, if a policy applies. */
    Optional<String> getFilter(UserIdentity user, Set<String> effectiveRoles,
                               String catalog, String schema, String table);
}
