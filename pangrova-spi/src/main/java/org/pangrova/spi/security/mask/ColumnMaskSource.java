package org.pangrova.spi.security.mask;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;
import java.util.Set;

import org.pangrova.spi.security.UserIdentity;

/**
 * Read-only view of current column masks used by authorization checks.
 *
 * <p>Keeps the authorization package independent of the mask implementation;
 * masks must always be resolved from the caller's current authorization state.</p>
 */
@Experimental
public interface ColumnMaskSource {

    /** Returns the rendered mask for a single column, if a policy applies. */
    Optional<String> getMask(UserIdentity user, Set<String> effectiveRoles,
                             String catalog, String schema, String table, String column);

    /** Reports whether any column of the table would be masked for the caller. */
    boolean hasApplicableMask(UserIdentity user, Set<String> effectiveRoles,
                              String catalog, String schema, String table);
}
