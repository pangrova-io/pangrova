package org.pangrova.security.api.mask;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Set;

import org.pangrova.security.api.identity.UserIdentity;

/**
 * Read-only view of current column masks used by authorization checks.
 *
 * <p>Keeps the authorization package independent of the mask implementation;
 * masks must always be resolved from the caller's current authorization state.</p>
 */
@Experimental
public interface ColumnMaskSource {

    /** Captures policies for one table using the supplied current subject roles. */
    TableColumnMasks getMasks(UserIdentity user, Set<String> effectiveRoles,
            String catalog, String schema, String table);

    /** Reports whether any column of the table would be masked for the caller. */
    boolean hasApplicableMask(UserIdentity user, Set<String> effectiveRoles,
                              String catalog, String schema, String table);
}
