package org.pangrova.security.api.authorization;

import org.pangrova.security.api.identity.UserIdentity;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.mask.TableColumnMasks;

import java.util.Optional;

/**
 * Access control for column-level policy retrieval, including row filters
 * and column masks applied during query execution.
 */
@Experimental
public interface ColumnAccessControl {

    Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table);

    /** Reads the table's current masks once for the supplied subject. */
    TableColumnMasks getColumnMasks(UserIdentity user, String catalog, String schema, String table);
}
