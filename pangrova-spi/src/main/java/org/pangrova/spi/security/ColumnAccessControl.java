package org.pangrova.spi.security;

import org.pangrova.spi.annotation.Experimental;

import java.util.Optional;

/**
 * Access control for column-level policy retrieval, including row filters
 * and column masks applied during query execution.
 */
@Experimental
public interface ColumnAccessControl {

    Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table);

    Optional<String> getColumnMask(UserIdentity user, String catalog, String schema,
                                   String table, String column);
}
