package org.pangrova.security.api;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Set;

/**
 * Access control for table-level operations including select, insert, delete,
 * create, drop, alter, rename, and column-level visibility.
 */
@Experimental
public interface TableAccessControl {

    void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table);

    /**
     * Requires full base-table SELECT and authority to read its unfiltered metadata.
     * Counts, paths, partition values and summaries cannot bypass applicable base row filters or masks.
     * The table argument is the base relation, not the metadata suffix name.
     */
    void checkCanReadTableMetadata(UserIdentity user, String catalog, String schema, String table);

    void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanUpdateTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanDeleteFromTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanCreateTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanDropTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanAlterTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanRenameTable(UserIdentity user, String catalog, String schema, String oldTable,
                             String newCatalog, String newSchema, String newTable);

    void checkCanShowColumns(UserIdentity user, String catalog, String schema, String table);

    void checkCanSelectFromColumns(UserIdentity user, String catalog, String schema,
                                   String table, Set<String> columns);
}
