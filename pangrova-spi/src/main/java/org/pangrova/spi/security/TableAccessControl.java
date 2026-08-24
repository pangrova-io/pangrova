package org.pangrova.spi.security;

import org.pangrova.spi.annotation.Experimental;

import java.util.Set;

/**
 * Access control for table-level operations including select, insert, delete,
 * create, drop, alter, rename, and column-level visibility.
 */
@Experimental
public interface TableAccessControl {

    void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table);

    void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table);

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
