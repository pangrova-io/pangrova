package org.pangrova.spi.security;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Access control for catalog-level operations.
 * Provides default implementations for create/drop/alter that delegate
 * to the base catalog access check.
 */
@Experimental
public interface CatalogAccessControl {

    void checkCanAccessCatalog(UserIdentity user, String catalog);

    default void checkCanCreateCatalog(UserIdentity user, String catalog) {
        checkCanAccessCatalog(user, catalog);
    }

    default void checkCanDropCatalog(UserIdentity user, String catalog) {
        checkCanAccessCatalog(user, catalog);
    }

    default void checkCanAlterCatalog(UserIdentity user, String catalog) {
        checkCanAccessCatalog(user, catalog);
    }
}
