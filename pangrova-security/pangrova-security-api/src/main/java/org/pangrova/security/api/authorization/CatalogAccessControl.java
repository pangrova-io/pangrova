package org.pangrova.security.api.authorization;

import org.pangrova.security.api.identity.UserIdentity;

import org.pangrova.common.api.annotation.Experimental;

/** Catalog inspection and mutation privileges are independent authorization decisions. */
@Experimental
public interface CatalogAccessControl {

    void checkCanAccessCatalog(UserIdentity user, String catalog);

    void checkCanCreateCatalog(UserIdentity user, String catalog);

    void checkCanDropCatalog(UserIdentity user, String catalog);

    void checkCanAlterCatalog(UserIdentity user, String catalog);
}
