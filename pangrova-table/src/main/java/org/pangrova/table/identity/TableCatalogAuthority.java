package org.pangrova.table.identity;

import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.util.Objects;
import java.util.function.Function;

/** Checks ownership against the current catalog incarnation and deployment revision. */
public final class TableCatalogAuthority {
    private final Function<String, CatalogDeploymentIdentity> catalogs;

    public TableCatalogAuthority(Function<String, CatalogDeploymentIdentity> catalogs) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    public CatalogDeploymentIdentity current(String catalog) {
        return catalogs.apply(catalog);
    }

    public void requireCurrent(TableControlRecord record, String operation) {
        if (!matches(record, current(record.catalog()))) {
            throw new AccessDeniedException(operation + " rejected: catalog deployment identity changed or "
                    + "was never registered for " + record.qualifiedName()
                    + "; explicitly reconcile the control record before using its authority");
        }
    }

    public static boolean matches(TableControlRecord record, CatalogDeploymentIdentity deployment) {
        return deployment.incarnationId().equals(record.catalogIncarnationId())
                && java.util.Objects.equals(deployment.revision(), record.catalogRevision());
    }
}
