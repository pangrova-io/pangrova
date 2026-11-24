package org.pangrova.table.api.ownership;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;

import java.util.Optional;

/** Observes table ownership and its co-located catalog deployment together. */
@Internal
public interface TableReadOwnerStore {
    /** Returns the registered table and its current catalog deployment from the same observation. */
    Optional<ReadOwnerObservation> observeReadOwner(String catalog, String schema, String table);

    /** Returns whether this active table still owns its exact name, format and catalog deployment. */
    boolean matchesCurrentReadOwner(TableControlRecord expected);

    record ReadOwnerObservation(TableControlRecord record, Optional<CatalogDeploymentIdentity> deployment) {}
}
