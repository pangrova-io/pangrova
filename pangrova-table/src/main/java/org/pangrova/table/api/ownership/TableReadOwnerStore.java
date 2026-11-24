package org.pangrova.table.api.ownership;

import org.pangrova.common.api.annotation.Internal;

/** Checks a captured table owner and its co-located catalog deployment in one read. */
@Internal
@FunctionalInterface
public interface TableReadOwnerStore {
    /** Returns whether this active table still owns its exact name, format and catalog deployment. */
    boolean matchesCurrentReadOwner(TableControlRecord expected);
}
