package org.pangrova.coordinator.adapter.scheduler;

import java.util.Set;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.scheduler.core.speculation.SpeculativeQueryInventory;

/** Reads active query identities from the query lifecycle owner. */
public final class CoordinatorSpeculativeQueryInventory implements SpeculativeQueryInventory {

    private final QueryManager queryManager;

    public CoordinatorSpeculativeQueryInventory(QueryManager queryManager) {
        this.queryManager = queryManager;
    }

    @Override
    public Set<String> activeQueryIds() {
        return queryManager.getActiveQueryIds();
    }
}
