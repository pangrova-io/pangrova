package org.pangrova.scheduler.api.speculation;

import java.util.Set;

import org.pangrova.common.api.annotation.Experimental;

/** Read-only view of queries that may still have running tasks. */
@Experimental
@FunctionalInterface
public interface SpeculativeQueryInventory {

    Set<String> activeQueryIds();
}
