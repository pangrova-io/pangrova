package org.pangrova.scheduler.speculation;

import java.util.Set;

/** Read-only view of queries that may still have running tasks. */
@FunctionalInterface
public interface SpeculativeQueryInventory {

    Set<String> activeQueryIds();
}
