package org.pangrova.materialization.port;

import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.PhysicalTableRef;

/**
 * Connector-side lifecycle of physical generation tables.
 *
 * <p>Both operations must be idempotent. The kernel marks a generation
 * collected only after the store proves the table is gone; a store failure
 * leaves the durable intent in the GC ledger for a later retry. The owning
 * object carries the tenant whose catalog session performs the drop.
 */
public interface PhysicalGenerationStore {

    /**
     * Returns whether the reserved physical table exists.
     *
     * @param object owning materialization
     * @param table physical table identity
     * @return true when the table currently exists
     */
    boolean exists(MaterializationId object, PhysicalTableRef table);

    /**
     * Drops the physical table if it exists.
     *
     * @param object owning materialization
     * @param table physical table identity
     */
    void drop(MaterializationId object, PhysicalTableRef table);
}
