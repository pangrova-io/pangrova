package org.pangrova.metadata.control;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable store for LINKED and MANAGED table control records.
 *
 * <p>OBSERVED tables are never written here. {@link #find} returns only live
 * (non-{@link TableControlState#DROPPED}) records; dropped rows remain readable
 * by id for provenance.</p>
 */
public interface TableControlStore {

    Optional<TableControlRecord> find(String catalog, String schema, String table);

    Optional<TableControlRecord> findById(UUID tableId);

    List<TableControlRecord> listByCatalog(String catalog);

    List<TableControlRecord> listAll();

    void save(TableControlRecord record);

    /**
     * Appends a version transition for a controlled table. OBSERVED tables never
     * reach this method: they have no persisted identity.
     */
    default void recordVersionObservation(TableVersionObservation observation) {
    }

    /** Returns the most recent observations for a table, newest first. */
    default List<TableVersionObservation> listVersionObservations(UUID tableId, int limit) {
        return List.of();
    }
}
