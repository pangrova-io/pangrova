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
     * Atomically saves a control revision and its version transition. A failed
     * observation must roll back the record and cache invalidation as well.
     */
    void saveWithObservation(TableControlRecord record, TableVersionObservation observation);

    /** Returns the most recent observations for a table, newest first. */
    default List<TableVersionObservation> listVersionObservations(UUID tableId, int limit) {
        return List.of();
    }
}
