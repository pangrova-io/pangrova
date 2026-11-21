package org.pangrova.table.api.ownership;

import org.pangrova.common.api.annotation.Internal;

import java.time.Instant;
import org.pangrova.table.api.observation.TableVersionObservation;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable store for LINKED and MANAGED table control records.
 *
 * <p>OBSERVED tables are never written here. {@link #find} returns only live
 * (non-{@code DROPPED}) records; dropped rows remain readable
 * by id for provenance.</p>
 */
@Internal
public interface TableControlStore {

    Optional<TableControlRecord> find(String catalog, String schema, String table);

    Optional<TableControlRecord> findById(UUID tableId);

    List<TableControlRecord> listByCatalog(String catalog);

    List<TableControlRecord> listAll();

    void save(TableControlRecord record);

    /**
     * Atomically retires the exact live registration and reserves its name for a fresh identity.
     * A stale revision, pending rename, conflicting identity or failed outbox write leaves both unchanged.
     */
    void replaceRegistration(TableControlRecord expected, TableControlRecord replacement, Instant retiredAt);

    /**
     * Atomically saves a control revision and its version transition. A failed
     * observation must roll back the record and cache invalidation as well.
     */
    void saveWithObservation(TableControlRecord record, TableVersionObservation observation);

    /**
     * Returns this table's successfully persisted observations in reverse accepted
     * revision order, bounded by {@code limit}. The caller-supplied observedAt is
     * evidence, not an ordering key: clocks may move backwards or produce ties.
     * Failed or stale saves never appear. A non-positive limit returns an empty list.
     * The returned snapshot is detached from subsequent writes; no ordering across
     * different tables is implied.
     */
    List<TableVersionObservation> listVersionObservations(UUID tableId, int limit);
}
