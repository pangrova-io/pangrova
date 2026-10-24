package org.pangrova.metadata.control;

import java.util.Optional;
import java.util.UUID;

/** Durable storage for statistics collected by {@code ANALYZE TABLE}. */
public interface TableControlStatisticsStore {

    Optional<TableControlStatistics> find(UUID tableId);

    /** Inserts or replaces the table's current statistics row. */
    void save(TableControlStatistics statistics);

    void delete(UUID tableId);
}
