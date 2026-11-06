package org.pangrova.persistence.table.api.statistics;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.table.api.statistics.TableControlStatistics;

import java.util.Optional;
import java.util.UUID;

/** Durable storage for statistics collected by {@code ANALYZE TABLE}. */
@Internal
public interface TableControlStatisticsStore {

    Optional<TableControlStatistics> find(UUID tableId);

    /** Inserts or replaces the table's current statistics row. */
    void save(TableControlStatistics statistics);

    void delete(UUID tableId);
}
