package org.pangrova.persistence.table.memory.statistics;

import org.pangrova.table.api.statistics.TableControlStatistics;
import org.pangrova.table.api.statistics.TableControlStatisticsStore;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Process-local statistics store for tests and metadata-less deployments. */
public final class InMemoryTableControlStatisticsStore implements TableControlStatisticsStore {

    private final Map<UUID, TableControlStatistics> entries = new ConcurrentHashMap<>();

    @Override
    public Optional<TableControlStatistics> find(UUID tableId) {
        return Optional.ofNullable(entries.get(tableId));
    }

    @Override
    public void save(TableControlStatistics statistics) {
        entries.put(statistics.tableId(), statistics);
    }

    @Override
    public void delete(UUID tableId) {
        entries.remove(tableId);
    }
}
