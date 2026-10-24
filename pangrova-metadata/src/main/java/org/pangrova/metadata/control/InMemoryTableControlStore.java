package org.pangrova.metadata.control;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local control store used when no metadata datasource is configured.
 *
 * <p>Records do not survive restart and are not shared across coordinators; a
 * deployment that enables enforcement must use the JDBC store so ownership
 * decisions agree across nodes.</p>
 */
@ThreadSafe
public final class InMemoryTableControlStore implements TableControlStore {

    private static final Comparator<TableControlRecord> NAME_ORDER =
            Comparator.comparing(TableControlRecord::catalog)
                    .thenComparing(TableControlRecord::schema)
                    .thenComparing(TableControlRecord::table);

    private final Map<UUID, TableControlRecord> byId = new ConcurrentHashMap<>();
    private final Map<String, UUID> liveByName = new ConcurrentHashMap<>();
    private final java.util.List<TableVersionObservation> observations =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override
    public Optional<TableControlRecord> find(String catalog, String schema, String table) {
        UUID id = liveByName.get(key(catalog, schema, table));
        return id == null ? Optional.empty() : Optional.ofNullable(byId.get(id));
    }

    @Override
    public Optional<TableControlRecord> findById(UUID tableId) {
        return Optional.ofNullable(byId.get(tableId));
    }

    @Override
    public List<TableControlRecord> listByCatalog(String catalog) {
        return byId.values().stream()
                .filter(record -> record.catalog().equals(catalog))
                .sorted(NAME_ORDER)
                .toList();
    }

    @Override
    public List<TableControlRecord> listAll() {
        return byId.values().stream().sorted(NAME_ORDER).toList();
    }

    @Override
    public void save(TableControlRecord record) {
        String nameKey = key(record.catalog(), record.schema(), record.table());
        TableControlRecord previous = byId.put(record.tableId(), record);

        if (previous != null) {
            String previousKey = key(previous.catalog(), previous.schema(), previous.table());
            if (!previousKey.equals(nameKey)) {
                liveByName.remove(previousKey, previous.tableId());
            }
        }

        if (record.state() == TableControlState.DROPPED) {
            liveByName.remove(nameKey, record.tableId());
            return;
        }

        UUID replaced = liveByName.put(nameKey, record.tableId());
        if (replaced != null && !replaced.equals(record.tableId())) {
            retire(replaced, record.updatedAt());
        }
    }

    @Override
    public void recordVersionObservation(TableVersionObservation observation) {
        observations.add(observation);
    }

    @Override
    public List<TableVersionObservation> listVersionObservations(UUID tableId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return observations.stream()
                .filter(observation -> observation.tableId().equals(tableId))
                .sorted(java.util.Comparator.comparing(
                        TableVersionObservation::observedAt).reversed())
                .limit(limit)
                .toList();
    }

    private void retire(UUID tableId, java.time.Instant updatedAt) {
        byId.computeIfPresent(tableId, (id, existing) -> existing.state() == TableControlState.DROPPED
                ? existing
                : existing.toBuilder()
                        .state(TableControlState.DROPPED)
                        .metadataRevision(existing.metadataRevision() + 1)
                        .updatedAt(updatedAt)
                        .build());
    }

    private static String key(String catalog, String schema, String table) {
        return catalog + '\u001F' + schema + '\u001F' + table;
    }
}
