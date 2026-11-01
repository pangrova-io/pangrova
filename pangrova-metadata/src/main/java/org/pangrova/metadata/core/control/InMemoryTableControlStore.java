package org.pangrova.metadata.core.control;

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
    public synchronized Optional<TableControlRecord> find(String catalog, String schema, String table) {
        UUID id = liveByName.get(key(catalog, schema, table));
        return id == null ? Optional.empty() : Optional.ofNullable(byId.get(id));
    }

    @Override
    public synchronized Optional<TableControlRecord> findById(UUID tableId) {
        return Optional.ofNullable(byId.get(tableId));
    }

    @Override
    public synchronized List<TableControlRecord> listByCatalog(String catalog) {
        return byId.values().stream()
                .filter(record -> record.catalog().equals(catalog))
                .sorted(NAME_ORDER)
                .toList();
    }

    @Override
    public synchronized List<TableControlRecord> listAll() {
        return byId.values().stream().sorted(NAME_ORDER).toList();
    }

    @Override
    public synchronized void save(TableControlRecord record) {
        TableControlRecord previous = byId.get(record.tableId());
        if (previous != null && previous.metadataRevision() >= record.metadataRevision()) {
            throw new org.pangrova.metadata.api.error.MetastoreException(
                    "Stale table control write for " + record.qualifiedName());
        }
        var names = new java.util.LinkedHashSet<String>();
        if (record.state() != TableControlState.DROPPED) {
            names.add(key(record.catalog(), record.schema(), record.table()));
            TableControlRenameIntent.source(record).ifPresent(source ->
                    names.add(key(source.catalog(), source.schema(), source.table())));
        }
        for (String name : names) {
            UUID owner = liveByName.get(name);
            if (owner != null && !owner.equals(record.tableId())) {
                throw new org.pangrova.metadata.api.error.MetastoreException(
                        "A live table control record already reserves " + name);
            }
        }
        byId.put(record.tableId(), record);
        if (previous != null) {
            liveByName.remove(key(previous.catalog(), previous.schema(), previous.table()), record.tableId());
            TableControlRenameIntent.source(previous).ifPresent(source ->
                    liveByName.remove(key(source.catalog(), source.schema(), source.table()), record.tableId()));
        }
        names.forEach(name -> liveByName.put(name, record.tableId()));
    }

    @Override
    public synchronized void saveWithObservation(TableControlRecord record, TableVersionObservation observation) {
        if (!record.tableId().equals(observation.tableId())
                || !java.util.Objects.equals(record.versionToken(), observation.versionToken())) {
            throw new IllegalArgumentException("Observation must describe the saved table and version");
        }
        save(record);
        observations.add(observation);
    }

    @Override
    public synchronized List<TableVersionObservation> listVersionObservations(UUID tableId, int limit) {
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

    private static String key(String catalog, String schema, String table) {
        return catalog + '\u001F' + schema + '\u001F' + table;
    }
}
