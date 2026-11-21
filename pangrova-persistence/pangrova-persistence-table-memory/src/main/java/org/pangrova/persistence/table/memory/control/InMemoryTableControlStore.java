package org.pangrova.persistence.table.memory.control;

import org.pangrova.table.api.exception.TableControlException;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.identity.TableRef;
import org.pangrova.table.api.rename.TableControlRenameIntent;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.api.observation.TableVersionObservation;
import org.pangrova.common.concurrent.ThreadSafe;

import java.util.ArrayList;
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
    private final Map<TableRef, UUID> liveByName = new ConcurrentHashMap<>();
    // All access is guarded by this store's monitor, including the record write.
    private final List<TableVersionObservation> observations = new ArrayList<>();

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
            throw TableControlException.staleWrite(
                    "Stale table control write for " + record.qualifiedName());
        }
        var names = new java.util.LinkedHashSet<TableRef>();
        if (record.state() != TableControlState.DROPPED) {
            names.add(key(record.catalog(), record.schema(), record.table()));
            TableControlRenameIntent.source(record).ifPresent(source ->
                    names.add(key(source.catalog(), source.schema(), source.table())));
        }
        for (TableRef name : names) {
            UUID owner = liveByName.get(name);
            if (owner != null && !owner.equals(record.tableId())) {
                throw new TableControlException(
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
    public synchronized void replaceRegistration(TableControlRecord expected, TableControlRecord replacement,
            java.time.Instant retiredAt) {
        expected.requireReplacement(replacement);
        var current = byId.get(expected.tableId());
        if (!expected.matchesRegistration(current)
                || !expected.tableId().equals(liveByName.get(expected.tableRef()))
                || byId.containsKey(replacement.tableId())) {
            throw TableControlException.staleWrite("Table registration changed for " + expected.qualifiedName());
        }
        var dropped = current.retiredAt(retiredAt);
        byId.put(expected.tableId(), dropped);
        byId.put(replacement.tableId(), replacement);
        liveByName.put(replacement.tableRef(), replacement.tableId());
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
        List<TableVersionObservation> result = new ArrayList<>();
        for (int index = observations.size() - 1; index >= 0 && result.size() < limit; index--) {
            TableVersionObservation observation = observations.get(index);
            if (observation.tableId().equals(tableId)) {
                result.add(observation);
            }
        }
        return List.copyOf(result);
    }

    private static TableRef key(String catalog, String schema, String table) {
        return new TableRef(catalog, schema, table);
    }
}
