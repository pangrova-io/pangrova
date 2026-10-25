package org.pangrova.governance.classification;

import org.pangrova.spi.security.ColumnMaskRecord;
import org.pangrova.spi.security.ColumnMaskStore;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-memory column mask store for development and testing.
 * Stores masks in a concurrent list with auto-incrementing IDs.
 */
public final class InMemoryColumnMaskStore implements ColumnMaskStore {

    private final CopyOnWriteArrayList<ColumnMaskRecord> masks = new CopyOnWriteArrayList<>();
    private final AtomicLong nextId = new AtomicLong(1);

    @Override
    public List<ColumnMaskRecord> getMasks(String catalog, String schema, String table, String column) {
        return masks.stream()
                .filter(m -> m.catalogName().equals(catalog)
                        && m.schemaName().equals(schema)
                        && m.tableName().equals(table)
                        && m.columnName().equals(column))
                .toList();
    }

    @Override
    public void saveMask(ColumnMaskRecord mask) {
        masks.removeIf(m -> m.maskName().equals(mask.maskName())
                && m.tableName().equals(mask.tableName())
                && m.columnName().equals(mask.columnName())
                && java.util.Objects.equals(m.tableId(), mask.tableId()));
        masks.add(new ColumnMaskRecord(nextId.getAndIncrement(), mask.maskName(),
                mask.catalogName(), mask.schemaName(), mask.tableName(), mask.columnName(),
                mask.maskExpression(), mask.applyToRoles(), mask.exemptRoles(), mask.enabled(), mask.tableId()));
    }

    @Override
    public void deleteMask(long maskId) {
        masks.removeIf(m -> m.maskId() == maskId);
    }
}
