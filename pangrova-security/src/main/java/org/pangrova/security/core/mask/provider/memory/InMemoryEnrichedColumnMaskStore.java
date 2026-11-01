package org.pangrova.security.core.mask.provider.memory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.persistence.api.security.mask.EnrichedColumnMaskStore;

/**
 * Thread-safe in-memory column mask store implementing the enriched interface.
 * Used as the shared store when no JDBC datasource is configured, enabling both
 * enforcement (ColumnMaskingProcessor) and policy generation (TagBasedSecurityPolicy)
 * to operate on the same masks in development/test environments.
 *
 * <p>Masks stored here are transient — they do not survive coordinator restart.
 * For persistent masking, configure a JDBC datasource so that SecurityBootstrap
 * uses {@link org.pangrova.persistence.jdbc.security.mask.JdbcColumnMaskStore} instead.</p>
 */
public final class InMemoryEnrichedColumnMaskStore implements EnrichedColumnMaskStore {

    private final CopyOnWriteArrayList<ColumnMask> masks = new CopyOnWriteArrayList<>();
    private final AtomicLong nextId = new AtomicLong(1);

    @Override
    public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
        return masks.stream().filter(mask -> mask.catalogName().equals(catalog)
                && mask.schemaName().equals(schema) && mask.tableName().equals(table)).toList();
    }

    @Override
    public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
        return masks.stream()
                .filter(m -> m.catalogName().equals(catalog)
                        && m.schemaName().equals(schema)
                        && m.tableName().equals(table)
                        && m.columnName().equals(column))
                .toList();
    }

    @Override
    public void saveColumnMask(ColumnMask mask) {
        masks.removeIf(m -> m.maskName().equals(mask.maskName())
                && m.catalogName().equals(mask.catalogName())
                && m.schemaName().equals(mask.schemaName())
                && m.tableName().equals(mask.tableName())
                && m.columnName().equals(mask.columnName())
                && java.util.Objects.equals(m.tableId(), mask.tableId()));
        masks.add(new ColumnMask(nextId.getAndIncrement(), mask.maskName(),
                mask.catalogName(), mask.schemaName(), mask.tableName(), mask.columnName(),
                mask.maskExpression(), mask.applyToRoles(), mask.exemptRoles(),
                mask.enabled(), mask.tableId()));
    }

    @Override
    public void deleteMask(long maskId) {
        masks.removeIf(m -> m.maskId() == maskId);
    }
}
