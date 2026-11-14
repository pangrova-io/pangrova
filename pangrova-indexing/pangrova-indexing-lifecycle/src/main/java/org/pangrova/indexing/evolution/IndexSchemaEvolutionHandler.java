package org.pangrova.indexing.evolution;

import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Locale;
import java.util.List;
import java.util.Set;

/**
 * Handles schema change events and updates affected indexes accordingly.
 *
 * <p>Dispatch rules by event type:</p>
 * <ul>
 *   <li>ADD_COLUMN — no index impact (new columns have no indexes)</li>
 *   <li>DROP_COLUMN — mark all indexes containing the column as DROPPED</li>
 *   <li>RENAME_COLUMN — update columnNames in affected indexes</li>
 *   <li>MODIFY_COLUMN_TYPE — STALE for widening-compatible types, DROPPED for incompatible</li>
 *   <li>DROP_TABLE — cascade DROPPED for all table indexes</li>
 *   <li>RENAME_TABLE — update tableName in all table indexes</li>
 * </ul>
 */
public final class IndexSchemaEvolutionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSchemaEvolutionHandler.class);

    /** Index types that require exact token/text semantics — always incompatible with type changes. */
    private static final Set<IndexType> TEXT_SENSITIVE_TYPES = Set.of(
            IndexType.INVERTED, IndexType.GIN);

    private final IndexMetadataStore metadataStore;

    public IndexSchemaEvolutionHandler(IndexMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    /**
     * Processes a schema change event and updates affected indexes.
     */
    public void onSchemaChange(SchemaChangeEvent event) {
        switch (event.type()) {
            case ADD_COLUMN -> handleAddColumn(event);
            case DROP_COLUMN -> handleDropColumn(event);
            case RENAME_COLUMN -> handleRenameColumn(event);
            case MODIFY_COLUMN_TYPE -> handleModifyColumnType(event);
            case DROP_TABLE -> handleDropTable(event);
            case RENAME_TABLE -> handleRenameTable(event);
            default -> throw new IllegalStateException("Unhandled schema change type: " + event.type());
        }
    }

    private void handleAddColumn(SchemaChangeEvent event) {
        LOG.debug("action=schema_evolution type=ADD_COLUMN table={}.{}.{} column={} impact=none",
                event.catalogName(), event.schemaName(), event.tableName(), event.newColumnName());
    }

    private void handleDropColumn(SchemaChangeEvent event) {
        List<IndexDescriptor> affected = metadataStore.findIndexesByColumn(
                event.tenantId(), event.catalogName(), event.schemaName(), event.tableName(),
                event.oldColumnName());

        Instant now = Instant.now();
        for (IndexDescriptor idx : affected) {
            if (idx.state() == IndexState.DROPPED) {
                continue;
            }
            metadataStore.updateIndexState(idx.indexId(), IndexState.DROPPED.name(), now, idx.updatedAt());
            emitCacheEvent(idx.indexId(), event, now);
            LOG.info("action=schema_evolution type=DROP_COLUMN index={} table={}.{}.{} "
                            + "column={} newState=DROPPED",
                    idx.indexName(), event.catalogName(), event.schemaName(),
                    event.tableName(), event.oldColumnName());
        }
    }

    private void emitCacheEvent(String indexId, SchemaChangeEvent event, Instant now) {
        try {
            metadataStore.insertCacheEvent("SCHEMA_CHANGE", indexId,
                    event.catalogName(), event.schemaName(), event.tableName(), now);
        } catch (Exception e) {
            LOG.warn("action=cache_event_insert_failed eventType=SCHEMA_CHANGE indexId={} error={}",
                    indexId, e.getMessage());
        }
    }

    private void handleRenameColumn(SchemaChangeEvent event) {
        metadataStore.renameColumnInIndexes(
                event.tenantId(), event.catalogName(), event.schemaName(), event.tableName(),
                event.oldColumnName(), event.newColumnName());
        LOG.info("action=schema_evolution type=RENAME_COLUMN table={}.{}.{} "
                        + "oldColumn={} newColumn={}",
                event.catalogName(), event.schemaName(), event.tableName(),
                event.oldColumnName(), event.newColumnName());
    }

    private void handleModifyColumnType(SchemaChangeEvent event) {
        List<IndexDescriptor> affected = metadataStore.findIndexesByColumn(
                event.tenantId(), event.catalogName(), event.schemaName(), event.tableName(),
                event.oldColumnName());

        Instant now = Instant.now();
        for (IndexDescriptor idx : affected) {
            if (idx.state() == IndexState.DROPPED) {
                continue;
            }
            applyTypeChange(idx, event, now);
            emitCacheEvent(idx.indexId(), event, now);
        }
    }

    private void applyTypeChange(IndexDescriptor idx, SchemaChangeEvent event, Instant now) {
        boolean compatible = isTypeCompatible(
                idx.indexType(), event.oldColumnType(), event.newColumnType());
        IndexState newState = compatible ? IndexState.STALE : IndexState.DROPPED;
        metadataStore.updateIndexState(idx.indexId(), newState.name(), now);
        LOG.info("action=schema_evolution type=MODIFY_COLUMN_TYPE index={} "
                        + "column={} oldType={} newType={} newState={}{}",
                idx.indexName(), event.oldColumnName(),
                event.oldColumnType(), event.newColumnType(), newState,
                compatible ? "" : " reason=incompatible_type_change");
    }

    private void handleDropTable(SchemaChangeEvent event) {
        Instant now = Instant.now();
        metadataStore.dropAllIndexesForTable(
                event.tenantId(), event.catalogName(), event.schemaName(), event.tableName(), now);
        try {
            metadataStore.insertCacheEvent("SCHEMA_CHANGE", "",
                    event.catalogName(), event.schemaName(), event.tableName(), now);
        } catch (Exception e) {
            LOG.warn("action=cache_event_insert_failed eventType=SCHEMA_CHANGE table={}.{}.{} error={}",
                    event.catalogName(), event.schemaName(), event.tableName(), e.getMessage());
        }
        LOG.info("action=schema_evolution type=DROP_TABLE table={}.{}.{} newState=DROPPED",
                event.catalogName(), event.schemaName(), event.tableName());
    }

    private void handleRenameTable(SchemaChangeEvent event) {
        metadataStore.renameTable(
                event.tenantId(), event.catalogName(), event.schemaName(),
                event.oldTableName(), event.tableName());
        LOG.info("action=schema_evolution type=RENAME_TABLE table={}.{}.{} oldTable={}",
                event.catalogName(), event.schemaName(), event.tableName(),
                event.oldTableName());
    }

    /**
     * Determines if a column type change is compatible with the given index type.
     *
     * <p>Inverted and GIN indexes depend on exact token/text semantics and are
     * always incompatible with type changes. Other index types tolerate widening
     * changes (e.g., INT → BIGINT, FLOAT → DOUBLE) but not narrowing.</p>
     */
    static boolean isTypeCompatible(IndexType indexType, String oldType, String newType) {
        if (TEXT_SENSITIVE_TYPES.contains(indexType)) {
            return false;
        }
        if (oldType == null || newType == null) {
            return false;
        }
        return isWideningChange(oldType.toUpperCase(Locale.ROOT), newType.toUpperCase(Locale.ROOT));
    }

    private static boolean isWideningChange(String oldType, String newType) {
        if (oldType.equals(newType)) {
            return true;
        }
        return numericRank(oldType) >= 0
                && numericRank(newType) >= 0
                && numericRank(newType) >= numericRank(oldType);
    }

    private static int numericRank(String type) {
        return switch (type) {
            case "TINYINT", "INT8" -> 0;
            case "SMALLINT", "INT16" -> 1;
            case "INT", "INTEGER", "INT32" -> 2;
            case "BIGINT", "INT64" -> 3;
            case "FLOAT", "REAL" -> 4;
            case "DOUBLE" -> 5;
            case "DECIMAL" -> 6;
            default -> -1;
        };
    }
}
