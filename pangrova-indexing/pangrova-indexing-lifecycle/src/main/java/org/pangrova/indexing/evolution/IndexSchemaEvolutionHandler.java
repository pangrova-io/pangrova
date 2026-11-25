package org.pangrova.indexing.evolution;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.indexing.api.enums.SchemaChangeType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaTransition;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/** Durable, fenced schema evolution for index descriptors. */
public final class IndexSchemaEvolutionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSchemaEvolutionHandler.class);
    private static final Set<IndexType> TEXT_SENSITIVE_TYPES = Set.of(
            IndexType.INVERTED, IndexType.GIN);

    private final IndexMetadataStore metadataStore;
    private final BiFunction<IndexDescriptor, SchemaChangeEvent, Optional<ConnectorTableMetadata>> sourceObserver;

    public IndexSchemaEvolutionHandler(
            IndexMetadataStore metadataStore,
            BiFunction<IndexDescriptor, SchemaChangeEvent, Optional<ConnectorTableMetadata>> sourceObserver) {
        this.metadataStore = java.util.Objects.requireNonNull(metadataStore, "metadataStore");
        this.sourceObserver = java.util.Objects.requireNonNull(sourceObserver, "sourceObserver");
    }

    /** Persists the write-ahead fence before connector schema mutation starts. */
    public UUID prepare(SchemaChangeEvent event) {
        return metadataStore.prepareSchemaEvolution(event, Instant.now());
    }

    public void committed(UUID operationId) {
        metadataStore.markSchemaEvolutionCommitted(operationId, Instant.now());
    }

    public void aborted(UUID operationId) {
        metadataStore.markSchemaEvolutionAborted(operationId, Instant.now());
    }

    /** Reconciles external schema drift that never passed through Pangrova's DDL journal. */
    public int reconcile() {
        int fenced = 0;
        for (IndexDescriptor descriptor : metadataStore.findIndexesByState(IndexState.READY)) {
            try {
                if (reconcileDescriptor(descriptor)) {
                    fenced++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=index_schema_reconcile_retry indexId={} table={} error={}",
                        descriptor.indexId(), descriptor.tableName(), failure.getMessage(), failure);
            }
        }
        return fenced;
    }

    private boolean reconcileDescriptor(IndexDescriptor descriptor) {
        SchemaChangeEvent event = new SchemaChangeEvent(
                SchemaChangeType.SCHEMA_MISMATCH, descriptor.tenantId(), descriptor.catalogName(),
                descriptor.schemaName(), descriptor.tableName(), null, null, null, null, null, null);
        Optional<ConnectorTableMetadata> source = sourceObserver.apply(descriptor, event);
        if (source.isEmpty()) {
            metadataStore.fenceIndexSchemaMismatch(descriptor, "source_table_missing", Instant.now());
            return true;
        }
        String fingerprint;
        try {
            fingerprint = IndexSchemaFingerprint.of(
                    descriptor.columnNames(), source.orElseThrow().typedColumns());
        } catch (IllegalArgumentException missing) {
            metadataStore.fenceIndexSchemaMismatch(
                    descriptor, "indexed_column_missing", Instant.now());
            return true;
        }
        if (!fingerprint.equals(descriptor.sourceSchemaFingerprint())) {
            metadataStore.fenceIndexSchemaMismatch(
                    descriptor, "source_schema_mismatch", Instant.now());
            return true;
        }
        return false;
    }

    /** Applies a bounded durable batch; failed operations remain pending for the next cycle. */
    public int recoverPending() {
        int applied = 0;
        for (IndexSchemaEvolutionRecord record : metadataStore.listPendingSchemaEvolution(100)) {
            try {
                if (record.state() == IndexSchemaEvolutionState.INTENDED) {
                    metadataStore.markSchemaEvolutionCommitted(record.operationId(), Instant.now());
                }
                List<IndexSchemaTransition> transitions = transitions(record);
                metadataStore.applySchemaEvolution(
                        record.operationId(), transitions, Instant.now());
                applied++;
            } catch (RuntimeException failure) {
                LOG.warn("event=index_schema_evolution_retry operationId={} indexId={} error={}",
                        record.operationId(), record.indexId(), failure.getMessage(), failure);
            }
        }
        return applied;
    }

    private List<IndexSchemaTransition> transitions(IndexSchemaEvolutionRecord record) {
        List<IndexDescriptor> descriptors = descriptors(record);
        List<IndexSchemaTransition> transitions = new ArrayList<>();
        for (IndexDescriptor descriptor : descriptors) {
            if (descriptor.state() == IndexState.DROPPED) {
                continue;
            }
            transitions.add(transition(record.event(), descriptor));
        }
        return transitions;
    }

    private List<IndexDescriptor> descriptors(IndexSchemaEvolutionRecord record) {
        if (record.indexId() != null) {
            return metadataStore.findIndex(record.indexId()).map(List::of).orElseGet(List::of);
        }
        Set<String> indexIds = new HashSet<>();
        if (SchemaChangeType.RENAME_TABLE.equals(record.event().type())) {
            indexIds.addAll(indexIds(record.event().tenantId(), record.event().catalogName(),
                    record.event().schemaName(), record.event().oldTableName()));
        }
        indexIds.addAll(indexIds(record.event().tenantId(), record.event().catalogName(),
                targetSchema(record.event()), record.event().tableName()));
        return indexIds.stream()
                .flatMap(indexId -> metadataStore.findIndex(indexId).stream())
                .toList();
    }

    private List<String> indexIds(String tenantId, String catalog, String schema, String table) {
        return metadataStore.findIndexesByTable(tenantId, catalog, schema, table).stream()
                .map(IndexDescriptor::indexId)
                .toList();
    }

    private IndexSchemaTransition transition(SchemaChangeEvent event, IndexDescriptor descriptor) {
        Optional<ConnectorTableMetadata> source = sourceObserver.apply(descriptor, event);
        if (source.isEmpty()) {
            return dropped(event, descriptor);
        }
        if (mutationNotObserved(event, source.orElseThrow())) {
            return unchanged(descriptor);
        }
        List<String> columns = new ArrayList<>(descriptor.columnNames());
        if (SchemaChangeType.RENAME_COLUMN.equals(event.type())) {
            columns.replaceAll(name -> IndexSchemaFingerprint.normalizeColumnName(name).equals(
                    IndexSchemaFingerprint.normalizeColumnName(event.oldColumnName()))
                    ? event.newColumnName() : name);
        }
        if (SchemaChangeType.DROP_COLUMN.equals(event.type())
                && containsColumn(columns, event.oldColumnName())) {
            return dropped(event, descriptor);
        }
        String fingerprint;
        try {
            fingerprint = IndexSchemaFingerprint.of(columns, source.orElseThrow().typedColumns());
        } catch (IllegalArgumentException missing) {
            return dropped(event, descriptor);
        }
        boolean unchanged = fingerprint.equals(descriptor.sourceSchemaFingerprint())
                && columns.equals(descriptor.columnNames())
                && descriptor.tableName().equals(event.tableName())
                && descriptor.schemaName().equals(targetSchema(event));
        String schemaName = SchemaChangeType.RENAME_TABLE.equals(event.type())
                ? targetSchema(event) : descriptor.schemaName();
        IndexState state = unchanged && metadataStore.findActiveIndexGeneration(descriptor.indexId()).isPresent()
                ? IndexState.READY : IndexState.STALE;
        if (!unchanged && TEXT_SENSITIVE_TYPES.contains(descriptor.indexType())
                && SchemaChangeType.MODIFY_COLUMN_TYPE.equals(event.type())) {
            state = IndexState.DROPPED;
        }
        String tableName = SchemaChangeType.RENAME_TABLE.equals(event.type())
                ? event.tableName() : descriptor.tableName();
        return new IndexSchemaTransition(
                descriptor.indexId(), descriptor.catalogName(), descriptor.schemaName(),
                state, tableName, columns, fingerprint,
                state == IndexState.STALE ? "schema_evolution" : null,
                descriptor.updatedAt(), descriptor.state(), descriptor.generation(), schemaName);
    }

    private static boolean mutationNotObserved(
            SchemaChangeEvent event, ConnectorTableMetadata source) {
        if (SchemaChangeType.DROP_TABLE.equals(event.type())) {
            return true;
        }
        if (SchemaChangeType.RENAME_TABLE.equals(event.type())) {
            return !event.tableName().equals(source.table().table())
                    || !targetSchema(event).equals(source.table().schema());
        }
        if (SchemaChangeType.RENAME_COLUMN.equals(event.type())) {
            String renamed = IndexSchemaFingerprint.normalizeColumnName(event.newColumnName());
            boolean renamedPresent = source.typedColumns().stream().anyMatch(column ->
                    IndexSchemaFingerprint.normalizeColumnName(column.name()).equals(renamed));
            if (renamedPresent) {
                return false;
            }
            String original = IndexSchemaFingerprint.normalizeColumnName(event.oldColumnName());
            return source.typedColumns().stream().anyMatch(column ->
                    IndexSchemaFingerprint.normalizeColumnName(column.name()).equals(original));
        }
        return false;
    }

    private IndexSchemaTransition unchanged(IndexDescriptor descriptor) {
        IndexState state = metadataStore.findActiveIndexGeneration(descriptor.indexId()).isPresent()
                ? IndexState.READY : IndexState.STALE;
        return new IndexSchemaTransition(
                descriptor.indexId(), descriptor.catalogName(), descriptor.schemaName(), state,
                descriptor.tableName(), descriptor.columnNames(), descriptor.sourceSchemaFingerprint(),
                state == IndexState.STALE ? "schema_change_pending" : null,
                descriptor.updatedAt(), descriptor.state(), descriptor.generation(), descriptor.schemaName());
    }

    private static IndexSchemaTransition dropped(SchemaChangeEvent event, IndexDescriptor descriptor) {
        return new IndexSchemaTransition(
                descriptor.indexId(), descriptor.catalogName(), descriptor.schemaName(),
                IndexState.DROPPED,
                SchemaChangeType.RENAME_TABLE.equals(event.type())
                        ? event.tableName() : descriptor.tableName(),
                descriptor.columnNames(), descriptor.sourceSchemaFingerprint(), null,
                descriptor.updatedAt(), descriptor.state(), descriptor.generation(),
                SchemaChangeType.RENAME_TABLE.equals(event.type())
                        ? targetSchema(event) : descriptor.schemaName());
    }

    private static String targetSchema(SchemaChangeEvent event) {
        return event.newSchemaName() == null ? event.schemaName() : event.newSchemaName();
    }

    private static boolean containsColumn(List<String> columns, String name) {
        String normalized = IndexSchemaFingerprint.normalizeColumnName(name);
        return columns.stream().anyMatch(column ->
                IndexSchemaFingerprint.normalizeColumnName(column).equals(normalized));
    }
}
