package org.pangrova.indexing.evolution;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.indexing.api.enums.SchemaChangeType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaTransition;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

/** Durable, fenced schema evolution for index descriptors. */
public final class IndexSchemaEvolutionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSchemaEvolutionHandler.class);
    private static final Set<IndexType> TEXT_SENSITIVE_TYPES = Set.of(
            IndexType.INVERTED, IndexType.GIN);

    private final IndexMetadataStore metadataStore;
    private final BiFunction<IndexDescriptor, SchemaChangeEvent, SchemaObservation> sourceObserver;

    public IndexSchemaEvolutionHandler(
            IndexMetadataStore metadataStore,
            BiFunction<IndexDescriptor, SchemaChangeEvent, SchemaObservation> sourceObserver) {
        this.metadataStore = java.util.Objects.requireNonNull(metadataStore, "metadataStore");
        this.sourceObserver = java.util.Objects.requireNonNull(sourceObserver, "sourceObserver");
    }

    /** Current connector metadata and format identity, observed together under exact authority. */
    public record SchemaObservation(Optional<ConnectorTableMetadata> metadata, Optional<TableIdentity> identity) {
        public SchemaObservation {
            java.util.Objects.requireNonNull(metadata, "metadata");
            java.util.Objects.requireNonNull(identity, "identity");
            if (metadata.isEmpty() && identity.isPresent()) {
                throw new IllegalArgumentException("Missing table cannot have current identity evidence");
            }
        }

        public static SchemaObservation absent() {
            return new SchemaObservation(Optional.empty(), Optional.empty());
        }
    }

    /** Persists the write-ahead fence before connector schema mutation starts. */
    public UUID prepare(SchemaChangeEvent event) {
        return prepare(event, Optional.empty());
    }

    public UUID prepare(SchemaChangeEvent event, Optional<TableIdentity> sourceIdentity) {
        return metadataStore.prepareSchemaEvolution(event, sourceIdentity, Instant.now());
    }

    public void committed(UUID operationId) {
        committed(operationId, Optional.empty());
    }

    public void committed(UUID operationId, Optional<TableIdentity> targetIdentity) {
        metadataStore.markSchemaEvolutionCommitted(operationId, targetIdentity, Instant.now());
    }

    /** No connector implementation may manufacture an abort from a generic exception. */
    public void unknown(UUID operationId) {
        metadataStore.markSchemaEvolutionUnknown(operationId, Instant.now());
    }

    /** Only callers proving mutation did not start may use this transition. */
    public void aborted(UUID operationId) {
        metadataStore.markSchemaEvolutionAborted(operationId, Instant.now());
    }

    /** Reconciles external schema drift that never passed through Pangrova's DDL journal. */
    public int reconcile() {
        return reconcile(() -> true);
    }

    /** Reconciles drift while the scheduler still owns its captured maintenance epoch. */
    public int reconcile(BooleanSupplier stillLeader) {
        int fenced = 0;
        if (!stillLeader.getAsBoolean()) {
            return fenced;
        }
        for (IndexDescriptor descriptor : metadataStore.findIndexesByState(IndexState.READY)) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                if (reconcileDescriptor(descriptor, stillLeader)) {
                    fenced++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=index_schema_reconcile_retry indexId={} table={} error={}",
                        descriptor.indexId(), descriptor.tableName(), failure.getMessage(), failure);
            }
        }
        return fenced;
    }

    private boolean reconcileDescriptor(IndexDescriptor descriptor, BooleanSupplier stillLeader) {
        SchemaChangeEvent event = new SchemaChangeEvent(
                SchemaChangeType.SCHEMA_MISMATCH, descriptor.tenantId(), descriptor.catalogName(),
                descriptor.schemaName(), descriptor.tableName(), null, null, null, null, null, null);
        Optional<ConnectorTableMetadata> source = sourceObserver.apply(descriptor, event).metadata();
        if (!stillLeader.getAsBoolean()) {
            return false;
        }
        if (source.isEmpty()) {
            return metadataStore.fenceIndexSchemaMismatch(descriptor, "source_table_missing", Instant.now())
                    .isPresent();
        }
        String fingerprint;
        try {
            fingerprint = IndexSchemaFingerprint.of(
                    descriptor.columnNames(), source.orElseThrow().typedColumns());
        } catch (IllegalArgumentException missing) {
            return stillLeader.getAsBoolean() && metadataStore.fenceIndexSchemaMismatch(
                    descriptor, "indexed_column_missing", Instant.now()).isPresent();
        }
        if (!fingerprint.equals(descriptor.sourceSchemaFingerprint())) {
            return stillLeader.getAsBoolean() && metadataStore.fenceIndexSchemaMismatch(
                    descriptor, "source_schema_mismatch", Instant.now()).isPresent();
        }
        return false;
    }

    /** Applies a bounded durable batch; failed operations remain pending for the next cycle. */
    public int recoverPending() {
        return recoverPending(() -> true);
    }

    /** Applies only complete operation batches while the captured maintenance epoch remains valid. */
    public int recoverPending(BooleanSupplier stillLeader) {
        int applied = 0;
        if (!stillLeader.getAsBoolean()) {
            return applied;
        }
        for (IndexSchemaEvolutionRecord record : metadataStore.listPendingSchemaEvolution(100)) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                // INTENDED/UNKNOWN are not connector success receipts. A connector
                // with no exact outcome authority cannot auto-promote either state.
                if (record.state() != IndexSchemaEvolutionState.COMMITTED) {
                    continue;
                }
                Optional<List<IndexSchemaTransition>> transitions = transitions(record, stillLeader);
                if (transitions.isEmpty() || !stillLeader.getAsBoolean()) {
                    break;
                }
                metadataStore.applySchemaEvolution(
                        record.operationId(), transitions.orElseThrow(), Instant.now());
                applied++;
            } catch (RuntimeException failure) {
                LOG.warn("event=index_schema_evolution_retry operationId={} indexId={} error={}",
                        record.operationId(), record.indexId(), failure.getMessage(), failure);
                try {
                    if (stillLeader.getAsBoolean()) {
                        metadataStore.deferSchemaEvolutionRetry(record.operationId(), Instant.now());
                    }
                } catch (RuntimeException retryFailure) {
                    LOG.warn("event=index_schema_evolution_retry_defer_failed operationId={} error={}",
                            record.operationId(), retryFailure.getMessage());
                }
            }
        }
        return applied;
    }

    private Optional<List<IndexSchemaTransition>> transitions(
            IndexSchemaEvolutionRecord record, BooleanSupplier stillLeader) {
        List<IndexSchemaTransition> transitions = new ArrayList<>();
        for (IndexDescriptor descriptor : metadataStore.listSchemaEvolutionTargets(record.operationId())) {
            if (!stillLeader.getAsBoolean()) {
                return Optional.empty();
            }
            if (descriptor.state() == IndexState.DROPPED) {
                continue;
            }
            transitions.add(transition(record, descriptor));
        }
        return Optional.of(List.copyOf(transitions));
    }

    private IndexSchemaTransition transition(IndexSchemaEvolutionRecord record, IndexDescriptor descriptor) {
        SchemaChangeEvent event = record.event();
        if (event.type() == SchemaChangeType.DROP_TABLE) {
            // The connector returned success for this exact write-ahead operation.
            return dropped(event, descriptor);
        }
        if (record.targetIdentity().isEmpty()) {
            return conservativeStale(descriptor);
        }
        SchemaObservation observed = sourceObserver.apply(descriptor, event);
        if (observed.metadata().isEmpty()
                || !record.targetIdentity().equals(observed.identity())
                || mutationNotObserved(event, observed.metadata().orElseThrow())) {
            return conservativeStale(descriptor);
        }
        Optional<ConnectorTableMetadata> source = observed.metadata();
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
        String schemaName = SchemaChangeType.RENAME_TABLE.equals(event.type())
                ? targetSchema(event) : descriptor.schemaName();
        // Existing active generation records do not prove physical table
        // incarnation. Even an unchanged schema cannot establish that proof.
        IndexState state = IndexState.STALE;
        if (TEXT_SENSITIVE_TYPES.contains(descriptor.indexType())
                && SchemaChangeType.MODIFY_COLUMN_TYPE.equals(event.type())
                && !fingerprint.equals(descriptor.sourceSchemaFingerprint())) {
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

    private static IndexSchemaTransition conservativeStale(IndexDescriptor descriptor) {
        return new IndexSchemaTransition(
                descriptor.indexId(), descriptor.catalogName(), descriptor.schemaName(), IndexState.STALE,
                descriptor.tableName(), descriptor.columnNames(), descriptor.sourceSchemaFingerprint(),
                "schema_identity_unverified", descriptor.updatedAt(), descriptor.state(), descriptor.generation(),
                descriptor.schemaName());
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
