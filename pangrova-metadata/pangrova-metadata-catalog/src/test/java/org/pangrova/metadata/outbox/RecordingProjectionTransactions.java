package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;
import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import java.util.*;

/** Single-thread component recorder. SQL atomicity and outbox guarantees are tested against PostgreSQL. */
final class RecordingProjectionTransactions implements TableProjectionRepository, MetadataProjectionTransactions {
    private final CatalogDescriptor deployment;
    private final Map<QualifiedTableRef, TableMetadata> tables = new HashMap<>();
    private final Map<UUID, MetadataProjectionReceipt> receipts = new HashMap<>();
    final Map<String, List<String>> savedSchemas = new HashMap<>();

    RecordingProjectionTransactions(CatalogDescriptor deployment) { this.deployment = deployment; }

    /** Test data seeding only. */
    void saveTable(TableMetadata table) { tables.put(ref(table), table); }

    @Override public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
        return Optional.ofNullable(tables.get(new QualifiedTableRef(catalog, schema, table)));
    }
    @Override public List<QualifiedTableRef> searchTables(String pattern) {
        throw new UnsupportedOperationException("This component fixture does not implement search");
    }
    @Override public Optional<MetadataProjectionReceipt> findReceipt(UUID id) {
        return Optional.ofNullable(receipts.get(id));
    }
    @Override public void recoverPending(org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity scope) {
        // This synchronous component recorder has no crash boundary or pending operations.
        if (!deployment.identity().equals(scope)) throw new MetadataProjectionConflictException("Projection deployment differs");
    }
    @Override public MetadataProjectionReceipt apply(MetadataProjectionMutation mutation) {
        var existing = receipts.get(mutation.operationId());
        if (existing != null) {
            if (!existing.mutation().equals(mutation)) throw new MetadataProjectionConflictException("Operation content differs");
            return existing;
        }
        if (!deployment.identity().equals(mutation.deployment())) {
            throw new MetadataProjectionConflictException("Projection deployment differs");
        }
        TableMetadata previous = null;
        boolean changed;
        if (mutation instanceof MetadataProjectionMutation.UpsertTable upsert) {
            previous = tables.put(ref(upsert.table()), upsert.table());
            changed = true;
        } else if (mutation instanceof MetadataProjectionMutation.DeleteTable deletion) {
            previous = tables.remove(deletion.table());
            changed = previous != null;
        } else if (mutation instanceof MetadataProjectionMutation.ObserveSchemas observation) {
            var names = new TreeSet<>(savedSchemas.getOrDefault(deployment.name(), List.of()));
            names.addAll(observation.schemas());
            savedSchemas.put(deployment.name(), List.copyOf(names));
            changed = !observation.schemas().isEmpty();
        } else throw new IllegalArgumentException("Unknown mutation");
        var receipt = new MetadataProjectionReceipt(mutation, changed, Optional.ofNullable(previous), deployment.allowedTenants());
        receipts.put(mutation.operationId(), receipt);
        return receipt;
    }
    private static QualifiedTableRef ref(TableMetadata table) {
        return new QualifiedTableRef(table.catalog(), table.schema(), table.table());
    }
}
