package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Durable write-ahead journal and fenced descriptor transitions for index schema changes. */
@Experimental
public interface IndexSchemaEvolutionStore {
    default UUID prepareSchemaEvolution(SchemaChangeEvent event, Instant now) {
        throw new UnsupportedOperationException("Durable index schema evolution is unavailable");
    }

    default void markSchemaEvolutionCommitted(UUID operationId, Instant now) {
        throw new UnsupportedOperationException("Durable index schema evolution is unavailable");
    }

    default void markSchemaEvolutionAborted(UUID operationId, Instant now) {
        throw new UnsupportedOperationException("Durable index schema evolution is unavailable");
    }

    default List<IndexSchemaEvolutionRecord> listPendingSchemaEvolution(int limit) {
        throw new UnsupportedOperationException("Durable index schema evolution is unavailable");
    }

    default void applySchemaEvolution(
            UUID operationId, List<IndexSchemaTransition> transitions, Instant now) {
        throw new UnsupportedOperationException("Durable index schema evolution is unavailable");
    }

    default Optional<UUID> fenceIndexSchemaMismatch(
            IndexDescriptor descriptor, String reason, Instant now) {
        throw new UnsupportedOperationException("Durable index schema evolution is unavailable");
    }

    record IndexSchemaEvolutionRecord(
            UUID operationId,
            String indexId,
            SchemaChangeEvent event,
            IndexSchemaEvolutionState state,
            Instant createdAt,
            Instant updatedAt) {}

    record IndexSchemaTransition(
            String indexId,
            String catalogName,
            String schemaName,
            IndexState newState,
            String newTableName,
            List<String> newColumnNames,
            String sourceSchemaFingerprint,
            String staleReason,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration,
            String newSchemaName) {
        @Override
        public List<String> newColumnNames() {
            return newColumnNames == null ? null : List.copyOf(newColumnNames);
        }

        public IndexSchemaTransition {
            if (newColumnNames != null) {
                newColumnNames = List.copyOf(newColumnNames);
            }
            if (newTableName == null || newTableName.isBlank()) {
                throw new IllegalArgumentException("newTableName is required");
            }
            if (newSchemaName == null || newSchemaName.isBlank()) {
                throw new IllegalArgumentException("newSchemaName is required");
            }
        }
    }
}
