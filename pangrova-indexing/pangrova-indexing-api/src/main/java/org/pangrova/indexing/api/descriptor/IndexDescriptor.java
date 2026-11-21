package org.pangrova.indexing.api.descriptor;

import org.pangrova.indexing.api.enums.IndexType;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.indexing.api.enums.IndexState;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable descriptor for an index, carrying all metadata needed to
 * identify, build, evaluate, and manage the index lifecycle.
 *
 * <p>This value does not perform lifecycle transitions; domain orchestration owns them.</p>
 *
 * <p>Collections ({@code columnNames}, {@code properties}) are defensively
 * copied on construction to guarantee immutability.</p>
 *
 * @param indexId                unique index identifier
 * @param indexName              human-readable index name
 * @param catalogName            catalog containing the indexed table
 * @param schemaName             schema containing the indexed table
 * @param tableName              indexed table name
 * @param columnNames            columns covered by this index (non-empty)
 * @param indexType              the index strategy type
 * @param properties             index-specific configuration properties
 * @param state                  current lifecycle state
 * @param staleReason            reason for STALE state; {@code null} when not STALE
 * @param generation             rebuild generation counter (default 0)
 * @param buildSnapshotId        snapshot at which this generation was built; {@code null} if unknown
 * @param lastIndexedSnapshotId  last snapshot processed; {@code null} if unknown
 * @param createdAt              creation timestamp
 * @param updatedAt              last update timestamp
 * @param tenantId               owning tenant identifier; defaults to {@code "default"} if null
 */
@Experimental
public record IndexDescriptor(
        String indexId,
        String indexName,
        String catalogName,
        String schemaName,
        String tableName,
        List<String> columnNames,
        IndexType indexType,
        Map<String, String> properties,
        IndexState state,
        String staleReason,
        int generation,
        String buildSnapshotId,
        String lastIndexedSnapshotId,
        Instant createdAt,
        Instant updatedAt,
        String tenantId) {

    /**
     * Compact constructor — validates required fields and defensively copies collections.
     */
    public IndexDescriptor {
        requireText(indexId, "indexId");
        requireText(indexName, "indexName");
        requireText(catalogName, "catalogName");
        requireText(schemaName, "schemaName");
        requireText(tableName, "tableName");
        requireNonNull(indexType, "indexType");
        requireNonNull(state, "state");
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("columnNames must not be null or empty");
        }
        columnNames = List.copyOf(columnNames);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        tenantId = tenantId == null ? "default" : tenantId;
    }

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
    }

    /**
     * Returns a new descriptor with an updated generation counter.
     *
     * @param newGeneration the new generation value
     * @param timestamp     update timestamp
     * @return new descriptor with updated generation and timestamp
     */
    public IndexDescriptor withGeneration(int newGeneration, Instant timestamp) {
        return new IndexDescriptor(
                indexId, indexName, catalogName, schemaName, tableName,
                columnNames, indexType, properties,
                state, staleReason, newGeneration, buildSnapshotId, lastIndexedSnapshotId,
                createdAt, timestamp, tenantId);
    }

    /**
     * Returns a new descriptor with an updated build snapshot identifier.
     *
     * @param newBuildSnapshotId the snapshot at which this generation was built
     * @param timestamp          update timestamp
     * @return new descriptor with updated buildSnapshotId and timestamp
     */
    public IndexDescriptor withBuildSnapshotId(String newBuildSnapshotId, Instant timestamp) {
        return new IndexDescriptor(
                indexId, indexName, catalogName, schemaName, tableName,
                columnNames, indexType, properties,
                state, staleReason, generation, newBuildSnapshotId, lastIndexedSnapshotId,
                createdAt, timestamp, tenantId);
    }

}
