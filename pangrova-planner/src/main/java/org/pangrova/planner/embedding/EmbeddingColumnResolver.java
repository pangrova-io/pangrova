package org.pangrova.planner.embedding;

import java.util.List;
import java.util.Optional;

/**
 * Resolves auto-embedding column metadata for a given table.
 *
 * <p>Implementations query the catalog to determine which columns on a table
 * have {@code GENERATED ALWAYS AS (ai_embed(...))} definitions. Used by
 * {@link QueryTextEmbedRule} during retrieval planning. Mutation planning instead uses
 * the canonical generated-column declaration retained by its compiled table.</p>
 *
 * <p>Thread safety: implementations must be safe for concurrent use.</p>
 */
@FunctionalInterface
public interface EmbeddingColumnResolver {

    /**
     * Returns all auto-embedding column descriptors for the given table.
     *
     * @param tableName fully qualified table name (catalog.schema.table)
     * @return list of auto-embedding column descriptors; empty if none defined
     */
    List<AutoEmbeddingColumnDescriptor> resolveEmbeddingColumns(String tableName);

    /**
     * Finds the auto-embedding descriptor for a specific vector column.
     *
     * @param tableName    fully qualified table name
     * @param vectorColumn name of the vector column
     * @return the descriptor if the column is an auto-embedding column, empty otherwise
     */
    default Optional<AutoEmbeddingColumnDescriptor> resolveByVectorColumn(String tableName, String vectorColumn) {
        return resolveEmbeddingColumns(tableName).stream()
                .filter(d -> d.vectorColumn().equalsIgnoreCase(vectorColumn))
                .findFirst();
    }
}
