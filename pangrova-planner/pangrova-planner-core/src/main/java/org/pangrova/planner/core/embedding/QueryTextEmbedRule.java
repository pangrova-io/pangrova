package org.pangrova.planner.core.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Planner rule that detects text literals used as the second argument to vector
 * similarity predicates (cosine_distance, l2_distance, inner_product) against
 * auto-embedding columns, and rewrites them to embed the query text at plan time.
 *
 * <p>When a query contains {@code ORDER BY cosine_distance(embedding, 'some text')},
 * this rule resolves the embedding model for the {@code embedding} column and produces
 * a {@link QueryEmbedDirective} instructing the executor to embed the text literal
 * using the column's model (with query_prefix applied).</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class QueryTextEmbedRule {

    private static final Logger LOG = LoggerFactory.getLogger(QueryTextEmbedRule.class);

    private static final Set<String> DISTANCE_FUNCTIONS = Set.of(
            "COSINE_DISTANCE", "L2_DISTANCE", "INNER_PRODUCT");

    private final EmbeddingColumnResolver columnResolver;

    /**
     * Creates the rule with the given column resolver.
     *
     * @param columnResolver resolves auto-embedding column metadata from the catalog
     */
    public QueryTextEmbedRule(EmbeddingColumnResolver columnResolver) {
        if (columnResolver == null) {
            throw new IllegalArgumentException("columnResolver must not be null");
        }
        this.columnResolver = columnResolver;
    }

    /**
     * Attempts to rewrite a text literal in a distance function call to an embedding directive.
     *
     * <p>Matches the pattern: {@code distance_func(vector_column, 'text_literal')} where
     * {@code vector_column} is an auto-embedding column. The text literal will be embedded
     * using the column's registered model with query_prefix applied.</p>
     *
     * @param tableName    fully qualified table name
     * @param functionName the distance function name (e.g., "COSINE_DISTANCE")
     * @param columnName   the vector column name used in the distance function
     * @param textLiteral  the text literal to embed
     * @return embed directive if the column is an auto-embedding column, empty otherwise
     */
    public Optional<QueryEmbedDirective> tryRewrite(
            String tableName, String functionName, String columnName, String textLiteral) {

        if (!isDistanceFunction(functionName)) {
            return Optional.empty();
        }

        if (tableName == null || tableName.isBlank()
                || columnName == null || columnName.isBlank()
                || textLiteral == null || textLiteral.isBlank()) {
            return Optional.empty();
        }

        Optional<AutoEmbeddingColumnDescriptor> descriptor =
                columnResolver.resolveByVectorColumn(tableName, columnName);

        if (descriptor.isEmpty()) {
            LOG.debug("event=query_embed_skipped table={} column={} reason=not_auto_embedding_column",
                    tableName, columnName);
            return Optional.empty();
        }

        AutoEmbeddingColumnDescriptor col = descriptor.get();

        LOG.debug("event=query_text_embed_detected table={} column={} model={} textLength={}",
                tableName, columnName, col.modelName(), textLiteral.length());

        return Optional.of(new QueryEmbedDirective(
                col.modelName(),
                textLiteral,
                col.dimension(),
                columnName));
    }

    /**
     * Checks whether the given function name is a vector distance function.
     *
     * @param functionName function name to check (case-insensitive)
     * @return true if it is a recognized distance function
     */
    public static boolean isDistanceFunction(String functionName) {
        return functionName != null && DISTANCE_FUNCTIONS.contains(functionName.toUpperCase());
    }

    /**
     * Directive instructing the executor to embed a query text literal.
     *
     * <p>The executor applies the model's query_prefix before embedding.</p>
     *
     * @param modelName    embedding model to use for the query text
     * @param queryText    the text literal to embed
     * @param dimension    expected output vector dimension
     * @param vectorColumn the vector column this query targets (for context)
     */
    public record QueryEmbedDirective(
            String modelName,
            String queryText,
            int dimension,
            String vectorColumn) {

        public QueryEmbedDirective {
            if (modelName == null || modelName.isBlank()) {
                throw new IllegalArgumentException("modelName must not be blank");
            }
            if (queryText == null || queryText.isBlank()) {
                throw new IllegalArgumentException("queryText must not be blank");
            }
            if (dimension <= 0) {
                throw new IllegalArgumentException("dimension must be positive");
            }
            if (vectorColumn == null || vectorColumn.isBlank()) {
                throw new IllegalArgumentException("vectorColumn must not be blank");
            }
        }
    }
}
