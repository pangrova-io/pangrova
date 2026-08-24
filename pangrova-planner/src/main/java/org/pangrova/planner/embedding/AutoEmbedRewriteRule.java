package org.pangrova.planner.embedding;

import org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor.EmbeddingMode;
import org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Planner rule that detects INSERT/UPDATE on tables with auto-embedding columns
 * and produces {@link EmbedInjectionPlan} directives for the executor.
 *
 * <p>For SYNC mode columns, the rule signals that embedding computation must occur
 * before the DML commits. For ASYNC mode columns, the rule signals that rows should
 * be committed with NULL vectors and enqueued for background processing.</p>
 *
 * <p>On UPDATE, the rule compares the SET columns with each embedding's source columns.
 * Recomputation is triggered only when the intersection is non-empty (unchanged sources
 * retain existing vectors).</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class AutoEmbedRewriteRule {

    private static final Logger LOG = LoggerFactory.getLogger(AutoEmbedRewriteRule.class);

    private final EmbeddingColumnResolver columnResolver;

    /**
     * Creates the rule with the given column resolver.
     *
     * @param columnResolver resolves auto-embedding column metadata from the catalog
     */
    public AutoEmbedRewriteRule(EmbeddingColumnResolver columnResolver) {
        if (columnResolver == null) {
            throw new IllegalArgumentException("columnResolver must not be null");
        }
        this.columnResolver = columnResolver;
    }

    /**
     * Analyzes an INSERT operation and returns embedding injection directives.
     *
     * <p>All SYNC auto-embedding columns on the target table produce injection entries.
     * ASYNC columns produce entries that signal NULL-commit-then-enqueue behavior.</p>
     *
     * @param tableName fully qualified target table name
     * @return injection plan; empty directives list if no auto-embedding columns exist
     */
    public EmbedInjectionPlan analyzeInsert(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return EmbedInjectionPlan.EMPTY;
        }

        List<AutoEmbeddingColumnDescriptor> columns = columnResolver.resolveEmbeddingColumns(tableName);
        if (columns.isEmpty()) {
            return EmbedInjectionPlan.EMPTY;
        }

        LOG.debug("event=auto_embed_insert_detected table={} embeddingColumns={}",
                tableName, columns.size());

        List<EmbedDirective> directives = columns.stream()
                .map(col -> new EmbedDirective(
                        col.vectorColumn(),
                        col.modelName(),
                        col.sourceColumns(),
                        col.mode(),
                        col.dimension(),
                        col.sourceType()))
                .toList();

        return new EmbedInjectionPlan(tableName, DmlOperation.INSERT, directives);
    }

    /**
     * Analyzes an UPDATE operation and returns embedding injection directives
     * only for columns whose source columns overlap with the updated columns.
     *
     * <p>If none of the updated columns intersect with any embedding's source columns,
     * returns an empty plan (existing vectors are retained).</p>
     *
     * @param tableName      fully qualified target table name
     * @param updatedColumns set of column names being updated (SET clause columns)
     * @return injection plan with directives for affected embeddings only
     */
    public EmbedInjectionPlan analyzeUpdate(String tableName, Set<String> updatedColumns) {
        if (tableName == null || tableName.isBlank() || updatedColumns == null || updatedColumns.isEmpty()) {
            return EmbedInjectionPlan.EMPTY;
        }

        List<AutoEmbeddingColumnDescriptor> columns = columnResolver.resolveEmbeddingColumns(tableName);
        if (columns.isEmpty()) {
            return EmbedInjectionPlan.EMPTY;
        }

        Set<String> normalizedUpdated = updatedColumns.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toUnmodifiableSet());

        List<EmbedDirective> directives = new ArrayList<>();
        for (AutoEmbeddingColumnDescriptor col : columns) {
            boolean sourceChanged = col.sourceColumns().stream()
                    .anyMatch(src -> normalizedUpdated.contains(src.toUpperCase()));
            if (sourceChanged) {
                directives.add(new EmbedDirective(
                        col.vectorColumn(),
                        col.modelName(),
                        col.sourceColumns(),
                        col.mode(),
                        col.dimension(),
                        col.sourceType()));
            }
        }

        if (directives.isEmpty()) {
            LOG.debug("event=auto_embed_update_skipped table={} reason=no_source_column_changed", tableName);
            return EmbedInjectionPlan.EMPTY;
        }

        LOG.debug("event=auto_embed_update_detected table={} affectedEmbeddings={}",
                tableName, directives.size());

        return new EmbedInjectionPlan(tableName, DmlOperation.UPDATE, Collections.unmodifiableList(directives));
    }

    /**
     * The type of DML operation being analyzed.
     */
    public enum DmlOperation {
        INSERT,
        UPDATE
    }

    /**
     * A single embedding injection directive for one auto-embedding column.
     *
     * @param vectorColumn  target vector column name
     * @param modelName     embedding model to use
     * @param sourceColumns source columns to concatenate for embedding input
     * @param mode          SYNC (embed before commit) or ASYNC (NULL then enqueue)
     * @param dimension     expected output vector dimension
     * @param sourceType    source data type (TEXT or FILE)
     */
    public record EmbedDirective(
            String vectorColumn,
            String modelName,
            List<String> sourceColumns,
            EmbeddingMode mode,
            int dimension,
            SourceType sourceType) {

        public EmbedDirective {
            sourceColumns = List.copyOf(sourceColumns);
            if (sourceType == null) {
                sourceType = SourceType.TEXT;
            }
        }

        /** Returns true if this directive embeds FILE (blob) content. */
        public boolean isFileSource() {
            return sourceType == SourceType.FILE;
        }
    }

    /**
     * Result of analyzing a DML statement for auto-embedding injection.
     *
     * @param tableName  target table
     * @param operation  the DML operation type
     * @param directives ordered list of embedding directives to execute
     */
    public record EmbedInjectionPlan(
            String tableName,
            DmlOperation operation,
            List<EmbedDirective> directives) {

        /** Empty plan indicating no embedding injection is needed. */
        public static final EmbedInjectionPlan EMPTY =
                new EmbedInjectionPlan("", null, List.of());

        public EmbedInjectionPlan {
            directives = List.copyOf(directives);
        }

        /** Returns true if this plan contains no directives. */
        public boolean isEmpty() {
            return directives.isEmpty();
        }

        /** Returns true if any directive requires synchronous embedding. */
        public boolean hasSyncDirectives() {
            return directives.stream().anyMatch(d -> d.mode() == EmbeddingMode.SYNC);
        }

        /** Returns true if any directive uses asynchronous embedding. */
        public boolean hasAsyncDirectives() {
            return directives.stream().anyMatch(d -> d.mode() == EmbeddingMode.ASYNC);
        }
    }
}
