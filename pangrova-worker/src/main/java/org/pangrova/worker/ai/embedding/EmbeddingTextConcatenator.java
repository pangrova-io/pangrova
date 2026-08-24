package org.pangrova.worker.ai.embedding;

import java.util.List;
import java.util.Map;

/**
 * Shared utility for concatenating source column values into embedding input text.
 *
 * <p>Used by both {@link SyncEmbeddingExecutor} (DML path) and
 * {@link AsyncEmbeddingDrainer} (background drain path) to ensure identical
 * text construction semantics. Column values are joined with newline separators;
 * null values are silently skipped.</p>
 */
public final class EmbeddingTextConcatenator {

    private EmbeddingTextConcatenator() {}

    /**
     * Concatenates source column values with newline separator.
     *
     * <p>NULL columns are skipped. Column order follows the declaration order
     * in the source columns list. Returns an empty string if all values are null.</p>
     *
     * @param sourceColumns ordered list of column names to concatenate
     * @param values        map of column name to value for the row
     * @return concatenated text for embedding input (never null)
     */
    public static String concatenate(List<String> sourceColumns, Map<String, String> values) {
        StringBuilder sb = new StringBuilder();
        for (String col : sourceColumns) {
            String value = values.get(col);
            if (value != null) {
                if (!sb.isEmpty()) {
                    sb.append('\n');
                }
                sb.append(value);
            }
        }
        return sb.toString();
    }
}
