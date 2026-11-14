package org.pangrova.coordinator.indexing;

import org.pangrova.indexing.api.enums.IndexType;

import java.util.Map;
import java.util.Set;

/**
 * Column type compatibility matrix for index types.
 * Determines which SQL column types are valid for each index type.
 */
public final class IndexTypeCompatibility {

    private IndexTypeCompatibility() {}

    private static final Set<String> NUMERIC_TYPES = Set.of(
            "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
            "FLOAT", "DOUBLE", "DECIMAL", "REAL");

    private static final Set<String> STRING_TYPES = Set.of(
            "VARCHAR", "CHAR", "STRING", "TEXT");

    private static final Set<String> TEMPORAL_TYPES = Set.of(
            "DATE", "TIMESTAMP", "TIME");

    private static final Set<String> ORDERED_TYPES = union(NUMERIC_TYPES, STRING_TYPES, TEMPORAL_TYPES);

    private static final Set<String> EQUALITY_TYPES = union(
            ORDERED_TYPES, Set.of("BOOLEAN", "BINARY", "VARBINARY"));

    /** Single-column-only index types (multi-column not allowed). */
    public static final Set<IndexType> SINGLE_COLUMN_ONLY = Set.of(
            IndexType.BLOOM_FILTER, IndexType.PREFIX,
            IndexType.NGRAM_BLOOM_FILTER, IndexType.BITMAP, IndexType.HASH,
            IndexType.SPARSE, IndexType.GIN, IndexType.VECTOR);

    private static final Map<IndexType, Set<String>> COMPATIBLE_TYPES = Map.ofEntries(
            Map.entry(IndexType.BTREE, ORDERED_TYPES),
            Map.entry(IndexType.HASH, EQUALITY_TYPES),
            Map.entry(IndexType.BLOOM_FILTER, EQUALITY_TYPES),
            Map.entry(IndexType.BITMAP, union(NUMERIC_TYPES, STRING_TYPES, Set.of("BOOLEAN"))),
            Map.entry(IndexType.SPARSE, ORDERED_TYPES),
            Map.entry(IndexType.INVERTED, STRING_TYPES),
            Map.entry(IndexType.PREFIX, STRING_TYPES),
            Map.entry(IndexType.NGRAM_BLOOM_FILTER, STRING_TYPES),
            Map.entry(IndexType.R_TREE, NUMERIC_TYPES),
            Map.entry(IndexType.GIN, union(STRING_TYPES, Set.of("ARRAY"))),
            Map.entry(IndexType.VECTOR, Set.of("VECTOR"))
    );

    /**
     * Checks whether the given SQL type is compatible with the index type.
     *
     * @param indexType the index type
     * @param sqlType   the SQL column type (upper-cased)
     * @return true if compatible
     */
    public static boolean isCompatible(IndexType indexType, String sqlType) {
        Set<String> allowed = COMPATIBLE_TYPES.get(indexType);
        if (allowed == null) {
            return false;
        }
        String normalized = normalizeSqlType(sqlType);
        return allowed.contains(normalized);
    }

    private static String normalizeSqlType(String sqlType) {
        String upper = sqlType.toUpperCase().trim();
        // Strip precision/scale: VARCHAR(255) → VARCHAR, DECIMAL(10,2) → DECIMAL
        int parenIdx = upper.indexOf('(');
        if (parenIdx > 0) {
            upper = upper.substring(0, parenIdx).trim();
        }
        return upper;
    }

    @SafeVarargs
    private static Set<String> union(Set<String>... sets) {
        var result = new java.util.HashSet<String>();
        for (Set<String> s : sets) {
            result.addAll(s);
        }
        return Set.copyOf(result);
    }
}
