package org.pangrova.coordinator.ai.feature;

/**
 * Built-in feature transform types supported in feature Dynamic Tables.
 *
 * <p>Each transform type corresponds to a SQL function that can appear in
 * the defining query of a feature Dynamic Table (type = 'feature').</p>
 *
 * <ul>
 *   <li>{@link #TOKENIZE} — splits text into token strings (ARRAY&lt;VARCHAR&gt;)</li>
 *   <li>{@link #NORMALIZE} — applies statistical normalization (z-score, min-max)</li>
 *   <li>{@link #ONE_HOT_ENCODE} — encodes categorical values as binary vectors</li>
 *   <li>{@link #BUCKETIZE} — assigns numeric values to discrete buckets</li>
 * </ul>
 */
public enum FeatureTransformType {

    /**
     * Tokenizes text into an array of token strings.
     * SQL: {@code TOKENIZE(column, 'tokenizer_name')}
     * Returns: {@code ARRAY<VARCHAR>}
     */
    TOKENIZE("TOKENIZE"),

    /**
     * Applies statistical normalization to numeric values.
     * SQL: {@code NORMALIZE(column, 'strategy')}
     * Strategies: z-score, min-max.
     * Statistics (mean, stddev, min, max) are computed from the source table at refresh time.
     */
    NORMALIZE("NORMALIZE"),

    /**
     * Encodes categorical values as binary one-hot vectors.
     * SQL: {@code ONE_HOT_ENCODE(column, ARRAY['cat1', 'cat2', ...])}
     * Returns: {@code ARRAY<INT>} with exactly one element set to 1.
     */
    ONE_HOT_ENCODE("ONE_HOT_ENCODE"),

    /**
     * Assigns numeric values to discrete buckets based on boundary arrays.
     * SQL: {@code BUCKETIZE(column, ARRAY[1.0, 2.0, 3.0, ...])}
     * Returns: {@code INT} bucket index (0-based).
     */
    BUCKETIZE("BUCKETIZE");

    private final String functionName;

    FeatureTransformType(String functionName) {
        this.functionName = functionName;
    }

    /** Returns the SQL function name for this transform. */
    public String functionName() {
        return functionName;
    }

    /**
     * Resolves a transform type from its SQL function name (case-insensitive).
     *
     * @param name the function name to resolve
     * @return the matching transform type
     * @throws IllegalArgumentException if no matching transform type exists
     */
    public static FeatureTransformType fromFunctionName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Function name must not be null or blank");
        }
        String upper = name.toUpperCase();
        for (FeatureTransformType type : values()) {
            if (type.functionName.equals(upper)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown feature transform function: " + name);
    }
}
