package org.pangrova.coordinator.ai.feature;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Registry for built-in feature transforms available in feature Dynamic Tables.
 *
 * <p>Registers and resolves the four built-in transforms (TOKENIZE, NORMALIZE,
 * ONE_HOT_ENCODE, BUCKETIZE) that can appear in the defining query of a
 * feature Dynamic Table ({@code WITH (type = 'feature')}).</p>
 *
 * <p>Each registered transform is associated with a {@link FeatureTransformDescriptor}
 * that describes its input/output contract. The registry is populated at startup
 * with all built-in transforms and is immutable thereafter.</p>
 *
 * <p>Thread safety: this class is safe for concurrent reads after construction.
 * No mutation occurs after the factory method returns.</p>
 */
@ThreadSafe
public final class FeatureTransformRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureTransformRegistry.class);

    private final Map<FeatureTransformType, FeatureTransformDescriptor> transforms;

    private FeatureTransformRegistry(Map<FeatureTransformType, FeatureTransformDescriptor> transforms) {
        this.transforms = Collections.unmodifiableMap(new EnumMap<>(transforms));
    }

    /**
     * Creates a registry pre-populated with all built-in feature transforms.
     *
     * @return a fully initialized registry with TOKENIZE, NORMALIZE, ONE_HOT_ENCODE, BUCKETIZE
     */
    public static FeatureTransformRegistry createWithBuiltins() {
        EnumMap<FeatureTransformType, FeatureTransformDescriptor> map = new EnumMap<>(FeatureTransformType.class);

        map.put(FeatureTransformType.TOKENIZE, new FeatureTransformDescriptor(
                FeatureTransformType.TOKENIZE,
                "VARCHAR",
                "ARRAY<VARCHAR>",
                2,
                "Splits text into token strings using the specified tokenizer"));

        map.put(FeatureTransformType.NORMALIZE, new FeatureTransformDescriptor(
                FeatureTransformType.NORMALIZE,
                "NUMERIC",
                "DOUBLE",
                2,
                "Applies statistical normalization (z-score or min-max) to numeric values"));

        map.put(FeatureTransformType.ONE_HOT_ENCODE, new FeatureTransformDescriptor(
                FeatureTransformType.ONE_HOT_ENCODE,
                "VARCHAR",
                "ARRAY<INT>",
                2,
                "Encodes categorical values as binary one-hot vectors"));

        map.put(FeatureTransformType.BUCKETIZE, new FeatureTransformDescriptor(
                FeatureTransformType.BUCKETIZE,
                "NUMERIC",
                "INT",
                2,
                "Assigns numeric values to discrete buckets based on boundary array"));

        LOG.info("event=feature_transform_registry_initialized transform_count={}", map.size());
        return new FeatureTransformRegistry(map);
    }

    /**
     * Resolves a transform descriptor by type.
     *
     * @param type the transform type to look up
     * @return the descriptor for the given type
     * @throws IllegalArgumentException if the type is not registered
     */
    public FeatureTransformDescriptor resolve(FeatureTransformType type) {
        Objects.requireNonNull(type, "type must not be null");
        FeatureTransformDescriptor descriptor = transforms.get(type);
        if (descriptor == null) {
            throw new IllegalArgumentException("No descriptor registered for transform: " + type);
        }
        return descriptor;
    }

    /**
     * Resolves a transform descriptor by SQL function name (case-insensitive).
     *
     * @param functionName the SQL function name (e.g., "TOKENIZE", "normalize")
     * @return the descriptor for the matching transform
     * @throws IllegalArgumentException if no matching transform exists
     */
    public FeatureTransformDescriptor resolveByFunctionName(String functionName) {
        FeatureTransformType type = FeatureTransformType.fromFunctionName(functionName);
        return resolve(type);
    }

    /**
     * Checks whether a given SQL function name corresponds to a registered feature transform.
     *
     * @param functionName the SQL function name to check
     * @return true if the function is a registered feature transform
     */
    public boolean isFeatureTransform(String functionName) {
        if (functionName == null || functionName.isBlank()) {
            return false;
        }
        try {
            FeatureTransformType.fromFunctionName(functionName);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Returns the set of all registered transform types.
     *
     * @return unmodifiable set of registered transform types
     */
    public Set<FeatureTransformType> registeredTypes() {
        return transforms.keySet();
    }

    /**
     * Returns the total number of registered transforms.
     *
     * @return count of registered transforms
     */
    public int size() {
        return transforms.size();
    }
}
