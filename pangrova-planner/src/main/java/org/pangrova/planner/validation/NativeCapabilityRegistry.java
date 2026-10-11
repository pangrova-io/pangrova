package org.pangrova.planner.validation;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.Set;

/**
 * Registry of native engine capabilities for Substrait plan validation.
 *
 * <p>Maintains immutable snapshots of supported Rel types, Expression types,
 * and function signatures. Thread safety is achieved via volatile snapshot
 * replacement — each {@link #update} publishes a new immutable {@code Set}
 * through a volatile write, and every read sees a consistent snapshot.</p>
 *
 * <p>This class has zero dependency on protobuf types. The proto→Set
 * conversion is performed by {@code pangrova-worker} side before calling
 * {@link #update}.</p>
 */
@ThreadSafe
public final class NativeCapabilityRegistry {

    /**
     * Immutable snapshot of all capability sets. A single volatile reference
     * ensures atomic visibility — readers never see a mix of old and new sets.
     */
    private record CapabilitySnapshot(
            Set<String> relTypes,
            Set<String> exprTypes,
            Set<String> functions) {
        CapabilitySnapshot {
            relTypes = Set.copyOf(relTypes);
            exprTypes = Set.copyOf(exprTypes);
            functions = Set.copyOf(functions);
        }
    }

    private volatile CapabilitySnapshot snapshot;

    private NativeCapabilityRegistry(
            Set<String> relTypes,
            Set<String> exprTypes,
            Set<String> functions) {
        this.snapshot = new CapabilitySnapshot(relTypes, exprTypes, functions);
    }

    /**
     * Creates a registry pre-populated with the default capability set,
     * aligned with {@code NativePlanConverter} supported operators.
     */
    public static NativeCapabilityRegistry defaults() {
        Set<String> defaultRelTypes = Set.of(
                "ReadRel",
                "FilterRel",
                "ProjectRel",
                "AggregateRel",
                "HashJoinRel",
                "MergeJoinRel",
                "JoinRel",
                "ExchangeRel",
                "ExtensionLeafRel",
                "ExtensionSingleRel",
                "SortRel",
                "FetchRel",
                "ConsistentPartitionWindowRel",
                "SetRel"
        );

        Set<String> defaultExprTypes = Set.of(
                "FieldReference",
                "Literal",
                "ScalarFunction",
                "Cast",
                "IfThen",
                "Nested",
                "WindowFunction"
        );

        // Default functions aligned with FunctionMapper scalar/aggregate/window mappings.
        // The native engine natively supports all standard Substrait comparison, arithmetic,
        // boolean, aggregate, string, and window functions.
        // Runtime discovery via NativeCapabilityLoader.update() can override this set.
        Set<String> defaultFunctions = Set.of(
                // comparison
                "equal", "not_equal", "lt", "gt", "lte", "gte",
                "is_null", "is_not_null",
                // arithmetic
                "add", "subtract", "multiply", "divide",
                "abs", "modulus", "power", "sqrt",
                "round",
                // Presto date extraction is registered by the packaged Velox runtime.
                "year", "month", "day", "hour", "minute",
                // boolean
                "and", "or", "not", "coalesce",
                // string
                "concat", "upper", "lower", "substring", "trim",
                "char_length", "like",
                // set membership
                "in",
                // spatial
                "st_intersects", "st_within", "st_contains",
                "st_geomfromtext", "st_geographyfromtext",
                // Exact canonical names emitted from sql-bindings.json.
                "subscript", "contains", "array_contains_all",
                // vector distance
                "l2_distance", "cosine_distance", "inner_product", "pangrova_cosine_similarity",
                // packaged Native scalar callbacks registered by the Worker function catalogue
                "file_from_bytes", "file_from_url",
                // aggregate
                "sum", "sum0", "count", "min", "max", "avg",
                "stddev_pop", "stddev_samp", "variance_pop", "variance_samp",
                "some", "any_value", "collect",
                // window
                "row_number", "rank", "dense_rank", "ntile",
                "lag", "lead", "first_value", "last_value"
        );

        return new NativeCapabilityRegistry(
                defaultRelTypes, defaultExprTypes, defaultFunctions);
    }

    /**
     * Replaces the capability snapshot with runtime-discovered values.
     *
     * <p>Called by {@code pangrova-worker} after converting the proto
     * {@code NativeCapabilityResponse} into plain Java sets.</p>
     *
     * @param relTypes  supported Substrait Rel type names
     * @param exprTypes supported Substrait Expression type names
     * @param functions supported function signatures
     */
    public void update(Set<String> relTypes,
                       Set<String> exprTypes,
                       Set<String> functions) {
        this.snapshot = new CapabilitySnapshot(relTypes, exprTypes, functions);
    }

    /**
     * Returns {@code true} if the given Substrait Rel type is supported
     * by the native engine.
     */
    public boolean isRelSupported(String relType) {
        return snapshot.relTypes().contains(relType);
    }

    /**
     * Returns {@code true} if the given Substrait Expression type is
     * supported by the native engine.
     */
    public boolean isExprSupported(String exprType) {
        return snapshot.exprTypes().contains(exprType);
    }

    /**
     * Returns {@code true} if the given function signature is supported
     * by the native engine.
     */
    public boolean isFunctionSupported(String functionSignature) {
        return snapshot.functions().contains(functionSignature);
    }

    /**
     * Returns {@code true} if the native engine supports distinct set operations
     * (UNION DISTINCT, INTERSECT, EXCEPT) natively, without requiring an explicit
     * deduplication aggregate wrapper at the Substrait level.
     *
     * <p>When this returns {@code true}, {@code SetOperationConverter} can emit
     * the native SetRel op directly (e.g. UNION_DISTINCT) instead of wrapping
     * UNION_ALL + GROUP BY all columns. This avoids double-deduplication overhead.</p>
     *
     * @see org.pangrova.planner.substrait.plan.SetOperationConverter
     */
    public boolean supportsDistinctSetOps() {
        return snapshot.relTypes().contains("SetRel.DISTINCT");
    }
}
