package org.pangrova.planner.substrait;

import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ImmutableParameterizedType;
import io.substrait.function.TypeExpression;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps Calcite SqlOperator to Substrait function references and extension variants.
 * Builds variants manually to avoid ANTLR version conflict from SimpleExtension.loadDefaults().
 */
public class FunctionMapper {

    /**
     * Wildcard placeholder return type used when the SDK serializer requires a
     * non-null returnType but the actual type must be resolved by the consumer
     * from the input expression. Using a Substrait type parameter ("any1")
     * matches the substrait spec for type-parameter-driven resolution and
     * avoids misrepresenting unrelated functions as I32/I64.
     *
     * <p>This is intentionally shared across scalar / aggregate / window variants
     * because the consumer never trusts variant.returnType() during execution
     * (see {@code SubstraitConverter} — output types come from input types).</p>
     */
    private static final TypeExpression PLACEHOLDER_RETURN_TYPE =
            ImmutableParameterizedType.StringLiteral.builder()
                    .nullable(true)
                    .value("any1")
                    .build();

    private static final String ARITHMETIC_URI = "/functions_arithmetic.yaml";
    private static final String COMPARISON_URI = "/functions_comparison.yaml";
    private static final String BOOLEAN_URI = "/functions_boolean.yaml";
    private static final String AGGREGATE_URI = "/functions_aggregate_generic.yaml";

    private static final Map<SqlKind, FunctionReference> SCALAR_MAP = Map.ofEntries(
            Map.entry(SqlKind.PLUS, new FunctionReference("arithmetic", "add")),
            Map.entry(SqlKind.MINUS, new FunctionReference("arithmetic", "subtract")),
            Map.entry(SqlKind.TIMES, new FunctionReference("arithmetic", "multiply")),
            Map.entry(SqlKind.DIVIDE, new FunctionReference("arithmetic", "divide")),
            Map.entry(SqlKind.MOD, new FunctionReference("arithmetic", "modulus")),
            Map.entry(SqlKind.EQUALS, new FunctionReference("comparison", "equal")),
            Map.entry(SqlKind.NOT_EQUALS, new FunctionReference("comparison", "not_equal")),
            Map.entry(SqlKind.LESS_THAN, new FunctionReference("comparison", "lt")),
            Map.entry(SqlKind.GREATER_THAN, new FunctionReference("comparison", "gt")),
            Map.entry(SqlKind.LESS_THAN_OR_EQUAL, new FunctionReference("comparison", "lte")),
            Map.entry(SqlKind.GREATER_THAN_OR_EQUAL, new FunctionReference("comparison", "gte")),
            Map.entry(SqlKind.AND, new FunctionReference("boolean", "and")),
            Map.entry(SqlKind.OR, new FunctionReference("boolean", "or")),
            Map.entry(SqlKind.NOT, new FunctionReference("boolean", "not")),
            Map.entry(SqlKind.IS_NOT_NULL, new FunctionReference("comparison", "is_not_null")),
            Map.entry(SqlKind.IS_NULL, new FunctionReference("comparison", "is_null")),
            Map.entry(SqlKind.IN, new FunctionReference("set_membership", "in")),
            Map.entry(SqlKind.LIKE, new FunctionReference("string", "like")),
            Map.entry(SqlKind.COALESCE, new FunctionReference("boolean", "coalesce")),
            Map.entry(SqlKind.GROUPING, new FunctionReference("aggregate", "grouping")),
            Map.entry(SqlKind.ROW, new FunctionReference("struct", "row_constructor")),
            Map.entry(SqlKind.ITEM, new FunctionReference("collection", "subscript"))
    );

    private static final Map<String, FunctionReference> NAMED_FUNCTION_MAP = Map.ofEntries(
            Map.entry("||", new FunctionReference("string", "concat")),
            Map.entry("CONCAT", new FunctionReference("string", "concat")),
            Map.entry("UPPER", new FunctionReference("string", "upper")),
            Map.entry("LOWER", new FunctionReference("string", "lower")),
            Map.entry("SUBSTRING", new FunctionReference("string", "substring")),
            Map.entry("TRIM", new FunctionReference("string", "trim")),
            Map.entry("CHAR_LENGTH", new FunctionReference("string", "char_length")),
            Map.entry("LENGTH", new FunctionReference("string", "char_length")),
            Map.entry("CHARACTER_LENGTH", new FunctionReference("string", "char_length")),
            Map.entry("ABS", new FunctionReference("arithmetic", "abs")),
            Map.entry("MOD", new FunctionReference("arithmetic", "modulus")),
            Map.entry("POWER", new FunctionReference("arithmetic", "power")),
            Map.entry("SQRT", new FunctionReference("arithmetic", "sqrt")),
            // Rounding family — native VeloxPlanConverter maps round/ceil/floor 1:1.
            Map.entry("ROUND", new FunctionReference("arithmetic", "round")),
            Map.entry("CEIL", new FunctionReference("arithmetic", "ceil")),
            Map.entry("CEILING", new FunctionReference("arithmetic", "ceil")),
            Map.entry("FLOOR", new FunctionReference("arithmetic", "floor")),
            Map.entry("ST_INTERSECTS", new FunctionReference("spatial", "st_intersects")),
            Map.entry("ST_WITHIN", new FunctionReference("spatial", "st_within")),
            Map.entry("ST_CONTAINS", new FunctionReference("spatial", "st_contains")),
            Map.entry("ST_GEOMFROMTEXT", new FunctionReference("spatial", "st_geomfromtext")),
            Map.entry("ST_GEOGRAPHYFROMTEXT", new FunctionReference("spatial", "st_geographyfromtext")),
            Map.entry("ARRAY_CONTAINS", new FunctionReference("array", "contains")),
            Map.entry("ARRAY_CONTAINS_ALL", new FunctionReference("array", "array_contains_all")),
            Map.entry("L2_DISTANCE", new FunctionReference("vector", "l2_distance")),
            Map.entry("COSINE_DISTANCE", new FunctionReference("vector", "cosine_distance")),
            Map.entry("INNER_PRODUCT", new FunctionReference("vector", "inner_product")),
            Map.entry("COSINE_SIMILARITY", new FunctionReference("vector", "cosine_similarity")),
            Map.entry("APPROX_COUNT_DISTINCT", new FunctionReference("aggregate", "approx_distinct")),
            Map.entry("VARIANT_GET", new FunctionReference("string", "json_extract_scalar")),
            Map.entry("AI_COMPLETE", new FunctionReference("ai", "ai_complete")),
            Map.entry("AI_CLASSIFY", new FunctionReference("ai", "ai_classify")),
            Map.entry("AI_SUMMARIZE", new FunctionReference("ai", "ai_summarize")),
            Map.entry("AI_FILTER", new FunctionReference("ai", "ai_filter")),
            Map.entry("AI_EMBED", new FunctionReference("ai", "ai_embed")),
            Map.entry("AI_EXTRACT", new FunctionReference("ai", "ai_extract")),
            Map.entry("AI_GUARD", new FunctionReference("ai", "ai_guard")),
            Map.entry("AI_PREDICT", new FunctionReference("ai", "ai_predict")),
            Map.entry("AI_TRANSLATE", new FunctionReference("ai", "ai_translate"))
    );

    private static final Map<SqlKind, FunctionReference> AGGREGATE_MAP = Map.ofEntries(
            Map.entry(SqlKind.SUM, new FunctionReference("aggregate", "sum")),
            Map.entry(SqlKind.SUM0, new FunctionReference("aggregate", "sum0")),
            Map.entry(SqlKind.COUNT, new FunctionReference("aggregate", "count")),
            Map.entry(SqlKind.MIN, new FunctionReference("aggregate", "min")),
            Map.entry(SqlKind.MAX, new FunctionReference("aggregate", "max")),
            Map.entry(SqlKind.AVG, new FunctionReference("aggregate", "avg")),
            Map.entry(SqlKind.STDDEV_POP, new FunctionReference("aggregate", "stddev_pop")),
            Map.entry(SqlKind.STDDEV_SAMP, new FunctionReference("aggregate", "stddev_samp")),
            Map.entry(SqlKind.VAR_POP, new FunctionReference("aggregate", "variance_pop")),
            Map.entry(SqlKind.VAR_SAMP, new FunctionReference("aggregate", "variance_samp")),
            Map.entry(SqlKind.SOME, new FunctionReference("aggregate", "some")),
            Map.entry(SqlKind.ANY_VALUE, new FunctionReference("aggregate", "any_value")),
            Map.entry(SqlKind.COLLECT, new FunctionReference("aggregate", "collect")),
            Map.entry(SqlKind.ARRAY_AGG, new FunctionReference("aggregate", "collect"))
    );

    private static final Set<String> WINDOW_FUNCTION_NAMES = Set.of(
            "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
            "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
            "COUNT", "SUM", "SUM0", "$SUM0", "MIN", "MAX", "AVG"
    );

    private final Map<String, SimpleExtension.ScalarFunctionVariant> scalarCache =
            new ConcurrentHashMap<>();
    private final Map<String, SimpleExtension.AggregateFunctionVariant> aggregateCache =
            new ConcurrentHashMap<>();
    private final Map<String, SimpleExtension.WindowFunctionVariant> windowCache =
            new ConcurrentHashMap<>();

    /**
     * Maps a Calcite SQL operator to its Substrait function reference.
     *
     * @param operator operator to map
     * @return resolved Substrait function reference
     */
    public FunctionReference map(SqlOperator operator) {
        // Name-based lookup first — handles functions like APPROX_COUNT_DISTINCT
        // that share a SqlKind (COUNT) with a different function.
        String upperName = operator.getName().toUpperCase(java.util.Locale.ROOT);
        FunctionReference ref = NAMED_FUNCTION_MAP.get(upperName);
        if (ref == null) {
            ref = SCALAR_MAP.get(operator.getKind());
        }
        if (ref == null) {
            ref = AGGREGATE_MAP.get(operator.getKind());
        }
        if (ref == null) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_FUNCTION_MAPPING_FAILURE,
                    "Unsupported operator: " + operator.getName());
        }
        return ref;
    }

    public SimpleExtension.ScalarFunctionVariant resolveScalarVariant(FunctionReference ref) {
        return scalarCache.computeIfAbsent(ref.compoundName(), k -> buildScalarVariant(ref));
    }

    public SimpleExtension.AggregateFunctionVariant resolveAggregateVariant(FunctionReference ref) {
        return aggregateCache.computeIfAbsent(ref.compoundName(), k -> buildAggregateVariant(ref));
    }

    public boolean isAggregate(SqlKind kind) {
        return AGGREGATE_MAP.containsKey(kind);
    }

    public boolean isWindowFunction(String name) {
        return WINDOW_FUNCTION_NAMES.contains(name.toUpperCase());
    }

    /** Resolves a window function by name (e.g. ROW_NUMBER, RANK). */
    public FunctionReference mapWindowFunction(String name) {
        String upper = name.toUpperCase();
        if (!WINDOW_FUNCTION_NAMES.contains(upper)) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_FUNCTION_MAPPING_FAILURE,
                    "Unsupported window function: " + name);
        }
        String normalized = upper.startsWith("$") ? upper.substring(1) : upper;
        // The native engine does not have a sum0 window function; map to sum.
        // Calcite's $SUM0 returns 0 instead of NULL for empty sets, but in
        // window context the frame always has at least the current row.
        if (normalized.equals("SUM0")) {
            normalized = "SUM";
        }
        return new FunctionReference("window", normalized.toLowerCase());
    }

    public SimpleExtension.WindowFunctionVariant resolveWindowVariant(FunctionReference ref) {
        return windowCache.computeIfAbsent(ref.compoundName(), k -> buildWindowVariant(ref));
    }

    /**
     * Builds a scalar variant with a Substrait wildcard placeholder return type ("any1").
     * The consumer always derives the concrete return type from input expression types,
     * so the variant return type is intentionally a type parameter rather than a
     * misleading concrete type like I32.
     */
    private static SimpleExtension.ScalarFunctionVariant buildScalarVariant(FunctionReference ref) {
        return ImmutableSimpleExtension.ScalarFunctionVariant.builder()
                .name(ref.name())
                .uri(resolveUri(ref.namespace()))
                .returnType(PLACEHOLDER_RETURN_TYPE)
                .build();
    }

    /**
     * Builds an aggregate variant with a Substrait wildcard placeholder return type.
     * See {@link #buildScalarVariant(FunctionReference)} for rationale.
     */
    private static SimpleExtension.AggregateFunctionVariant buildAggregateVariant(FunctionReference ref) {
        return ImmutableSimpleExtension.AggregateFunctionVariant.builder()
                .name(ref.name())
                .uri(resolveUri(ref.namespace()))
                .returnType(PLACEHOLDER_RETURN_TYPE)
                .build();
    }

    /**
     * Builds a window variant with a Substrait wildcard placeholder return type.
     * See {@link #buildScalarVariant(FunctionReference)} for rationale.
     */
    private static SimpleExtension.WindowFunctionVariant buildWindowVariant(FunctionReference ref) {
        return ImmutableSimpleExtension.WindowFunctionVariant.builder()
                .name(ref.name())
                .uri(resolveUri(ref.namespace()))
                .returnType(PLACEHOLDER_RETURN_TYPE)
                .windowType(SimpleExtension.WindowType.PARTITION)
                .build();
    }

    private static String resolveUri(String namespace) {
        return switch (namespace) {
            case "arithmetic" -> ARITHMETIC_URI;
            case "comparison" -> COMPARISON_URI;
            case "boolean" -> BOOLEAN_URI;
            case "aggregate" -> AGGREGATE_URI;
            case "set_membership" -> "/functions_set.yaml";
            case "string" -> "/functions_string.yaml";
            case "spatial" -> "/functions_spatial.yaml";
            case "array" -> "/functions_array.yaml";
            case "window" -> "/functions_window.yaml";
            case "vector" -> "/functions_vector.yaml";
            default -> "/" + namespace + ".yaml";
        };
    }
}
