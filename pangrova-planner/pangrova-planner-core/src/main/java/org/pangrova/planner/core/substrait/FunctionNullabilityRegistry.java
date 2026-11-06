package org.pangrova.planner.core.substrait;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Declares the output {@link FunctionNullability} of the functions that {@link FunctionMapper}
 * maps to Substrait extension variants. The variant builders attach only a nullable placeholder
 * return type; this registry supplies the per-function nullability semantics needed to infer a
 * correct, non-lossy result nullability (cf. F-SUBSTRAIT-05 / G-IR-04).
 *
 * <p>Lookups are by {@link FunctionReference#compoundName()} ({@code namespace:name}). Unknown
 * functions fall back to {@link FunctionNullability#ALWAYS_NULLABLE} — the safe over-approximation.</p>
 */
public final class FunctionNullabilityRegistry {

    private static final Map<String, FunctionNullability> RULES = Map.ofEntries(
            // Arithmetic — MIRROR.
            entry("arithmetic", "add", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "subtract", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "multiply", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "divide", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "abs", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "modulus", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "power", FunctionNullability.NULL_IF_ANY_NULL),
            entry("arithmetic", "sqrt", FunctionNullability.NULL_IF_ANY_NULL),
            // Comparison — MIRROR.
            entry("comparison", "equal", FunctionNullability.NULL_IF_ANY_NULL),
            entry("comparison", "not_equal", FunctionNullability.NULL_IF_ANY_NULL),
            entry("comparison", "lt", FunctionNullability.NULL_IF_ANY_NULL),
            entry("comparison", "gt", FunctionNullability.NULL_IF_ANY_NULL),
            entry("comparison", "lte", FunctionNullability.NULL_IF_ANY_NULL),
            entry("comparison", "gte", FunctionNullability.NULL_IF_ANY_NULL),
            // Null predicates — always a non-null boolean.
            entry("comparison", "is_null", FunctionNullability.NEVER_NULL),
            entry("comparison", "is_not_null", FunctionNullability.NEVER_NULL),
            // Boolean — three-valued logic: and(null,false)=false, or(null,true)=true, so not MIRROR.
            entry("boolean", "and", FunctionNullability.ALWAYS_NULLABLE),
            entry("boolean", "or", FunctionNullability.ALWAYS_NULLABLE),
            entry("boolean", "not", FunctionNullability.NULL_IF_ANY_NULL),
            entry("boolean", "coalesce", FunctionNullability.NULL_IF_ALL_NULL),
            // String — MIRROR.
            entry("string", "concat", FunctionNullability.NULL_IF_ANY_NULL),
            entry("string", "upper", FunctionNullability.NULL_IF_ANY_NULL),
            entry("string", "lower", FunctionNullability.NULL_IF_ANY_NULL),
            entry("string", "substring", FunctionNullability.NULL_IF_ANY_NULL),
            entry("string", "trim", FunctionNullability.NULL_IF_ANY_NULL),
            entry("string", "char_length", FunctionNullability.NULL_IF_ANY_NULL),
            entry("string", "like", FunctionNullability.NULL_IF_ANY_NULL),
            // AI and FILE infrastructure functions propagate SQL NULL inputs.
            // Provider failures are exceptions, never SQL NULL fallbacks.
            entry("ai", "ai_complete", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_classify", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_summarize", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_filter", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_embed", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_extract", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_guard", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_predict", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_translate", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_describe_image", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_parse_document", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_transcribe", FunctionNullability.NULL_IF_ANY_NULL),
            entry("ai", "ai_ocr", FunctionNullability.NULL_IF_ANY_NULL),
            entry("file", "file_from_url", FunctionNullability.NULL_IF_ANY_NULL),
            entry("file", "file_from_bytes", FunctionNullability.NULL_IF_ANY_NULL),
            // Aggregates — count/sum0/approx_distinct are non-null; the rest are null on empty input.
            entry("aggregate", "count", FunctionNullability.NEVER_NULL),
            entry("aggregate", "sum0", FunctionNullability.NEVER_NULL),
            entry("aggregate", "approx_distinct", FunctionNullability.NEVER_NULL),
            entry("aggregate", "sum", FunctionNullability.ALWAYS_NULLABLE),
            entry("aggregate", "min", FunctionNullability.ALWAYS_NULLABLE),
            entry("aggregate", "max", FunctionNullability.ALWAYS_NULLABLE),
            entry("aggregate", "avg", FunctionNullability.ALWAYS_NULLABLE));

    private static Map.Entry<String, FunctionNullability> entry(
            String namespace, String name, FunctionNullability rule) {
        return Map.entry(namespace + ":" + name, rule);
    }

    /** @return the declared nullability rule for the function, or ALWAYS_NULLABLE if unknown. */
    public FunctionNullability rule(FunctionReference ref) {
        Objects.requireNonNull(ref, "ref must not be null");
        return RULES.getOrDefault(ref.compoundName(), FunctionNullability.ALWAYS_NULLABLE);
    }

    /** @return true when this function is registered with an explicit (non-default) rule. */
    public boolean isDeclared(FunctionReference ref) {
        Objects.requireNonNull(ref, "ref must not be null");
        return RULES.containsKey(ref.compoundName());
    }

    /**
     * Infers the output nullability of a function call from its argument nullabilities, using the
     * declared rule.
     *
     * @param ref              the function
     * @param argNullabilities per-argument nullability, in argument order
     * @return true when the result may be null
     */
    public boolean outputNullable(FunctionReference ref, List<Boolean> argNullabilities) {
        Objects.requireNonNull(argNullabilities, "argNullabilities must not be null");
        return rule(ref).outputNullable(argNullabilities);
    }
}
