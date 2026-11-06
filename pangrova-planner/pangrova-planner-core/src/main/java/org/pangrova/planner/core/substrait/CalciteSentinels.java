package org.pangrova.planner.core.substrait;

/**
 * F-PLANNER-R2-003 helper: centralised handling of Calcite "unspecified value"
 * sentinels.
 *
 * <p>Calcite's {@code RelDataTypeImpl} encodes "precision not specified" /
 * "scale not specified" / "field index unknown" as {@link Integer#MIN_VALUE}.
 * Constant folding (especially via REDUCE_EXPRESSIONS) and certain operator
 * rewrites can leak this sentinel into downstream type / cost / index
 * computation if not normalised. Before this helper existed, the check was
 * duplicated in 5+ files (TypeMapper x2, ExpressionConverter, SqlCompiler,
 * MaterializedViewRewritePass), each implementing the {@code <= 0 ||
 * MIN_VALUE} comparison slightly differently. New transformation paths kept
 * forgetting it, which surfaced as NPEs or wildly-off cost estimates.</p>
 */
public final class CalciteSentinels {

    /**
     * Calcite's "unspecified integer" sentinel value, used by
     * {@code RelDataTypeImpl} for precision/scale/index when no value is set.
     */
    public static final int UNSPECIFIED = Integer.MIN_VALUE;

    private CalciteSentinels() {
        // utility class
    }

    /**
     * @return true if {@code value} is the Calcite unspecified sentinel, or a
     *         non-positive value that should be treated as missing (Calcite uses
     *         both 0 and MIN_VALUE interchangeably depending on the path).
     */
    public static boolean isUnspecified(int value) {
        return value <= 0;
    }

    /**
     * @return {@code value} if it is a real (positive, non-sentinel) integer,
     *         otherwise {@code fallback}.
     */
    public static int orElse(int value, int fallback) {
        return isUnspecified(value) ? fallback : value;
    }

    /**
     * Strict variant: throws {@link IllegalArgumentException} if the value is
     * the sentinel. Use when downstream code cannot tolerate unspecified
     * values and should fail loudly rather than substitute a fallback.
     */
    public static int requireSpecified(int value, String what) {
        if (isUnspecified(value)) {
            throw new IllegalArgumentException(
                    "Calcite " + what + " is unspecified (sentinel value " + value + ")");
        }
        return value;
    }
}
