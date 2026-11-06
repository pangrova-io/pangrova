package org.pangrova.planner.core.substrait;

import java.util.List;

/**
 * Declared output-nullability rule for a Substrait function, making explicit the nullability
 * that the hand-rolled variant builders previously left as a blanket nullable placeholder.
 * Mirrors the nullability handling Substrait function definitions express via
 * {@code nullability: MIRROR / DECLARED_OUTPUT}.
 */
public enum FunctionNullability {

    /**
     * Output is nullable iff any argument is nullable (Substrait {@code MIRROR}). Covers most
     * arithmetic/comparison/string functions: a null operand yields a null result.
     */
    NULL_IF_ANY_NULL {
        @Override
        public boolean outputNullable(List<Boolean> argNullabilities) {
            return argNullabilities.stream().anyMatch(Boolean::booleanValue);
        }
    },

    /**
     * Output is nullable only when every argument is nullable. Covers {@code coalesce}: the
     * result is non-null as long as at least one argument is guaranteed non-null.
     */
    NULL_IF_ALL_NULL {
        @Override
        public boolean outputNullable(List<Boolean> argNullabilities) {
            return !argNullabilities.isEmpty() && argNullabilities.stream().allMatch(Boolean::booleanValue);
        }
    },

    /**
     * Output is never null regardless of inputs. Covers {@code is_null}/{@code is_not_null}
     * (always a non-null boolean), {@code count}/{@code sum0} (non-null numeric).
     */
    NEVER_NULL {
        @Override
        public boolean outputNullable(List<Boolean> argNullabilities) {
            return false;
        }
    },

    /**
     * Output may be null irrespective of input nullability. Conservative default; also the
     * correct declaration for three-valued {@code and}/{@code or} and for aggregates that
     * return null on empty groups ({@code sum}/{@code min}/{@code max}/{@code avg}).
     */
    ALWAYS_NULLABLE {
        @Override
        public boolean outputNullable(List<Boolean> argNullabilities) {
            return true;
        }
    };

    /**
     * Computes whether the function's output can be null given which arguments can be null.
     *
     * @param argNullabilities per-argument nullability, in argument order
     * @return true when the output may be null
     */
    public abstract boolean outputNullable(List<Boolean> argNullabilities);
}
