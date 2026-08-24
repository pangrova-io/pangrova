package org.pangrova.coordinator.ai.training;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses and validates restricted split expressions for training data partitioning.
 *
 * <p>Split expressions use a restricted predicate grammar supporting only:
 * {@code hash(column) % divisor op value} where op is one of {@code <, <=, >, >=, ==, !=}.
 * Compound expressions are supported via {@code AND}.</p>
 *
 * <p>Examples:</p>
 * <ul>
 *   <li>{@code hash(id) % 10 < 8} — first 80% of data</li>
 *   <li>{@code hash(id) % 10 >= 8 AND hash(id) % 10 < 9} — 10% validation set</li>
 * </ul>
 *
 * <p>Arbitrary SQL is NOT allowed. Invalid expressions fail with
 * INVALID_SPLIT_EXPRESSION (3014).</p>
 */
public final class SplitExpressionParser {

    /**
     * Pattern for a single split predicate: hash(column) % divisor op value.
     * Groups: 1=column, 2=divisor, 3=operator, 4=value.
     */
    private static final Pattern PREDICATE_PATTERN = Pattern.compile(
            "hash\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)\\s*%\\s*(\\d+)\\s*([<>!=]=?|==)\\s*(\\d+)");

    private static final Pattern COMPOUND_SPLIT = Pattern.compile(
            "\\s+AND\\s+", Pattern.CASE_INSENSITIVE);

    private SplitExpressionParser() {
        // utility class
    }

    /**
     * Validates a split expression and returns a parsed representation.
     *
     * @param expression the raw split expression string
     * @return parsed split descriptor
     * @throws InvalidSplitExpressionException if the expression does not conform to the grammar
     */
    public static SplitDescriptor parse(String expression) {
        Objects.requireNonNull(expression, "expression must not be null");
        if (expression.isBlank()) {
            throw new InvalidSplitExpressionException(expression, "expression must not be blank");
        }

        String[] predicates = COMPOUND_SPLIT.split(expression.trim());
        SplitPredicate[] parsed = new SplitPredicate[predicates.length];

        for (int i = 0; i < predicates.length; i++) {
            parsed[i] = parseSinglePredicate(predicates[i].trim(), expression);
        }

        return new SplitDescriptor(expression, parsed);
    }

    private static SplitPredicate parseSinglePredicate(String predicate, String fullExpression) {
        Matcher matcher = PREDICATE_PATTERN.matcher(predicate);
        if (!matcher.matches()) {
            throw new InvalidSplitExpressionException(fullExpression,
                    "predicate does not match grammar 'hash(column) % divisor op value': " + predicate);
        }

        String column = matcher.group(1);
        int divisor = Integer.parseInt(matcher.group(2));
        String operator = matcher.group(3);
        int value = Integer.parseInt(matcher.group(4));

        if (divisor <= 0) {
            throw new InvalidSplitExpressionException(fullExpression,
                    "divisor must be positive, got: " + divisor);
        }
        if (value < 0 || value >= divisor) {
            throw new InvalidSplitExpressionException(fullExpression,
                    "value must be in [0, divisor), got value=" + value + " divisor=" + divisor);
        }

        return new SplitPredicate(column, divisor, operator, value);
    }

    /**
     * A parsed split descriptor containing one or more predicates joined by AND.
     *
     * @param rawExpression the original expression string
     * @param predicates    parsed predicates
     */
    public record SplitDescriptor(String rawExpression, SplitPredicate[] predicates) {
        public SplitDescriptor {
            Objects.requireNonNull(rawExpression, "rawExpression must not be null");
            Objects.requireNonNull(predicates, "predicates must not be null");
            if (predicates.length == 0) {
                throw new IllegalArgumentException("predicates must not be empty");
            }
            predicates = predicates.clone();
        }

        @Override
        public SplitPredicate[] predicates() {
            return predicates.clone();
        }
    }

    /**
     * A single hash-based split predicate.
     *
     * @param column   the column to hash
     * @param divisor  the modulo divisor
     * @param operator comparison operator
     * @param value    the comparison value
     */
    public record SplitPredicate(String column, int divisor, String operator, int value) {
        public SplitPredicate {
            Objects.requireNonNull(column, "column must not be null");
            Objects.requireNonNull(operator, "operator must not be null");
        }
    }
}
