package org.pangrova.security.api.rowfilter;

import org.pangrova.security.api.enums.FilterParameterType;
import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A row-level security filter expression with parameterized user values.
 *
 * <p>Instead of interpolating user-supplied values directly into SQL via string concatenation,
 * this class separates the expression template from bound parameter values. Parameters are
 * referenced in the template as indexed placeholders ({@code $1}, {@code $2}, etc.) and
 * resolved safely at render time using strict validation.
 *
 * <p>This prevents SQL injection because user values never participate in SQL string
 * concatenation — they are validated against a safe literal pattern and rejected if they
 * contain SQL metacharacters that could alter query semantics.
 */
@Experimental
public record ParameterizedFilter(
        String expressionTemplate,
        List<BoundParameter> parameters
) {

    private static final int MAX_REJECTED_VALUE_LENGTH = 50;

    /**
     * Pattern matching safe SQL string literal content: alphanumeric, spaces, underscores,
     * hyphens, dots, at-signs, and common punctuation that cannot break SQL syntax.
     * Rejects quotes, semicolons, comments, and other SQL metacharacters.
     */
    private static final Pattern SAFE_LITERAL_CONTENT = Pattern.compile(
            "^[\\w\\s.@\\-,+*/()=<>!&|^~#%]*$", Pattern.UNICODE_CHARACTER_CLASS);

    public ParameterizedFilter {
        parameters = parameters != null ? List.copyOf(parameters) : List.of();
    }

    /** Pattern to detect unresolved parameter placeholders ($0, $1, $99, etc.). */
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\d+");

    /**
     * Renders the parameterized filter into a safe SQL expression string.
     * Each parameter value is validated against a strict whitelist pattern before
     * being rendered as a SQL string literal. Values that fail validation cause
     * an {@link IllegalArgumentException} to be thrown, preventing injection.
     *
     * <p>Placeholders are replaced from highest index to lowest to prevent
     * prefix conflicts (e.g., {@code $1} matching inside {@code $10}).</p>
     *
     * @return the rendered SQL expression with parameters safely bound
     * @throws IllegalArgumentException if any parameter value contains unsafe characters,
     *         or if unresolved placeholders remain (out-of-range or zero-index)
     */
    public String toSafeExpression() {
        String result = expressionTemplate;
        // Replace from highest index to lowest to avoid prefix conflicts
        // ($1 matching inside $10, $11, etc.)
        for (int i = parameters.size() - 1; i >= 0; i--) {
            BoundParameter param = parameters.get(i);
            String safeLiteral = renderSafeLiteral(param);
            result = result.replace("$" + (i + 1), safeLiteral);
        }
        // Check for unresolved parameter placeholders (out-of-range or $0)
        Matcher m = PLACEHOLDER_PATTERN.matcher(result);
        if (m.find()) {
            String placeholder = m.group();
            throw new IllegalArgumentException(
                    "Unresolved placeholder " + placeholder
                    + "; " + parameters.size() + " parameters are bound");
        }
        return result;
    }

    private static String renderSafeLiteral(BoundParameter param) {
        String value = param.value();
        validateLiteralContent(value, param.name());
        return switch (param.type()) {
            case STRING -> "'" + value + "'";
            case STRING_LIST -> "'" + value + "'";
        };
    }

    /**
     * Validates that a value is safe to embed as a SQL string literal.
     * Rejects values containing SQL injection metacharacters (quotes, semicolons,
     * comment markers, etc.).
     *
     * @param value the value to validate
     * @param paramName the parameter name for error reporting
     * @throws IllegalArgumentException if the value contains unsafe characters
     */
    static void validateLiteralContent(String value, String paramName) {
        if (value == null) {
            return;
        }
        if (!SAFE_LITERAL_CONTENT.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "RLS parameter '%s' contains unsafe characters that could enable SQL injection. "
                            .formatted(paramName)
                    + "Value must match pattern: alphanumeric, spaces, underscores, hyphens, "
                    + "dots, at-signs, and common arithmetic operators. "
                    + "Rejected value (truncated): '%s'"
                            .formatted(truncate(value, MAX_REJECTED_VALUE_LENGTH)));
        }
    }

    private static String truncate(String s, int maxLen) {
        if (s == null || s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen) + "...";
    }

    /**
     * A bound parameter with its name, value, and type.
     */
    public record BoundParameter(String name, String value, FilterParameterType type) {
        public BoundParameter {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Parameter name must not be blank");
            }
        }
    }

}
