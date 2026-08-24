package org.pangrova.security.authz;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validates SQL identifiers and rejects DDL keywords in filter/mask expressions.
 */
public final class SqlIdentifierValidator {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    private static final Set<String> DDL_KEYWORDS = Set.of(
            "DROP", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"
    );

    private SqlIdentifierValidator() {}

    /**
     * Returns whether the value is a safe SQL identifier.
     */
    public static boolean isValidIdentifier(String identifier) {
        return identifier != null && SAFE_IDENTIFIER.matcher(identifier).matches();
    }

    /**
     * Validates a SQL identifier.
     *
     * @param identifier identifier to validate
     * @throws AuthorizationDeniedException when the identifier is unsafe
     */
    public static void validateIdentifier(String identifier) {
        if (!isValidIdentifier(identifier)) {
            throw new AuthorizationDeniedException("Invalid SQL identifier: " + identifier);
        }
    }

    /**
     * Returns whether an expression contains a DDL keyword.
     *
     * @param expression expression to inspect
     * @return true when a DDL keyword is present
     */
    public static boolean containsDdlKeyword(String expression) {
        if (expression == null) {
            return false;
        }
        String upper = expression.toUpperCase();
        return DDL_KEYWORDS.stream().anyMatch(kw -> containsWord(upper, kw));
    }

    /**
     * Rejects expressions containing DDL keywords.
     *
     * @param expression expression to inspect
     * @throws AuthorizationDeniedException when a DDL keyword is present
     */
    public static void rejectDdlInExpression(String expression) {
        if (containsDdlKeyword(expression)) {
            throw new AuthorizationDeniedException("DDL keywords not allowed in filter/mask expressions");
        }
    }

    private static boolean containsWord(String text, String word) {
        int idx = text.indexOf(word);
        if (idx < 0) {
            return false;
        }
        boolean startOk = idx == 0 || !Character.isLetterOrDigit(text.charAt(idx - 1));
        int end = idx + word.length();
        boolean endOk = end >= text.length() || !Character.isLetterOrDigit(text.charAt(end));
        return startOk && endOk;
    }
}
