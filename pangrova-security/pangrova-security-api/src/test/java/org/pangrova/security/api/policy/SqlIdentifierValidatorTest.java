package org.pangrova.security.api.policy;

import org.pangrova.security.api.policy.SqlIdentifierValidator;

import org.pangrova.security.api.exception.AccessDeniedException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.9 — SqlIdentifierValidator rejects DDL keywords and invalid identifiers.
 */
class SqlIdentifierValidatorTest {

    @Test
    void acceptsValidIdentifier() {
        assertThat(SqlIdentifierValidator.isValidIdentifier("my_table")).isTrue();
        assertThat(SqlIdentifierValidator.isValidIdentifier("_private")).isTrue();
        assertThat(SqlIdentifierValidator.isValidIdentifier("Column1")).isTrue();
    }

    @Test
    void rejectsInvalidIdentifier() {
        assertThat(SqlIdentifierValidator.isValidIdentifier(null)).isFalse();
        assertThat(SqlIdentifierValidator.isValidIdentifier("")).isFalse();
        assertThat(SqlIdentifierValidator.isValidIdentifier("1starts_with_digit")).isFalse();
        assertThat(SqlIdentifierValidator.isValidIdentifier("has space")).isFalse();
        assertThat(SqlIdentifierValidator.isValidIdentifier("has-dash")).isFalse();
    }

    @Test
    void validateIdentifierThrowsOnInvalid() {
        assertThatThrownBy(() -> SqlIdentifierValidator.validateIdentifier("1bad"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Invalid SQL identifier");
    }

    @Test
    void detectsDdlKeywords() {
        assertThat(SqlIdentifierValidator.containsDdlKeyword("DROP TABLE users")).isTrue();
        assertThat(SqlIdentifierValidator.containsDdlKeyword("ALTER TABLE users")).isTrue();
        assertThat(SqlIdentifierValidator.containsDdlKeyword("CREATE INDEX")).isTrue();
        assertThat(SqlIdentifierValidator.containsDdlKeyword("TRUNCATE TABLE")).isTrue();
    }

    @Test
    void doesNotFalsePositiveOnSubstrings() {
        // "BACKDROP" contains "DROP" but not as a word boundary
        assertThat(SqlIdentifierValidator.containsDdlKeyword("BACKDROP")).isFalse();
    }

    @Test
    void checksLaterKeywordOccurrencesAfterIdentifierFragments() {
        for (String keyword : java.util.List.of("DROP", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE")) {
            assertThat(SqlIdentifierValidator.containsDdlKeyword("prefix" + keyword + "; " + keyword))
                    .as("standalone %s after a non-keyword occurrence", keyword).isTrue();
            assertThatThrownBy(() -> SqlIdentifierValidator.rejectDdlInExpression(
                    keyword + "suffix; " + keyword))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    @Test
    void identifierSeparatorsDoNotMakeEmbeddedKeywordsStandalone() {
        for (String expression : java.util.List.of("drop_count > 0", "can_drop = true", "drop$flag = 1")) {
            assertThat(SqlIdentifierValidator.containsDdlKeyword(expression)).as(expression).isFalse();
        }
        assertThat(SqlIdentifierValidator.containsDdlKeyword("can_drop; DROP TABLE data")).isTrue();
    }

    @Test
    @org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.LOCALE)
    void keywordMatchingIsIndependentOfDefaultLocale() {
        java.util.Locale previous = java.util.Locale.getDefault();
        try {
            java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"));
            assertThat(SqlIdentifierValidator.containsDdlKeyword("backdrop; drop table data")).isTrue();
            assertThat(SqlIdentifierValidator.containsDdlKeyword("drop_count > 0")).isFalse();
        } finally {
            java.util.Locale.setDefault(previous);
        }
    }

    @Test
    void lexicalPrecheckRemainsConservativeForQuotedKeywords() {
        assertThatThrownBy(() -> SqlIdentifierValidator.rejectDdlInExpression("kind = 'DROP'"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsDdlInExpression() {
        assertThatThrownBy(() -> SqlIdentifierValidator.rejectDdlInExpression("DROP TABLE users"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("DDL keywords not allowed");
    }

    @Test
    void allowsSafeExpressions() {
        // Should not throw
        SqlIdentifierValidator.rejectDdlInExpression("department = 'engineering'");
        SqlIdentifierValidator.rejectDdlInExpression(null);
    }

    @Test
    void containsDdlKeywordReturnsFalseForNull() {
        assertThat(SqlIdentifierValidator.containsDdlKeyword(null)).isFalse();
    }
}
