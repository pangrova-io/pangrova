package org.pangrova.parser.parse;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SqlDiagnosticsTest {
    @Test
    void hidesGenericAssignmentsIncludingMalformedInput() {
        assertThat(SqlDiagnostics.forReporting("/* private */ SET 'unknown.key' = 'sensitive'"))
                .isEqualTo("SET [redacted]");
        assertThat(SqlDiagnostics.forReporting("set token = 'unterminated")).isEqualTo("SET [redacted]");
    }

    @Test
    void hidesCatalogOptionsIncludingUnknownNamesCommentsAndInvalidStatements() {
        assertThat(SqlDiagnostics.forReporting("-- secret comment\n CrEaTe /* hidden */ CATALOG c WITH ('custom'='abc')"))
                .isEqualTo("CREATE CATALOG [redacted]");
        assertThat(SqlDiagnostics.forReporting("ALTER /* hidden */ cAtAlOg c SET PROPERTIES ('a'='unterminated"))
                .isEqualTo("ALTER CATALOG [redacted]");
        assertThat(SqlDiagnostics.forReporting("SELECT 'ALTER CATALOG'")).isEqualTo("SELECT 'ALTER CATALOG'");
    }
}
