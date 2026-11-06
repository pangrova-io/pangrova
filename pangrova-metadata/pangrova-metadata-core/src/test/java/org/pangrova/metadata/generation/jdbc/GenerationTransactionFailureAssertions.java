package org.pangrova.metadata.generation.jdbc;

import org.junit.jupiter.params.provider.Arguments;
import org.pangrova.persistence.generation.api.GenerationStorageException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Shared assertions for failures after real Generation SQL and before commit. */
final class GenerationTransactionFailureAssertions {
    private GenerationTransactionFailureAssertions() {}

    static Stream<Arguments> failures() {
        return Stream.of(Arguments.of("sql", "sql"), Arguments.of("runtime", "sql"),
                Arguments.of("error", "sql"), Arguments.of("runtime", "runtime"),
                Arguments.of("error", "error"), Arguments.of("error", "same"),
                Arguments.of("error", "none"));
    }

    static Throwable failure(String kind) {
        return switch (kind) {
            case "sql" -> new SQLException("injected Generation SQL failure");
            case "runtime" -> new IllegalStateException("injected Generation runtime failure");
            case "error" -> new AssertionError("injected Generation error");
            default -> throw new IllegalArgumentException(kind);
        };
    }

    static Throwable rollback(String kind, Throwable primary) {
        return kind.equals("same") ? primary : kind.equals("none") ? null : failure(kind);
    }

    static void assertFailure(Throwable actual, Throwable primary, Throwable rollback) {
        if (primary instanceof SQLException) {
            assertThat(actual).isInstanceOf(GenerationStorageException.class).hasCauseReference(primary);
        } else {
            assertThat(actual).isSameAs(primary);
        }
        if (rollback == null || rollback == primary) assertThat(primary.getSuppressed()).isEmpty();
        else assertThat(primary.getSuppressed()).containsExactly(rollback);
    }

    static List<String> snapshot(DataSource source) throws SQLException {
        var facts = new ArrayList<String>();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            var tables = new ArrayList<String>();
            try (var rows = statement.executeQuery("SELECT tablename FROM pg_tables "
                    + "WHERE schemaname=current_schema() AND tablename LIKE 'artifact_%' ORDER BY tablename")) {
                while (rows.next()) tables.add(rows.getString(1));
            }
            assertThat(tables).contains("artifact_build_jobs", "artifact_build_pages",
                    "artifact_exchange_registrations", "artifact_exchange_finalizations");
            for (String table : tables) {
                // Names originate from our isolated migrated schema, never user SQL.
                assertThat(table).matches("[a-z_]+");
                try (var rows = statement.executeQuery("SELECT row_to_json(t)::text FROM \"" + table
                        + "\" t ORDER BY row_to_json(t)::text")) {
                    while (rows.next()) facts.add(table + ":" + rows.getString(1));
                }
            }
        }
        return List.copyOf(facts);
    }
}
