package org.pangrova.coordinator.coordination.store.jdbc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions;
import org.pangrova.persistence.file.postgresql.copy.JdbcCopySourceReadAdmissions;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.pangrova.coordinator.coordination.store.jdbc.JdbcCopySourceReadAdmissionsTest.fixture;
import static org.pangrova.coordinator.coordination.store.jdbc.JdbcCopySourceReadAdmissionsTest.inputAttempt;
import static org.pangrova.coordinator.coordination.store.jdbc.JdbcCopySourceReadAdmissionsTest.stageRoot;

/** Real admission authority, current permissions, execution fences and PostgreSQL mutations. */
class CopyAdmissionTransactionFailureTest {
    static Stream<Arguments> failures() {
        return Stream.of("issue", "frozen", "redeem", "record").flatMap(operation -> Stream.of(
                Arguments.of(operation, "sql", "sql"), Arguments.of(operation, "runtime", "sql"),
                Arguments.of(operation, "error", "sql"), Arguments.of(operation, "runtime", "runtime"),
                Arguments.of(operation, "error", "error"), Arguments.of(operation, "error", "same"),
                Arguments.of(operation, "error", "none")));
    }

    @ParameterizedTest(name = "{0}: primary={1}, rollback={2}")
    @MethodSource("failures")
    void failedAdmissionMutationPreservesOriginalFailureAndDurableState(
            String operation, String primaryKind, String rollbackKind) throws Exception {
        var fixture = fixture(MigratedPostgres.create());
        var identity = new UserIdentity("alice", Set.of("team-a"), Map.of("region", "west"),
                Instant.parse("2026-10-16T00:00:00Z"));
        Consumer<CopySourceReadAdmissions> action;
        String sql;
        switch (operation) {
            case "issue" -> {
                action = admissions -> admissions.issue(fixture.scope(), identity, fixture.anchor());
                sql = "INSERT INTO copy_source_read_admissions";
            }
            case "frozen" -> {
                stageRoot(fixture);
                var attempt = inputAttempt(fixture, "leaf-one");
                action = admissions -> admissions.issueFrozenInput(attempt, fixture.anchor());
                sql = "INSERT INTO copy_source_read_admissions";
            }
            case "redeem" -> {
                var ticket = fixture.issue(fixture.scope(), identity);
                action = admissions -> admissions.redeem(ticket, fixture.scope(), fixture.anchor());
                sql = "UPDATE copy_source_read_admissions SET redeemed_at";
            }
            case "record" -> {
                var ticket = fixture.issue(fixture.scope(), identity);
                fixture.redeem(ticket, fixture.scope());
                String tenant = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(("copy\0" + fixture.scope().tenantId()).getBytes(StandardCharsets.UTF_8)));
                var proof = new CopySourceReadAdmissions.StagedSource("version-one",
                        fixture.scope().managedStorageRootUri() + "/tenants/" + tenant
                                + "/sha256/bb/" + "b".repeat(64), "b".repeat(64), 123, tenant);
                action = admissions -> admissions.recordStagedSource(ticket, fixture.scope(), proof, fixture.anchor());
                sql = "UPDATE copy_source_read_admissions\nSET staged_source_version_id";
            }
            default -> throw new IllegalArgumentException(operation);
        }
        var before = snapshot(fixture.source());
        Throwable primary = failure(primaryKind);
        Throwable rollback = rollbackKind.equals("same") ? primary
                : rollbackKind.equals("none") ? null : failure(rollbackKind);
        DataSource faulty = JdbcMutationFault.afterMutation(fixture.source(), sql, primary, rollback);
        var admissions = new JdbcCopySourceReadAdmissions(faulty, fixture.accessControl(),
                new JdbcExecutionTransactionFence());
        Throwable actual = catchThrowable(() -> action.accept(admissions));
        if (primary instanceof SQLException) assertThat(actual).hasCauseReference(primary);
        else assertThat(actual).isSameAs(primary);
        if (rollback == null || rollback == primary) assertThat(primary.getSuppressed()).isEmpty();
        else assertThat(primary.getSuppressed()).containsExactly(rollback);
        assertThat(snapshot(fixture.source())).isEqualTo(before);
        action.accept(fixture.admissions());
        assertThat(snapshot(fixture.source())).isNotEqualTo(before);
    }

    private static Throwable failure(String kind) {
        return switch (kind) {
            case "sql" -> new SQLException("injected SQL failure");
            case "runtime" -> new IllegalStateException("injected runtime failure");
            case "error" -> new AssertionError("injected error");
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private static List<String> snapshot(DataSource source) throws SQLException {
        var facts = new ArrayList<String>();
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT row_to_json(t)::text "
                        + "FROM copy_source_read_admissions t ORDER BY nonce_sha256")) {
            while (rows.next()) facts.add(rows.getString(1));
        }
        return List.copyOf(facts);
    }
}
