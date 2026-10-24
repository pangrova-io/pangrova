package org.pangrova.coordinator.query.task.write;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Durability and idempotency of the JDBC write-commit quarantine store. */
class JdbcWriteCommitQuarantineStoreTest {

    private static final String FINGERPRINT = "a".repeat(64);

    @Test
    void recordedDecisionIsVisibleToOtherCoordinators() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_quarantine");
        Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        QuarantinedWriteCommit decision = new QuarantinedWriteCommit(
                "q1", FINGERPRINT, "iceberg", "analytics.events",
                "reconciler could not prove the commit outcome", at);

        new JdbcWriteCommitQuarantineStore(dataSource).record(decision);

        var reopened = new JdbcWriteCommitQuarantineStore(dataSource);
        assertThat(reopened.exists("q1", FINGERPRINT)).isTrue();
        assertThat(reopened.find("q1")).get()
                .satisfies(found -> {
                    assertThat(found.catalogName()).isEqualTo("iceberg");
                    assertThat(found.tableName()).isEqualTo("analytics.events");
                    assertThat(found.reason())
                            .isEqualTo("reconciler could not prove the commit outcome");
                    assertThat(found.quarantinedAt()).isEqualTo(at);
                });
    }

    @Test
    void repeatingTheSameDecisionIsIdempotent() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_quarantine_repeat");
        var store = new JdbcWriteCommitQuarantineStore(dataSource);
        Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        store.record(new QuarantinedWriteCommit(
                "q1", FINGERPRINT, "iceberg", "analytics.events", "first", at));
        store.record(new QuarantinedWriteCommit(
                "q1", FINGERPRINT, "iceberg", "analytics.events", "second", at.plusSeconds(60)));

        assertThat(store.find("q1")).get()
                .satisfies(found -> {
                    assertThat(found.reason()).isEqualTo("first");
                    assertThat(found.quarantinedAt()).isEqualTo(at);
                });
        assertThat(countRows(dataSource)).isEqualTo(1);
    }

    @Test
    void aDifferentFingerprintDoesNotMatchTheQuarantineDecision() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_quarantine_fingerprint");
        var store = new JdbcWriteCommitQuarantineStore(dataSource);
        store.record(new QuarantinedWriteCommit(
                "q1", FINGERPRINT, "iceberg", "analytics.events",
                "ambiguous", Instant.now()));

        assertThat(store.exists("q1", "b".repeat(64))).isFalse();
        assertThat(store.exists("q2", FINGERPRINT)).isFalse();
    }

    private static int countRows(javax.sql.DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT count(*) FROM write_commit_quarantine")) {
            rows.next();
            return rows.getInt(1);
        }
    }
}
