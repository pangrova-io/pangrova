package org.pangrova.persistence.generation.postgresql.job;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PostgreSQL fencing between snapshot release and live execution retention. */
class SnapshotPinReleaseFenceIntegrationTest extends ContainerTestBase {
    private static final Instant NOW = Instant.parse("2026-11-25T00:00:00Z");
    private static final String TENANT = "tenant-a";
    private static final String INDEX = "artifact-a";
    private static final String JOB = "job-a";
    private static final String OWNER = INDEX + ":generation:0:attempt:submission";
    private static final String EXECUTION = "execution-a";

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        insertJob();
        var table = new QualifiedTableName("lake", "raw", "items");
        byte[] authority = DeferredAuthorizationScopeCodec.encode(
                DeferredAuthorizationScope.raw(table, Set.of("body"), table));
        try (var connection = dataSource.getConnection()) {
            execute(connection, "INSERT INTO index_descriptors (tenant_id,index_id,index_name,catalog_name,"
                    + "schema_name,table_name,column_names,index_type,properties,state,generation,"
                    + "build_snapshot_id,created_at,updated_at) VALUES (?,?,?,'lake','raw','items',"
                    + "'[\"body\"]'::jsonb,'INVERTED','{\"algorithm\":\"BM25\"}'::jsonb,'READY',0,"
                    + "'snapshot-a',?,?)", TENANT, INDEX, INDEX, NOW, NOW);
            execute(connection, "INSERT INTO execution_anchor (execution_id,owner_id,owner_incarnation,"
                    + "fencing_token,acquired_at,expires_at) VALUES (?,'coordinator-a','incarnation-a',1,?,?)",
                    EXECUTION, NOW, NOW.plus(Duration.ofHours(1)));
            execute(connection, "INSERT INTO artifact_snapshot_pins (tenant_id,pin_owner,index_id,generation,"
                    + "attempt_id,source_id,source_snapshot,principal,authorization_scope_bytes,state,job_id,"
                    + "created_at,updated_at,pinned_at) VALUES (?,?,?,0,'submission','lake.raw.items',"
                    + "'snapshot-a','alice',?,'PINNED',?,?,?,?)",
                    TENANT, OWNER, INDEX, authority, JOB, NOW, NOW, NOW);
        }
    }

    @Test
    void liveExecutionReferencePreventsReleaseUntilTheAnchorIsDeleted() throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            PostgresSnapshotPinLedger.retainForExecution(
                    connection, TENANT, INDEX, "0", "snapshot-a", EXECUTION);
            connection.commit();
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.beginRelease(connection, TENANT, OWNER, NOW)).isFalse();
            connection.rollback();
        }
        try (var connection = dataSource.getConnection()) {
            execute(connection, "DELETE FROM execution_anchor WHERE execution_id=?", EXECUTION);
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.beginRelease(connection, TENANT, OWNER, NOW)).isTrue();
            assertThat(PostgresSnapshotPinLedger.markReleased(connection, TENANT, OWNER, NOW)).isTrue();
            connection.commit();
        }
        assertThat(pinState()).isEqualTo("RELEASED");
    }

    @Test
    void maintenanceRetainsCurrentPublishedGenerationAndReleasesDroppedGeneration() throws Exception {
        Instant cutoff = NOW.plusSeconds(1);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.cleanupCandidates(connection, 10, cutoff, cutoff)).isEmpty();
            assertThat(PostgresSnapshotPinLedger.claimCleanup(
                    connection, TENANT, OWNER, cutoff, cutoff, cutoff)).isEmpty();
            connection.commit();
        }
        try (var connection = dataSource.getConnection()) {
            execute(connection, "UPDATE index_descriptors SET state='DROPPED' WHERE index_id=?", INDEX);
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.cleanupCandidates(connection, 10, cutoff, cutoff)).hasSize(1);
            assertThat(PostgresSnapshotPinLedger.claimCleanup(
                    connection, TENANT, OWNER, cutoff, cutoff, cutoff))
                    .hasValueSatisfying(intent -> assertThat(intent.state().name()).isEqualTo("RELEASING"));
            connection.commit();
        }
        // Connector outcome may be uncertain, so RELEASING must be recoverable.
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.claimCleanup(
                    connection, TENANT, OWNER, cutoff, cutoff, cutoff)).isPresent();
            assertThat(PostgresSnapshotPinLedger.markReleased(connection, TENANT, OWNER, cutoff)).isTrue();
            assertThat(PostgresSnapshotPinLedger.cleanupCandidates(connection, 10, cutoff, cutoff)).isEmpty();
            connection.commit();
        }
    }

    @Test
    void maintenanceRetainsReservedPinForAnUnfinishedGeneration() throws Exception {
        Instant cutoff = NOW.plusSeconds(1);
        try (var connection = dataSource.getConnection()) {
            execute(connection, "UPDATE artifact_snapshot_pins SET job_id=NULL WHERE tenant_id=? AND pin_owner=?",
                    TENANT, OWNER);
            execute(connection, "UPDATE index_descriptors SET state='BUILDING' WHERE index_id=?", INDEX);
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.cleanupCandidates(connection, 10, cutoff, cutoff)).isEmpty();
            assertThat(PostgresSnapshotPinLedger.claimCleanup(
                    connection, TENANT, OWNER, cutoff, cutoff, cutoff)).isEmpty();
            connection.commit();
        }
        assertThat(pinState()).isEqualTo("PINNED");
    }

    @Test
    void oneJobCannotBindMultipleSnapshotPins() {
        assertThatThrownBy(() -> {
            try (var connection = dataSource.getConnection()) {
                execute(connection, "INSERT INTO artifact_snapshot_pins (tenant_id,pin_owner,index_id,generation,"
                        + "attempt_id,source_id,source_snapshot,principal,authorization_scope_bytes,state,job_id,"
                        + "created_at,updated_at,pinned_at) VALUES (?,?,?,0,'other-attempt','lake.raw.items',"
                        + "'snapshot-b','alice',?,'PINNED',?,?,?,?)",
                        TENANT, OWNER + ":other", INDEX, new byte[] {1}, JOB, NOW, NOW, NOW);
            }
        }).isInstanceOf(java.sql.SQLException.class)
                .hasMessageContaining("uq_snapshot_pin_job");
    }

    @Test
    void version121WaitsForActiveNativeJobWriters() throws Exception {
        var source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("120").load().migrate();
        var executor = Executors.newSingleThreadExecutor();
        try (var writer = source.getConnection()) {
            writer.setAutoCommit(false);
            try (var statement = writer.createStatement()) {
                statement.execute("LOCK TABLE artifact_build_jobs IN ROW EXCLUSIVE MODE");
            }
            var upgraded = Flyway.configure().dataSource(source).locations("classpath:db/migration")
                    .target("121").load();
            var migration = executor.submit(() -> upgraded.migrate().migrationsExecuted);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                try (var connection = source.getConnection(); var statement = connection.createStatement();
                        var rows = statement.executeQuery("SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                                + "WHERE pid<>pg_backend_pid() AND wait_event_type='Lock')")) {
                    rows.next();
                    return rows.getBoolean(1);
                }
            });
            writer.commit();
            assertThat(migration.get(10, TimeUnit.SECONDS)).isOne();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void releaseFenceMakesALateConcurrentRetentionFailClosed() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var owner = dataSource.getConnection()) {
            owner.setAutoCommit(false);
            assertThat(PostgresSnapshotPinLedger.beginRelease(owner, TENANT, OWNER, NOW)).isTrue();
            var retention = executor.submit(() -> {
                retainOnNamedConnection("snapshot-retention");
                return null;
            });
            awaitBlocked("snapshot-retention");
            owner.commit();

            assertThatThrownBy(() -> retention.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Published artifact has no retainable live snapshot pin");
            assertThat(pinState()).isEqualTo("RELEASING");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private void insertJob() throws Exception {
        try (var connection = dataSource.getConnection()) {
            execute(connection, "INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,"
                    + "staging_generation,source_id,source_snapshot,staging_uri,format_id,format_major,"
                    + "format_minor,schema_sha256,writer_build_id,idempotency_key,expected_pages,state,"
                    + "created_at,updated_at,publication_expected_updated_at,dependency_set_sha256) "
                    + "VALUES (?,?,?,'TEXT_INDEX','0','lake.raw.items','snapshot-a',"
                    + "'s3://warehouse/staging','pangrova.bm25.segment',1,0,?,'writer-a',?,0,'PUBLISHED',"
                    + "?,?,?,?)", TENANT, JOB, INDEX, "a".repeat(64), JOB, NOW, NOW, NOW, "b".repeat(64));
        }
    }

    private void retainOnNamedConnection(String name) throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            execute(connection, "SELECT set_config('application_name',?,false)", name);
            execute(connection, "SET LOCAL statement_timeout='10s'");
            PostgresSnapshotPinLedger.retainForExecution(
                    connection, TENANT, INDEX, "0", "snapshot-a", EXECUTION);
            connection.commit();
        }
    }

    private void awaitBlocked(String application) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
            try (var connection = dataSource.getConnection();
                    var statement = connection.prepareStatement(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                                    + "WHERE application_name=? AND wait_event_type='Lock')")) {
                statement.setString(1, application);
                try (var rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getBoolean(1);
                }
            }
        });
    }

    private String pinState() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT state FROM artifact_snapshot_pins WHERE tenant_id=? AND pin_owner=?")) {
            statement.setString(1, TENANT);
            statement.setString(2, OWNER);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getString(1);
            }
        }
    }

    private static void execute(Connection connection, String sql, Object... parameters) throws Exception {
        try (var statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < parameters.length; index++) {
                Object parameter = parameters[index];
                if (parameter instanceof Instant instant) {
                    statement.setTimestamp(index + 1, java.sql.Timestamp.from(instant));
                } else {
                    statement.setObject(index + 1, parameter);
                }
            }
            statement.execute();
        }
    }
}
