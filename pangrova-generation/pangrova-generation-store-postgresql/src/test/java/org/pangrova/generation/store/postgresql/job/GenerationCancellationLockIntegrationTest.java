package org.pangrova.generation.store.postgresql.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJobKey;
import org.pangrova.generation.spi.jdbc.GenerationPublicationCandidates;
import org.pangrova.generation.spi.jdbc.GenerationTransactionAuthority;
import org.pangrova.generation.spi.jdbc.GenerationExecutionTransactionFence;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PostgreSQL cancellation/row-lock contracts; no executable task or Native completion is simulated. */
class GenerationCancellationLockIntegrationTest extends ContainerTestBase {
    private static final Instant NOW = Instant.parse("2026-10-03T00:00:00Z");
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        insertJob("tenant-a", "job-a");
    }

    @Test
    void cancellationWaitsForJobBeforeTakingAnyPageLock() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var owner = dataSource.getConnection()) {
            owner.setAutoCommit(false);
            execute(owner, "SELECT job_id FROM artifact_build_jobs WHERE job_id='job-a' FOR UPDATE");
            var cancellation = executor.submit(() -> cancelOnNamedConnection("cancel-job-first"));
            awaitBlocked("cancel-job-first");
            // Completion owns job -> page. A pages-first cancellation would already hold this page.
            execute(owner, "SELECT page_id FROM artifact_build_pages WHERE job_id='job-a' FOR UPDATE NOWAIT");
            owner.commit();
            assertThat(cancellation.get(10, TimeUnit.SECONDS)).isOne();
            assertStates("tenant-a", "job-a", "CANCELLED", "CANCELLED");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void sameSelectedJobSetIsUsedAfterWaitingForPagesAndConcurrentGenerationInsertion() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var pageOwner = dataSource.getConnection()) {
            pageOwner.setAutoCommit(false);
            execute(pageOwner, "SELECT page_id FROM artifact_build_pages WHERE job_id='job-a' FOR UPDATE");
            var cancellation = executor.submit(() -> cancelOnNamedConnection("cancel-frozen-set"));
            awaitBlocked("cancel-frozen-set");
            insertJob("tenant-a", "job-new");
            insertJob("tenant-b", "job-other-tenant");
            pageOwner.commit();
            assertThat(cancellation.get(10, TimeUnit.SECONDS)).isOne();
            assertStates("tenant-a", "job-a", "CANCELLED", "CANCELLED");
            assertStates("tenant-a", "job-new", "QUEUED", "PENDING");
            assertStates("tenant-b", "job-other-tenant", "QUEUED", "PENDING");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void pageUpdateFailureRollsBackJobCancellationAndAllowsExactRetry() throws Exception {
        try (var connection = dataSource.getConnection()) {
            execute(connection, "CREATE FUNCTION reject_cancelled_page() RETURNS trigger LANGUAGE plpgsql "
                    + "AS $$ BEGIN RAISE EXCEPTION 'actual page cancellation failure'; END $$");
            execute(connection, "CREATE TRIGGER reject_cancelled_page BEFORE UPDATE ON artifact_build_pages "
                    + "FOR EACH ROW WHEN (NEW.state='CANCELLED') EXECUTE FUNCTION reject_cancelled_page()");
        }
        var store = cancellationStore();
        assertThatThrownBy(() -> store.cancelArtifactBuilds("tenant-a", "artifact-a", NOW))
                .isInstanceOf(org.pangrova.generation.api.GenerationStorageException.class);
        assertStates("tenant-a", "job-a", "QUEUED", "PENDING");
        try (var connection = dataSource.getConnection()) {
            execute(connection, "DROP TRIGGER reject_cancelled_page ON artifact_build_pages");
        }
        assertThat(store.cancelArtifactBuilds("tenant-a", "artifact-a", NOW)).isOne();
        assertThat(store.cancelArtifactBuilds("tenant-a", "artifact-a", NOW)).isZero();
        assertStates("tenant-a", "job-a", "CANCELLED", "CANCELLED");
    }

    /** Cancellation must not re-admit revoked work or consult execution/publication eligibility. */
    private PostgresGenerationJobStore cancellationStore() {
        var authority = new GenerationTransactionAuthority() {
            @Override
            public GenerationDispatchSpec requireCurrent(Connection connection, String tenant, String job) {
                throw unexpectedDependency("persisted authority");
            }

            @Override
            public void requireCurrent(Connection connection, GenerationDispatchSpec spec) {
                throw unexpectedDependency("submission authority");
            }
        };
        var fence = new GenerationExecutionTransactionFence() {
            @Override
            public ExecutionAnchor requireCurrent(Connection connection, String tenant, String job,
                    GenerationExecutionOwner expected) {
                throw unexpectedDependency("current execution fence");
            }

            @Override
            public void requireAnchor(Connection connection, String tenant, String job, ExecutionAnchor anchor) {
                throw unexpectedDependency("execution anchor fence");
            }

            @Override
            public void requirePublication(Connection connection, String tenant, String job) {
                throw unexpectedDependency("publication execution fence");
            }
        };
        var candidates = new GenerationPublicationCandidates() {
            @Override
            public List<GenerationJobKey> listReady(Connection connection, int limit, Instant now) {
                throw unexpectedDependency("publication candidates");
            }

            @Override
            public boolean hasLiveOwner(Connection connection, String tenant, String job) {
                throw unexpectedDependency("publication owner");
            }
        };
        return new PostgresGenerationJobStore(dataSource, authority, fence, (connection, principal) -> {
            throw unexpectedDependency("current direct roles");
        }, candidates);
    }

    private static AssertionError unexpectedDependency(String dependency) {
        return new AssertionError("Cancellation unexpectedly consulted " + dependency);
    }

    private int cancelOnNamedConnection(String name) throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("SELECT set_config('application_name',?,false)")) {
                statement.setString(1, name);
                statement.execute();
            }
            execute(connection, "SET LOCAL statement_timeout='10s'");
            int count = PostgresGenerationRevocation.cancelArtifact(connection, "tenant-a", "artifact-a", NOW);
            connection.commit();
            return count;
        }
    }

    private void awaitBlocked(String application) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
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

    private void insertJob(String tenant, String job) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO artifact_build_jobs (tenant_id,job_id,artifact_id,artifact_type,staging_generation,"
                        + "source_id,source_snapshot,staging_uri,format_id,format_major,format_minor,schema_sha256,"
                        + "writer_build_id,idempotency_key,expected_pages,state,created_at,updated_at,"
                        + "publication_expected_updated_at,dependency_set_sha256) "
                        + "VALUES (?,?,'artifact-a','VECTOR_INDEX',?,'lake.raw.items','snapshot-a',"
                        + "'s3://warehouse/staging','pangrova.diskann.segment',1,0,?,'writer-a',?,"
                        + "1,'QUEUED',?,?,?,?)")) {
            statement.setString(1, tenant);
            statement.setString(2, job);
            statement.setString(3, job);
            statement.setString(4, "a".repeat(64));
            statement.setString(5, job);
            for (int index = 6; index <= 8; index++) {
                statement.setObject(index, NOW.atOffset(java.time.ZoneOffset.UTC));
            }
            statement.setString(9, "b".repeat(64));
            statement.executeUpdate();
            insertPage(connection, tenant, job);
        }
    }

    private static void insertPage(Connection connection, String tenant, String job) throws Exception {
        try (var statement = connection.prepareStatement("INSERT INTO artifact_build_pages "
                + "(tenant_id,job_id,page_id,stage_id,partition_id,page_ordinal,page_kind,state,"
                + "next_available_at,max_attempts,created_at,updated_at) "
                + "VALUES (?,?,'page-a',0,0,0,'SOURCE_MAP','PENDING',?,3,?,?)")) {
            statement.setString(1, tenant);
            statement.setString(2, job);
            for (int index = 3; index <= 5; index++) {
                statement.setObject(index, NOW.atOffset(java.time.ZoneOffset.UTC));
            }
            statement.executeUpdate();
        }
    }

    private void assertStates(String tenant, String job, String jobState, String pageState) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT j.state,p.state FROM artifact_build_jobs j JOIN artifact_build_pages p "
                        + "ON p.tenant_id=j.tenant_id AND p.job_id=j.job_id WHERE j.tenant_id=? AND j.job_id=?")) {
            statement.setString(1, tenant);
            statement.setString(2, job);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo(jobState);
                assertThat(rows.getString(2)).isEqualTo(pageState);
                assertThat(rows.next()).isFalse();
            }
        }
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) { statement.execute(sql); }
    }
}
