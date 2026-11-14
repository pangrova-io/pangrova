package org.pangrova.metadata.generation.jdbc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.assertFailure;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.failure;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.rollback;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.snapshot;

import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;

import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;

import org.pangrova.generation.api.exception.GenerationExecutionOwnerLostException;

import org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores;

import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.jdbc.api.generation.GenerationJobSubmissionTransactions;
import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresGenerationJobStoreTest {

    private org.pangrova.metadata.testinfra.GenerationExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-05T00:00:00Z");
    private static final String TENANT = "tenant-1";

    private PostgresGenerationJobStore store;
    private GenerationJobSubmissionTransactions submissions;
    private final GenerationTransactionInputs inputs = new PostgresGenerationQueries();
    private javax.sql.DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("artifact_build");
        Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration")
                .load().migrate();
        store = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        submissions = store;
        executions = new org.pangrova.metadata.testinfra.GenerationExecutionFixtures(dataSource);
        org.pangrova.metadata.testinfra.GenerationAuthorityFixtures.provision(
                dataSource, TENANT, "principal-" + TENANT, dispatchSpec(TENANT).authorizationScope());
        org.pangrova.metadata.testinfra.GenerationAuthorityFixtures.provision(
                dataSource, "tenant-2", "principal-tenant-2", dispatchSpec("tenant-2").authorizationScope());
    }

    @ParameterizedTest(name = "job insertion: primary={0}, rollback={1}")
    @MethodSource("org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions#failures")
    void failedJobInsertionPreservesWholeCohortAndPrimaryFailure(String kind, String rollbackKind) throws Exception {
        var pages = pages(1);
        var spec = dispatchSpec(TENANT, pages);
        var before = snapshot(dataSource);
        Throwable primary = failure(kind);
        Throwable rollback = rollback(rollbackKind, primary);
        var faulty = JdbcMutationFault.afterMutation(dataSource, "INSERT INTO artifact_build_pages ", primary, rollback);
        assertFailure(catchThrowable(() -> GenerationAuthorityFixtures.artifacts().builds(faulty)
                .createJob(job(1), spec, pages)), primary, rollback);
        assertThat(snapshot(dataSource)).isEqualTo(before);
        store.createJob(job(1), spec, pages);
        assertThat(store.findDispatchSpec(TENANT, "job-1")).contains(spec);
        try (var connection = dataSource.getConnection()) {
            assertThat(inputs.loadPages(connection, TENANT, "job-1")).isEqualTo(pages);
        }
    }

    @Test
    void driverErrorAfterJobInsertionCannotCommitPartialBuildWhenConnectionCloses() throws Exception {
        AssertionError driverFailure = new AssertionError("injected page insertion failure");
        javax.sql.DataSource failingSource = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{javax.sql.DataSource.class}, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(dataSource, args);
                        if (!(result instanceof java.sql.Connection connection)) return result;
                        return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[]{java.sql.Connection.class}, (connectionProxy, operation, parameters) -> {
                                    if (operation.getName().equals("prepareStatement")
                                            && ((String) parameters[0]).startsWith("INSERT INTO artifact_build_pages ")) {
                                        throw driverFailure;
                                    }
                                    try {
                                        return operation.invoke(connection, parameters);
                                    } catch (java.lang.reflect.InvocationTargetException failure) {
                                        throw failure.getCause();
                                    }
                                });
                    } catch (java.lang.reflect.InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
        assertThatThrownBy(() -> GenerationAuthorityFixtures.artifacts().builds(failingSource)
                .createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1))).isSameAs(driverFailure);
        assertThat(store.findJob(TENANT, "job-1")).isEmpty();
        assertThat(store.findDispatchSpec(TENANT, "job-1")).isEmpty();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            for (String table : List.of("artifact_build_jobs", "artifact_build_dispatch_specs", "artifact_build_pages",
                    "artifact_build_dependencies", "artifact_execution_routes", "artifact_stage_exchanges")) {
                try (var result = statement.executeQuery("SELECT count(*) FROM " + table)) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getLong(1)).as(table).isZero();
                }
            }
        }
    }

    @Test
    void oversizedRawContractCannotLeavePartialRowsEvenIfCallerCommits() throws Exception {
        var ordinary = dispatchSpec(TENANT, pages(1));
        var graph = org.pangrova.generation.submission.GenerationStageGraphFactory.forArtifactPages(
                new byte[12 * 1024 * 1024], new byte[]{1}, pages(1), List.of());
        var raw = new GenerationDispatchSpec(ordinary.jobId(), graph, ordinary.tenantId(), ordinary.principal(),
                ordinary.authorizationScope(), ordinary.maxMemoryBytes(), ordinary.maxThreads(),
                ordinary.maxSpillBytes(), ordinary.taskTimeoutMillis());
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> submissions.createJob(connection, job(1), raw, pages(1)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("size limit");
            connection.commit();
        }
        assertThat(store.findJob(TENANT, "job-1")).isEmpty();
        assertThat(store.findDispatchSpec(TENANT, "job-1")).isEmpty();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            for (String table : List.of("artifact_build_jobs", "artifact_build_dispatch_specs", "artifact_build_pages",
                    "artifact_build_dependencies", "artifact_execution_routes", "artifact_stage_exchanges")) {
                try (var result = statement.executeQuery("SELECT count(*) FROM " + table)) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getLong(1)).as(table).isZero();
                }
            }
        }
    }

    @Test
    void callerOwnedTransactionCommitsOrRollsBackTheSameGraphAndPageInsertion() throws Exception {
        var pages = pages(1);
        var spec = dispatchSpec(TENANT, pages);
        try (var connection = dataSource.getConnection()) {
            assertThatThrownBy(() -> submissions.createJob(connection, job(1), spec, pages))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("caller-owned transaction");
            connection.setAutoCommit(false);
            submissions.createJob(connection, job(1), spec, pages);
            // Shared authority/publication reads must see the caller's uncommitted cohort, without another connection.
            assertThat(inputs.findDispatchSpec(connection, TENANT, "job-1")).contains(spec);
            assertThat(inputs.loadPages(connection, TENANT, "job-1")).isEqualTo(pages);
            assertThat(inputs.loadJobDependencies(connection, TENANT, "job-1"))
                    .isEqualTo(job(1).dependencySet().dependencies());
            assertThat(inputs.findDispatchSpec(connection, "tenant-2", "job-1")).isEmpty();
            assertThat(inputs.loadPages(connection, "tenant-2", "job-1")).isEmpty();
            assertThat(store.findJob(TENANT, "job-1")).isEmpty();
            connection.rollback();
            assertThat(store.findDispatchSpec(TENANT, "job-1")).isEmpty();
            submissions.createJob(connection, job(1), spec, pages);
            connection.commit();
        }
        assertThat(store.findDispatchSpec(TENANT, "job-1")).contains(spec);
        assertThat(store.claimNextPage(TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 1), 1,
                Duration.ofSeconds(30), START)).hasValueSatisfying(lease ->
                        assertThat(lease.sourceObjectIds()).containsExactly("source-0"));
    }

    @Test
    void inputPortAllowsAutoCommitReadsAndRejectsMissingOrCorruptCanonicalSplits() throws Exception {
        var expectedPages = List.of(
                sourcePage("page-z", 0, "source-0"), sourcePage("page-a", 1, "source-1"));
        var spec = dispatchSpec(TENANT, expectedPages);
        // Admission normalizes insertion order; assert the API order without depending on a PG access plan.
        store.createJob(job(2), spec, List.of(expectedPages.get(1), expectedPages.get(0)));
        try (var connection = dataSource.getConnection()) {
            assertThat(connection.getAutoCommit()).isTrue();
            assertThat(inputs.findDispatchSpec(connection, TENANT, "job-1")).contains(spec);
            assertThat(inputs.loadJobDependencies(connection, TENANT, "job-1"))
                    .isEqualTo(job(2).dependencySet().dependencies());
            assertThat(inputs.loadPages(connection, TENANT, "job-1")).isEqualTo(expectedPages);
            assertThat(inputs.findDispatchSpec(connection, "other-tenant", "job-1")).isEmpty();
            assertThat(inputs.loadJobDependencies(connection, "other-tenant", "job-1")).isEmpty();
            assertThat(inputs.loadPages(connection, "other-tenant", "job-1")).isEmpty();
            try (var statement = connection.prepareStatement(
                    "UPDATE artifact_build_page_sources SET split_sha256=? "
                            + "WHERE tenant_id=? AND job_id=? AND page_id=?")) {
                statement.setString(1, "0".repeat(64));
                statement.setString(2, TENANT);
                statement.setString(3, "job-1");
                statement.setString(4, "page-z");
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            assertThatThrownBy(() -> inputs.loadPages(connection, TENANT, "job-1"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("SHA-256 differs");
            try (var statement = connection.prepareStatement(
                    "UPDATE artifact_build_page_sources SET split_bytes=NULL "
                            + "WHERE tenant_id=? AND job_id=? AND page_id=?")) {
                statement.setString(1, TENANT);
                statement.setString(2, "job-1");
                statement.setString(3, "page-z");
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            assertThatThrownBy(() -> inputs.loadPages(connection, TENANT, "job-1"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("predates replayable");
            assertThat(connection.getAutoCommit()).isTrue();
        }
    }

    @Test
    void persistsFencedPagesAndOpensOnlyAnExactAllPagesBarrier() {
        store.createJob(job(2), dispatchSpec(TENANT, pages(2)), pages(2));
        store.createJob(job(2), dispatchSpec(TENANT, pages(2)), pages(2));

        assertThat(store.listRunnableJobs(10, START))
                .containsExactly(new GenerationJobKey(TENANT, "job-1"));
        assertThat(store.findDispatchSpec(TENANT, "job-1")).contains(dispatchSpec(TENANT, pages(2)));

        var first = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-a", 7), 7, Duration.ofMinutes(1), START)
                .orElseThrow();
        assertThat(first.pageId()).isEqualTo("page-0");
        assertThat(first.attemptId()).isZero();
        assertThat(first.fencingToken()).isPositive();

        byte[] firstReport = "report-0".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(
                first.jobId(), first.pageId(), first.fencingToken(), first.ownerEpoch(),
                firstReport, sha256(firstReport), outputs(0), START.plusSeconds(1)))
                .isTrue();
        assertThat(store.findJob(TENANT, "job-1").orElseThrow())
                .satisfies(job -> {
                    assertThat(job.state()).isEqualTo(GenerationJobState.RUNNING);
                    assertThat(job.completedPages()).isOne();
                });
        assertThatThrownBy(() -> store.loadCompleteOutputs(TENANT, "job-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("all-pages barrier");

        var second = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-a", 7), 7,
                Duration.ofMinutes(1), START.plusSeconds(2))
                .orElseThrow();
        byte[] secondReport = "report-1".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(
                second.jobId(), second.pageId(), second.fencingToken(), second.ownerEpoch(),
                secondReport, sha256(secondReport), outputs(1), START.plusSeconds(3)))
                .isTrue();

        assertThat(store.findJob(TENANT, "job-1").orElseThrow())
                .satisfies(job -> {
                    assertThat(job.state()).isEqualTo(GenerationJobState.READY_TO_PUBLISH);
                    assertThat(job.completedPages()).isEqualTo(2);
                    assertThat(job.publicationExpectedUpdatedAt()).isEqualTo(START);
                });
        assertThat(store.listReadyForPublication(10, START.plusSeconds(3)))
                .containsExactly(new GenerationJobKey(TENANT, "job-1"));
        assertThat(store.loadCompleteOutputs(TENANT, "job-1"))
                .extracting(GenerationOutput::sourceObjectId)
                .containsOnly("source-0", "source-1");
    }

    @Test
    void expiredAttemptIsReclaimedAndStaleWorkerCannotCommit() {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var stale = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-a", 3), 3, Duration.ofSeconds(10), START)
                .orElseThrow();
        var current = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-b", 4), 4,
                Duration.ofSeconds(10), START.plusSeconds(11))
                .orElseThrow();

        assertThat(current.attemptId()).isEqualTo(1);
        assertThat(current.fencingToken()).isGreaterThan(stale.fencingToken());
        byte[] report = "current-report".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(
                stale.jobId(), stale.pageId(), stale.fencingToken(), stale.ownerEpoch(),
                report, sha256(report), outputs(0), START.plusSeconds(12)))
                .isFalse();
        assertThat(completePage(
                current.jobId(), current.pageId(), current.fencingToken(), current.ownerEpoch(),
                report, sha256(report), outputs(0), START.plusSeconds(12)))
                .isTrue();
        assertThat(completePage(
                current.jobId(), current.pageId(), current.fencingToken(), current.ownerEpoch(),
                report, sha256(report), outputs(0), START.plusSeconds(13)))
                .isTrue();
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().completedPages()).isOne();
    }

    @Test
    void processRestartUsesTheSameDurableRouteAndRejectsOldNativeOwnerEvenAtTheSameControlTerm() {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var key = new GenerationJobKey(TENANT, "job-1");
        var anchors = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        var first = anchors.tryAcquire(key.executionId(), "coordinator", "process-before-restart", Duration.ofMinutes(1)).orElseThrow();
        var oldPage = store.claimNextPage(TENANT, key.jobId(), first, 1, Duration.ofSeconds(10), START).orElseThrow();
        var restarted = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        assertThat(restarted.findExecutionJob(key.executionId())).contains(key);
        var recoveredAnchors = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        assertThat(recoveredAnchors.current(key.executionId())).contains(first);
        assertThat(recoveredAnchors.tryAcquire(key.executionId(), "coordinator", "process-after-restart",
                Duration.ofMinutes(1))).isEmpty();
        assertThat(anchors.yield(first)).isTrue();
        var replacement = recoveredAnchors.tryAcquire(key.executionId(), "coordinator", "process-after-restart",
                Duration.ofMinutes(1)).orElseThrow();
        assertThat(replacement.fencingToken()).isGreaterThan(first.fencingToken());
        assertThatThrownBy(() -> restarted.renewPage(oldPage, Duration.ofSeconds(10), START.plusSeconds(1)))
                .isInstanceOf(GenerationExecutionOwnerLostException.class).hasMessageContaining("owner fence was lost");
        var current = restarted.claimNextPage(TENANT, key.jobId(), replacement, 1,
                Duration.ofSeconds(30), START.plusSeconds(11)).orElseThrow();
        byte[] report = new byte[] {1, 2, 3};
        // Copying the new page fence does not upgrade old immutable process evidence.
        assertThat(restarted.completePage(new GenerationPageCompletion(TENANT, key.jobId(), current.pageId(),
                current.fencingToken(), current.ownerEpoch(), report, sha256(report), outputs(0), START.plusSeconds(12),
                org.pangrova.generation.api.execution.GenerationExecutionOwner.from(first)))).isFalse();
        assertThat(restarted.completePage(new GenerationPageCompletion(TENANT, key.jobId(), current.pageId(),
                current.fencingToken(), current.ownerEpoch(), report, sha256(report), outputs(0), START.plusSeconds(12),
                org.pangrova.generation.api.execution.GenerationExecutionOwner.from(replacement)))).isTrue();
        var publication = restarted.claimReadyForPublication(TENANT, key.jobId(), replacement, 1,
                Duration.ofSeconds(30), START.plusSeconds(13)).orElseThrow();
        assertThat(recoveredAnchors.yield(replacement)).isTrue();
        var third = recoveredAnchors.tryAcquire(key.executionId(), "coordinator", "third-process",
                Duration.ofMinutes(1)).orElseThrow();
        assertThatThrownBy(() -> restarted.markPublished(TENANT, key.jobId(), replacement.ownerId(), 1,
                publication.publicationFencingToken(), START.plusSeconds(14)))
                .isInstanceOf(GenerationExecutionOwnerLostException.class).hasMessageContaining("owner fence was lost");
        var replacementPublication = restarted.claimReadyForPublication(TENANT, key.jobId(), third, 2,
                Duration.ofSeconds(30), START.plusSeconds(44)).orElseThrow();
        assertThat(restarted.markPublished(TENANT, key.jobId(), third.ownerId(), 2,
                replacementPublication.publicationFencingToken(), START.plusSeconds(45))).isTrue();
    }

    @Test
    void exactRoutingSeparatesEqualJobNamesAndRejectsAnotherTenantsAnchor() {
        store.createJob(job(TENANT, 1), dispatchSpec(TENANT, pages(1)), pages(1));
        store.createJob(job("tenant-2", 1), dispatchSpec("tenant-2", pages(1)), pages(1));
        var first = new GenerationJobKey(TENANT, "job-1");
        var second = new GenerationJobKey("tenant-2", "job-1");
        assertThat(first.executionId()).isNotEqualTo(second.executionId());
        assertThat(store.findExecutionJob(first.executionId())).contains(first);
        assertThat(store.findExecutionJob(second.executionId())).contains(second);
        var anchor = executions.takeOwnership(TENANT, first.jobId(), "coordinator", 1);
        assertThatThrownBy(() -> store.claimNextPage("tenant-2", second.jobId(), anchor, 1,
                Duration.ofMinutes(1), START)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another artifact job");
        assertThat(store.findJob("tenant-2", second.jobId()).orElseThrow().state()).isEqualTo(GenerationJobState.QUEUED);
    }

    @Test
    void ownerGateHoldsTheSameBackendRowAgainstTakeoverUntilTheMetadataTransactionEnds() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var anchor = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        var anchors = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        var executor = Executors.newSingleThreadExecutor();
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            GenerationAuthorityFixtures.artifacts().authority().requireCurrent(connection, TENANT, "job-1");
            org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores.executionFence().requireAnchor(connection, TENANT, "job-1", anchor);
            var entered = new java.util.concurrent.CountDownLatch(1);
            var takeover = executor.submit(() -> {
                entered.countDown();
                return anchors.yield(anchor);
            });
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> takeover.get(150, java.util.concurrent.TimeUnit.MILLISECONDS))
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);
            connection.commit();
            assertThat(takeover.get(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var next = anchors.tryAcquire(anchor.executionId(), "other-coordinator", "replacement", Duration.ofMinutes(1)).orElseThrow();
            assertThat(next.fencingToken()).isGreaterThan(anchor.fencingToken());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void publicationLeaseFencesDemotedCoordinatorAndAllowsImmediateTakeover() {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var page = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-a", 2), 2, Duration.ofMinutes(1), START)
                .orElseThrow();
        byte[] report = "report".getBytes(StandardCharsets.UTF_8);
        completePage(
                page.jobId(), page.pageId(), page.fencingToken(), page.ownerEpoch(),
                report, sha256(report), outputs(0), START.plusSeconds(1));

        var stale = store.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-a", 2), 2,
                Duration.ofSeconds(10), START.plusSeconds(2))
                .orElseThrow();
        var current = store.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator-b", 3), 3,
                Duration.ofSeconds(10), START.plusSeconds(3))
                .orElseThrow();

        assertThat(store.markPublished(
                TENANT, "job-1", "coordinator-a", 2,
                stale.publicationFencingToken(), START.plusSeconds(4))).isFalse();
        assertThat(store.markPublished(
                TENANT, "job-1", "coordinator-b", 3,
                current.publicationFencingToken(), START.plusSeconds(4))).isTrue();
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
    }

    @Test
    void concurrentClaimProducesOnlyOneLiveLease() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var executor = Executors.newFixedThreadPool(2);
        try {
            Callable<Boolean> claim = () -> store.claimNextPage(
                    TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 5), 5,
                    Duration.ofMinutes(1), START).isPresent();
            var results = executor.invokeAll(List.of(claim, claim));
            assertThat(results.get(0).get() ? 1 : 0)
                    .isEqualTo(results.get(1).get() ? 0 : 1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void cancellingArtifactFencesAllOutstandingPages() {
        store.createJob(job(2), dispatchSpec(TENANT, pages(2)), pages(2));
        assertThat(store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 2), 2, Duration.ofMinutes(1), START))
                .isPresent();

        assertThat(store.cancelArtifactBuilds(TENANT, "index-1", START.plusSeconds(1)))
                .isOne();
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.CANCELLED);
        assertThat(store.listRunnableJobs(10, START.plusSeconds(2))).isEmpty();
        assertThat(store.listReadyForPublication(10, START.plusSeconds(2))).isEmpty();
    }

    @Test
    void sameLogicalIdentityIsIsolatedAcrossTenants() {
        String otherTenant = "tenant-2";
        store.createJob(job(TENANT, 1), dispatchSpec(TENANT, pages(1)), pages(1));
        store.createJob(job(otherTenant, 1), dispatchSpec(otherTenant, pages(1)), pages(1));

        assertThat(store.findJob("unknown-tenant", "job-1")).isEmpty();
        assertThat(store.listRunnableJobs(10, START))
                .containsExactlyInAnyOrder(
                        new GenerationJobKey(TENANT, "job-1"),
                        new GenerationJobKey(otherTenant, "job-1"));

        var otherLease = store.claimNextPage(
                otherTenant, "job-1", executions.takeOwnership(otherTenant, "job-1", "coordinator-b", 8), 8,
                Duration.ofMinutes(1), START).orElseThrow();
        byte[] report = "tenant-2-report".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(
                otherTenant, otherLease.jobId(), otherLease.pageId(),
                otherLease.fencingToken(), otherLease.ownerEpoch(), report,
                sha256(report), outputs(0), START.plusSeconds(1))).isTrue();

        assertThat(store.cancelArtifactBuilds(
                TENANT, "index-1", START.plusSeconds(2))).isOne();
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.CANCELLED);
        assertThat(store.findJob(otherTenant, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
        assertThat(store.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "publisher", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(3))).isEmpty();

        var otherClaim = store.claimReadyForPublication(
                otherTenant, "job-1", executions.takeOwnership(otherTenant, "job-1", "publisher", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(3)).orElseThrow();
        assertThat(store.markPublished(
                TENANT, "job-1", "publisher", 9,
                otherClaim.publicationFencingToken(), START.plusSeconds(4))).isFalse();
        assertThat(store.markPublished(
                otherTenant, "job-1", "publisher", 9,
                otherClaim.publicationFencingToken(), START.plusSeconds(4))).isTrue();
    }

    @Test
    void publishedLookupIsPartitionScopedAndMatchesCanonicalSplits() {
        store.createJob(job(2), dispatchSpec(TENANT, pages(2)), pages(2));
        for (int ordinal = 0; ordinal < 2; ordinal++) {
            var lease = store.claimNextPage(
                    TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 9), 9, Duration.ofMinutes(1),
                    START.plusSeconds(ordinal)).orElseThrow();
            byte[] report = ("report-" + ordinal).getBytes(StandardCharsets.UTF_8);
            assertThat(completePage(
                    lease.jobId(), lease.pageId(), lease.fencingToken(),
                    lease.ownerEpoch(), report, sha256(report),
                    outputs(ordinal), START.plusSeconds(ordinal + 1)))
                    .isTrue();
        }
        assertThatThrownBy(() -> store.loadPublishedOutputs(
                TENANT, "job-1", java.util.Set.of("source-1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not atomically published");

        var claim = store.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "publisher", 12), 12, Duration.ofMinutes(1),
                START.plusSeconds(10)).orElseThrow();
        assertThat(store.markPublished(
                TENANT, claim.jobId(), "publisher", 12,
                claim.publicationFencingToken(), START.plusSeconds(11)))
                .isTrue();

        assertThat(store.loadPublishedOutputs(
                TENANT, "job-1", java.util.Set.of("source-1")))
                .extracting(GenerationOutput::sourceObjectId)
                .containsOnly("source-1");
        assertThat(store.matchesPublishedSplits(TENANT, "job-1", java.util.Map.of(
                "source-0", sha256("canonical-split-0".getBytes(StandardCharsets.UTF_8)),
                "source-1", sha256("canonical-split-1".getBytes(StandardCharsets.UTF_8)))))
                .isTrue();
        assertThat(store.matchesPublishedSplits(TENANT, "job-1", java.util.Map.of(
                "source-1", "f".repeat(64))))
                .isFalse();
        assertThat(store.loadPublishedSourceSplits(TENANT, "job-1", java.util.Set.of("source-0")))
                .containsEntry("source-0", GenerationSplit.of("source-0",
                        "canonical-split-0".getBytes(StandardCharsets.UTF_8)));
        assertThat(store.loadPublishedSourceSplits(TENANT, "job-1", java.util.Set.of("missing")))
                .isEmpty();
    }

    @Test
    void releasesBoundedReductionDagOnlyAfterEveryDependencySucceeds() {
        List<GenerationPage> dag = bm25Pages();
        store.createJob(bm25Job(dag.size()), dispatchSpec(TENANT, dag), dag);

        var first = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1), START)
                .orElseThrow();
        assertThat(first.kind()).isEqualTo(GenerationPageKind.SOURCE_MAP);
        complete(first, bm25SourceOutputs(first.pageId(), "source-0"), 1);

        var second = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1),
                START.plusSeconds(2)).orElseThrow();
        assertThat(second.kind()).isEqualTo(GenerationPageKind.SOURCE_MAP);
        complete(second, bm25SourceOutputs(second.pageId(), "source-1"), 3);

        var reduce = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1),
                START.plusSeconds(4)).orElseThrow();
        assertThat(reduce.kind()).isEqualTo(GenerationPageKind.DEPENDENCY_REDUCE);
        assertThat(reduce.dependencyPageIds()).containsExactly("map-0", "map-1");
        assertThat(store.loadDependencyOutputs(
                TENANT, "job-1", reduce.dependencyPageIds()))
                .hasSize(6);
        complete(reduce, List.of(scopedOutput(
                reduce.pageId(), "BM25_STATS_SHARD", 0,
                GenerationOutputScope.WORK_UNIT, 20)), 5);

        var finalize = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1),
                START.plusSeconds(6)).orElseThrow();
        assertThat(finalize.kind())
                .isEqualTo(GenerationPageKind.GENERATION_FINALIZE);
        assertThat(finalize.dependencyPageIds()).containsExactly("reduce-0");
        complete(finalize, List.of(scopedOutput(
                "2", "BM25_CORPUS_STATS", 0,
                GenerationOutputScope.GENERATION, 20)), 7);

        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
    }

    @Test
    void rejectsChangedIdempotentCreateAndWrongCoverage() {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        GenerationJob changed = new GenerationJob(
                TENANT, "job-1", "artifact-other", "DISKANN", "2",
                "catalog.schema.items", "snapshot-1",
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/job-1", "pangrova.diskann", 1, 0,
                "a".repeat(64), "writer-1", "kms/key", "request-1",
                1, 0, GenerationJobState.QUEUED, 0, START, START, START);
        assertThatThrownBy(() -> store.createJob(changed, dispatchSpec(TENANT, pages(1)), pages(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different work");

        var lease = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 1), 1, Duration.ofMinutes(1), START)
                .orElseThrow();
        byte[] report = "report".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> completePage(
                lease.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), List.of(new GenerationOutput(
                        "object", "s3://bucket/staging/job-1/object", "DISKANN_GRAPH",
                        "wrong-source", 0, GenerationOutputScope.SOURCE_OBJECT,
                        64, 1, "b".repeat(64), "kms/key")),
                START.plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("work-page role contract");
    }

    private static GenerationJob job(int pages) {
        return job(TENANT, pages);
    }

    private static GenerationJob job(String tenantId, int pages) {
        return new GenerationJob(
                tenantId, "job-1", "index-1", GenerationOutputTypeIds.VECTOR_INDEX, "2",
                "catalog.schema.items", "snapshot-1",
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/job-1", "pangrova.diskann.segment", 1, 0,
                "a".repeat(64), "writer-1", "kms/key", "request-1",
                pages, 0, GenerationJobState.QUEUED, 0, START, START, START);
    }

    private static GenerationJob bm25Job(int pages) {
        return new GenerationJob(
                TENANT, "job-1", "index-1", GenerationOutputTypeIds.TEXT_INDEX, "2",
                "catalog.schema.items", "snapshot-1",
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/job-1", "pangrova.bm25.segment", 1, 0,
                "a".repeat(64), "writer-1", "kms/key", "request-1",
                pages, 0, GenerationJobState.QUEUED, 0, START, START, START);
    }

    private static List<GenerationPage> bm25Pages() {
        return List.of(
                sourcePage("map-0", 0, "source-0"),
                sourcePage("map-1", 1, "source-1"),
                new GenerationPage(
                        "job-1", "reduce-0", 11, 0, 2,
                        GenerationPageKind.DEPENDENCY_REDUCE,
                        List.of(), List.of("map-0", "map-1"), 3),
                new GenerationPage(
                        "job-1", "finalize-0", 12, 0, 3,
                        GenerationPageKind.GENERATION_FINALIZE,
                        List.of(), List.of("reduce-0"), 3));
    }

    private static GenerationPage sourcePage(
            String pageId, int ordinal, String source) {
        return new GenerationPage(
                "job-1", pageId, 10, ordinal, ordinal,
                GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(
                        source, ("canonical-" + source).getBytes(StandardCharsets.UTF_8))),
                List.of(), 3);
    }

    private static List<GenerationOutput> bm25SourceOutputs(
            String pageId, String source) {
        return List.of(
                scopedOutput(source, "BM25_POSTINGS", 0,
                        GenerationOutputScope.SOURCE_OBJECT, 10),
                scopedOutput(source, "ROW_LOCATORS", 1,
                        GenerationOutputScope.SOURCE_OBJECT, 10),
                scopedOutput(pageId, "BM25_STATS_SHARD", 2,
                        GenerationOutputScope.WORK_UNIT, 10));
    }

    private static GenerationOutput scopedOutput(
            String scopeId, String role, int ordinal,
            GenerationOutputScope scope, long rows) {
        return new GenerationOutput(
                scopeId + "-" + role,
                "s3://bucket/staging/job-1/" + scopeId + "/" + role,
                role, scopeId, ordinal, scope, 256, rows,
                "b".repeat(64), "kms/key");
    }

    private void complete(
            org.pangrova.generation.api.page.GenerationLease lease,
            List<GenerationOutput> pageOutputs, long second) {
        byte[] report = ("report-" + lease.pageId()).getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(
                lease.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), pageOutputs, START.plusSeconds(second)))
                .isTrue();
    }

    private static List<GenerationPage> pages(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> new GenerationPage(
                        "job-1", "page-" + i, 10, i, i,
                        GenerationPageKind.SOURCE_MAP,
                        List.of(GenerationSplit.of(
                                "source-" + i,
                                ("canonical-split-" + i).getBytes(StandardCharsets.UTF_8))),
                        List.of(),
                        3))
                .toList();
    }

    private static GenerationDispatchSpec dispatchSpec(String tenantId) {
        return dispatchSpec(tenantId, List.of());
    }

    private static GenerationDispatchSpec dispatchSpec(String tenantId, List<GenerationPage> pages) {
        return GenerationDispatchSpecFactory.create(
                "job-1", org.pangrova.generation.submission.GenerationStageGraphFactory.forArtifactPages(
                        "canonical-plan".getBytes(StandardCharsets.UTF_8), "binding-template".getBytes(StandardCharsets.UTF_8), pages, java.util.List.of()),
                tenantId, "principal-" + tenantId,
                org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static List<GenerationOutput> outputs(int sourceOrdinal) {
        String source = "source-" + sourceOrdinal;
        return List.of(
                output(source, "DISKANN_GRAPH", 0),
                output(source, "RAW_VECTORS", 1),
                output(source, "ROW_LOCATORS", 2));
    }

    private static GenerationOutput output(String source, String role, int ordinal) {
        return new GenerationOutput(
                source + "-" + role,
                "s3://bucket/staging/job-1/" + source + "/" + role,
                role, source, ordinal, GenerationOutputScope.SOURCE_OBJECT,
                256, 10, "b".repeat(64), "kms/key");
    }

    private boolean completePage(
            String jobId, String pageId, long fencingToken, long ownerEpoch,
            byte[] reportBytes, String reportSha256,
            List<GenerationOutput> outputs, Instant now) {
        return completePage(
                TENANT, jobId, pageId, fencingToken, ownerEpoch,
                reportBytes, reportSha256, outputs, now);
    }

    private boolean completePage(
            String tenantId, String jobId, String pageId,
            long fencingToken, long ownerEpoch,
            byte[] reportBytes, String reportSha256,
            List<GenerationOutput> outputs, Instant now) {
        return store.completePage(new GenerationPageCompletion(
                tenantId, jobId, pageId, fencingToken, ownerEpoch,
                reportBytes, reportSha256, outputs, now, executions.reportedOwner(tenantId, jobId, ownerEpoch)));
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void injectedAuthorityFailureAbortsCreationWithoutDurableRows() {
        var denied = new org.pangrova.security.api.exception.AuthorizationDeniedException("authority backend denied admission");
        var authority = new org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority() {
            @Override
            public GenerationDispatchSpec requireCurrent(java.sql.Connection connection, String tenant, String job) {
                throw new AssertionError("Creation must validate the explicit compiler-observed specification");
            }
            @Override
            public void requireCurrent(java.sql.Connection connection, GenerationDispatchSpec spec) {
                throw denied;
            }
        };
        var guarded = new PostgresGenerationJobStore(dataSource, authority,
                org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores.executionFence(),
                PostgresGenerationStores.currentRoles(), PostgresGenerationStores.publicationCandidates());
        assertThatThrownBy(() -> guarded.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1)))
                .isSameAs(denied);
        assertThat(store.findJob(TENANT, "job-1")).isEmpty();
        assertThat(store.findDispatchSpec(TENANT, "job-1")).isEmpty();
    }

    @Test
    void authorityPortSeesCallerOwnedUncommittedTenantRevocationOnTheExactConnection() throws Exception {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (var caller = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            try (var statement = caller.prepareStatement("UPDATE tenants SET status='DELETED' WHERE tenant_id=?")) {
                statement.setString(1, TENANT);
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            var authority = new org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority() {
                @Override
                public GenerationDispatchSpec requireCurrent(java.sql.Connection connection, String tenant, String job) {
                    throw new AssertionError("Creation must not reread a nonexistent dispatch specification");
                }
                @Override
                public void requireCurrent(java.sql.Connection connection, GenerationDispatchSpec spec)
                        throws java.sql.SQLException {
                    assertThat(connection).isSameAs(caller);
                    calls.incrementAndGet();
                    GenerationAuthorityFixtures.artifacts().authority().requireCurrent(connection, spec);
                }
            };
            GenerationJobSubmissionTransactions guardedSubmissions = new PostgresGenerationJobStore(dataSource, authority,
                    PostgresGenerationStores.executionFence(), PostgresGenerationStores.currentRoles(),
                    PostgresGenerationStores.publicationCandidates());
            assertThatThrownBy(() -> guardedSubmissions.createJob(
                    caller, job(1), dispatchSpec(TENANT, pages(1)), pages(1)))
                    .isInstanceOf(org.pangrova.security.api.exception.AuthorizationDeniedException.class);
            assertThat(calls).hasValue(1);
            assertThat(caller.getAutoCommit()).isFalse();
            caller.rollback();
        }
        // The caller's rollback restores authority; the normal assembled store can create the job.
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        assertThat(store.findJob(TENANT, "job-1")).isPresent();
    }

    @Test
    void artifactFenceUsesTheQueryPortOnTheExactCallerTransaction() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var anchor = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (var caller = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            try (var update = caller.prepareStatement(
                    "UPDATE execution_anchor SET fencing_token=fencing_token+1 WHERE execution_id=?")) {
                update.setString(1, anchor.executionId());
                assertThat(update.executeUpdate()).isEqualTo(1);
            }
            var fence = new org.pangrova.persistence.generation.postgresql.execution.PostgresGenerationExecutionFence(
                    (connection, executionId, ownerId, incarnation, token) -> {
                        assertThat(connection).isSameAs(caller);
                        calls.incrementAndGet();
                        return new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionTransactionFence()
                                .requireCurrent(connection, executionId, ownerId, incarnation, token);
                    });
            var expected = new org.pangrova.generation.api.execution.GenerationExecutionOwner(
                    anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken() + 1);
            var actual = fence.requireCurrent(caller, TENANT, "job-1", expected);
            assertThat(actual.fencingToken()).isEqualTo(expected.fencingToken());
            assertThat(actual.acquiredAt()).isEqualTo(anchor.acquiredAt());
            assertThat(actual.expiresAt()).isEqualTo(anchor.expiresAt());
            assertThat(calls).hasValue(1);
            assertThat(caller.getAutoCommit()).isFalse();
            caller.rollback();
            PostgresGenerationStores.executionFence().requireAnchor(caller, TENANT, "job-1", anchor);
            caller.rollback();
        }
    }

    @Test
    void artifactFenceDoesNotTranslateStorageFailureOrMiswiredEvidenceToOwnerLoss() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var anchor = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        var expected = org.pangrova.generation.api.execution.GenerationExecutionOwner.from(anchor);
        try (var caller = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            var failure = new java.sql.SQLException("execution authority unavailable");
            var unavailable = new org.pangrova.persistence.generation.postgresql.execution.PostgresGenerationExecutionFence(
                    (connection, executionId, ownerId, incarnation, token) -> { throw failure; });
            assertThatThrownBy(() -> unavailable.requireCurrent(caller, TENANT, "job-1", expected))
                    .isSameAs(failure);
            var miswired = new org.pangrova.persistence.generation.postgresql.execution.PostgresGenerationExecutionFence(
                    (connection, executionId, ownerId, incarnation, token) ->
                            new org.pangrova.execution.api.ownership.ExecutionAnchor(
                                    "another-execution", ownerId, incarnation, token,
                                    anchor.acquiredAt(), anchor.expiresAt()));
            assertThatThrownBy(() -> miswired.requireCurrent(caller, TENANT, "job-1", expected))
                    .isInstanceOf(IllegalStateException.class)
                    .isNotInstanceOf(GenerationExecutionOwnerLostException.class)
                    .hasMessageContaining("another execution owner");
            caller.rollback();
        }
    }

    @Test
    void authorityResultCannotCombineAnotherTenantsSpecWithThisJobsExecutionAnchor() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var anchor = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        var miswired = new org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority() {
            @Override
            public GenerationDispatchSpec requireCurrent(java.sql.Connection connection, String tenant, String job) {
                return dispatchSpec("tenant-2");
            }
            @Override
            public void requireCurrent(java.sql.Connection connection, GenerationDispatchSpec spec) {
                throw new AssertionError("Resolution must use persisted scoped authority");
            }
        };
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> PostgresCurrentGenerationAuthority.resolve(miswired,
                    org.pangrova.persistence.workflow.postgresql.generation.PostgresGenerationStores.executionFence(),
                    PostgresGenerationStores.currentRoles(), connection,
                    TENANT, "job-1", org.pangrova.generation.api.execution.GenerationExecutionOwner.from(anchor)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another tenant or job");
            connection.rollback();
        }
    }

    @Test
    void currentRolesUseTheFencedConnectionAndFailureRollsBackClaimPreparation() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var anchor = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (var caller = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            var context = PostgresCurrentGenerationAuthority.resolve(GenerationAuthorityFixtures.artifacts().authority(),
                    PostgresGenerationStores.executionFence(), (connection, principal) -> {
                        assertThat(connection).isSameAs(caller);
                        assertThat(connection.getAutoCommit()).isFalse();
                        assertThat(principal).isEqualTo("principal-" + TENANT);
                        calls.incrementAndGet();
                        return PostgresGenerationStores.currentRoles().requireCurrentRoles(connection, principal);
                    }, caller, TENANT, "job-1", org.pangrova.generation.api.execution.GenerationExecutionOwner.from(anchor));
            assertThat(context.currentDirectRoles())
                    .isEqualTo(PostgresGenerationStores.currentRoles().requireCurrentRoles(caller, "principal-" + TENANT));
            assertThat(context.executionAnchor()).isEqualTo(anchor);
            assertThat(calls).hasValue(1);
            caller.rollback();
        }
        var failure = new java.sql.SQLException("current roles unavailable");
        var guarded = new PostgresGenerationJobStore(dataSource, GenerationAuthorityFixtures.artifacts().authority(),
                PostgresGenerationStores.executionFence(), (connection, principal) -> {
                    try (var statement = connection.createStatement()) {
                        statement.executeUpdate("UPDATE artifact_build_jobs SET failure_reason='must roll back'");
                    }
                    throw failure;
                }, PostgresGenerationStores.publicationCandidates());
        assertThatThrownBy(() -> guarded.claimNextPage(TENANT, "job-1", anchor, 1,
                Duration.ofMinutes(1), START))
                .isInstanceOf(org.pangrova.persistence.generation.api.exception.GenerationStorageException.class).hasCause(failure);
        assertThat(store.findJob(TENANT, "job-1")).contains(job(1));
        assertThat(store.claimNextPage(TENANT, "job-1", anchor, 1, Duration.ofMinutes(1), START)).isPresent();
    }

    @Test
    void publicationProjectionFiltersLiveOwnersBeforeLimitAndSeesUncommittedReplacement() throws Exception {
        byte[] report = "report".getBytes(StandardCharsets.UTF_8);
        for (String tenant : List.of(TENANT, "tenant-2")) {
            store.createJob(job(tenant, 1), dispatchSpec(tenant, pages(1)), pages(1));
            var page = store.claimNextPage(tenant, "job-1",
                    executions.takeOwnership(tenant, "job-1", "coordinator", 1), 1, Duration.ofMinutes(1), START)
                    .orElseThrow();
            assertThat(completePage(tenant, "job-1", page.pageId(), page.fencingToken(), page.ownerEpoch(),
                    report, sha256(report), outputs(0), START.plusSeconds(tenant.equals(TENANT) ? 1 : 3))).isTrue();
        }
        var actual = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        assertThat(store.claimReadyForPublication(TENANT, "job-1", actual, 1,
                Duration.ofMinutes(1), START.plusSeconds(2))).isPresent();
        assertThat(store.listReadyForPublication(1, START.plusSeconds(4)))
                .containsExactly(new org.pangrova.generation.api.identity.GenerationJobKey("tenant-2", "job-1"));
        try (var caller = dataSource.getConnection(); var observer = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            assertThat(PostgresGenerationStores.publicationCandidates().hasLiveOwner(caller, TENANT, "job-1")).isTrue();
            try (var statement = caller.prepareStatement(
                    "UPDATE execution_anchor SET fencing_token=fencing_token+1 WHERE execution_id=?")) {
                statement.setString(1, actual.executionId());
                assertThat(statement.executeUpdate()).isOne();
            }
            assertThat(PostgresGenerationStores.publicationCandidates().hasLiveOwner(caller, TENANT, "job-1")).isFalse();
            assertThat(PostgresGenerationStores.publicationCandidates().hasLiveOwner(observer, TENANT, "job-1")).isTrue();
            assertThat(PostgresGenerationStores.publicationCandidates().listReady(caller, 1, START.plusSeconds(4)))
                    .containsExactly(new org.pangrova.generation.api.identity.GenerationJobKey(TENANT, "job-1"));
            assertThat(caller.getAutoCommit()).isFalse();
            caller.rollback();
            assertThat(PostgresGenerationStores.publicationCandidates().hasLiveOwner(caller, TENANT, "job-1")).isTrue();
            caller.rollback();
        }
    }

}
