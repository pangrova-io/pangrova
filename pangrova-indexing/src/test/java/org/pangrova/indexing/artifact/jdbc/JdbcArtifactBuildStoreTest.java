package org.pangrova.indexing.artifact.jdbc;

import org.pangrova.indexing.store.jdbc.ArtifactExecutionFence;

import org.pangrova.indexing.store.jdbc.JdbcArtifactBuildStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.indexing.control.ArtifactBuildJob;
import org.pangrova.indexing.control.ArtifactBuildJobKey;
import org.pangrova.indexing.control.ArtifactBuildJobState;
import org.pangrova.indexing.control.ArtifactBuildOutput;
import org.pangrova.indexing.api.ArtifactBuildOutputScope;
import org.pangrova.indexing.control.ArtifactBuildPage;
import org.pangrova.indexing.api.ArtifactBuildPageKind;
import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.ArtifactBuildSplit;
import org.pangrova.indexing.control.ArtifactPageCompletion;
import org.pangrova.indexing.api.ArtifactTypeIds;
import org.pangrova.indexing.testinfra.ArtifactAuthorityTestDoubles;
import org.pangrova.indexing.testinfra.SharedPostgresContainer;

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

class JdbcArtifactBuildStoreTest {

    private org.pangrova.indexing.testinfra.ArtifactExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-05T00:00:00Z");
    private static final String TENANT = "tenant-1";

    private JdbcArtifactBuildStore store;
    private javax.sql.DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("artifact_build");
        ArtifactAuthorityTestDoubles.provisionTenant(dataSource, TENANT, "principal-" + TENANT);
        ArtifactAuthorityTestDoubles.provisionTenant(dataSource, "tenant-2", "principal-tenant-2");
        store = new JdbcArtifactBuildStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
        executions = new org.pangrova.indexing.testinfra.ArtifactExecutionFixtures(dataSource);
    }

    @Test
    void callerOwnedTransactionCommitsOrRollsBackTheSameGraphAndPageInsertion() throws Exception {
        var pages = pages(1);
        var spec = dispatchSpec(TENANT, pages);
        try (var connection = dataSource.getConnection()) {
            assertThatThrownBy(() -> JdbcArtifactBuildStore.createJobWithinTransaction(ArtifactAuthorityTestDoubles.permissive(), connection, job(1), spec, pages))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("caller-owned transaction");
            connection.setAutoCommit(false);
            JdbcArtifactBuildStore.createJobWithinTransaction(ArtifactAuthorityTestDoubles.permissive(), connection, job(1), spec, pages);
            assertThat(store.findJob(TENANT, "job-1")).isEmpty();
            connection.rollback();
            assertThat(store.findDispatchSpec(TENANT, "job-1")).isEmpty();
            JdbcArtifactBuildStore.createJobWithinTransaction(ArtifactAuthorityTestDoubles.permissive(), connection, job(1), spec, pages);
            connection.commit();
        }
        assertThat(store.findDispatchSpec(TENANT, "job-1")).contains(spec);
        assertThat(store.claimNextPage(TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 1), 1,
                Duration.ofSeconds(30), START)).hasValueSatisfying(lease ->
                        assertThat(lease.sourceObjectIds()).containsExactly("source-0"));
    }

    @Test
    void persistsFencedPagesAndOpensOnlyAnExactAllPagesBarrier() {
        store.createJob(job(2), dispatchSpec(TENANT, pages(2)), pages(2));
        store.createJob(job(2), dispatchSpec(TENANT, pages(2)), pages(2));

        assertThat(store.listRunnableJobs(10, START))
                .containsExactly(new ArtifactBuildJobKey(TENANT, "job-1"));
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
                    assertThat(job.state()).isEqualTo(ArtifactBuildJobState.RUNNING);
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
                    assertThat(job.state()).isEqualTo(ArtifactBuildJobState.READY_TO_PUBLISH);
                    assertThat(job.completedPages()).isEqualTo(2);
                    assertThat(job.publicationExpectedUpdatedAt()).isEqualTo(START);
                });
        assertThat(store.listReadyForPublication(10, START.plusSeconds(3)))
                .containsExactly(new ArtifactBuildJobKey(TENANT, "job-1"));
        assertThat(store.loadCompleteOutputs(TENANT, "job-1"))
                .extracting(ArtifactBuildOutput::sourceObjectId)
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
        var key = new ArtifactBuildJobKey(TENANT, "job-1");
        var anchors = new org.pangrova.query.transaction.JdbcExecutionAnchorStore(dataSource);
        var first = anchors.tryAcquire(key.executionId(), "coordinator", "process-before-restart", Duration.ofMinutes(1)).orElseThrow();
        var oldPage = store.claimNextPage(TENANT, key.jobId(), first, 1, Duration.ofSeconds(10), START).orElseThrow();
        var restarted = new JdbcArtifactBuildStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
        assertThat(restarted.findExecutionJob(key.executionId())).contains(key);
        var recoveredAnchors = new org.pangrova.query.transaction.JdbcExecutionAnchorStore(dataSource);
        assertThat(recoveredAnchors.current(key.executionId())).contains(first);
        assertThat(recoveredAnchors.tryAcquire(key.executionId(), "coordinator", "process-after-restart",
                Duration.ofMinutes(1))).isEmpty();
        assertThat(anchors.yield(first)).isTrue();
        var replacement = recoveredAnchors.tryAcquire(key.executionId(), "coordinator", "process-after-restart",
                Duration.ofMinutes(1)).orElseThrow();
        assertThat(replacement.fencingToken()).isGreaterThan(first.fencingToken());
        assertThatThrownBy(() -> restarted.renewPage(oldPage, Duration.ofSeconds(10), START.plusSeconds(1)))
                .isInstanceOf(ArtifactExecutionFence.LostOwnerException.class).hasMessageContaining("owner fence was lost");
        var current = restarted.claimNextPage(TENANT, key.jobId(), replacement, 1,
                Duration.ofSeconds(30), START.plusSeconds(11)).orElseThrow();
        byte[] report = new byte[] {1, 2, 3};
        // Copying the new page fence does not upgrade old immutable process evidence.
        assertThat(restarted.completePage(new ArtifactPageCompletion(TENANT, key.jobId(), current.pageId(),
                current.fencingToken(), current.ownerEpoch(), report, sha256(report), outputs(0), START.plusSeconds(12),
                org.pangrova.indexing.control.ArtifactExecutionOwner.from(first)))).isFalse();
        assertThat(restarted.completePage(new ArtifactPageCompletion(TENANT, key.jobId(), current.pageId(),
                current.fencingToken(), current.ownerEpoch(), report, sha256(report), outputs(0), START.plusSeconds(12),
                org.pangrova.indexing.control.ArtifactExecutionOwner.from(replacement)))).isTrue();
        var publication = restarted.claimReadyForPublication(TENANT, key.jobId(), replacement, 1,
                Duration.ofSeconds(30), START.plusSeconds(13)).orElseThrow();
        assertThat(recoveredAnchors.yield(replacement)).isTrue();
        var third = recoveredAnchors.tryAcquire(key.executionId(), "coordinator", "third-process",
                Duration.ofMinutes(1)).orElseThrow();
        assertThatThrownBy(() -> restarted.markPublished(TENANT, key.jobId(), replacement.ownerId(), 1,
                publication.publicationFencingToken(), START.plusSeconds(14)))
                .isInstanceOf(ArtifactExecutionFence.LostOwnerException.class).hasMessageContaining("owner fence was lost");
        var replacementPublication = restarted.claimReadyForPublication(TENANT, key.jobId(), third, 2,
                Duration.ofSeconds(30), START.plusSeconds(44)).orElseThrow();
        assertThat(restarted.markPublished(TENANT, key.jobId(), third.ownerId(), 2,
                replacementPublication.publicationFencingToken(), START.plusSeconds(45))).isTrue();
    }

    @Test
    void exactRoutingSeparatesEqualJobNamesAndRejectsAnotherTenantsAnchor() {
        store.createJob(job(TENANT, 1), dispatchSpec(TENANT, pages(1)), pages(1));
        store.createJob(job("tenant-2", 1), dispatchSpec("tenant-2", pages(1)), pages(1));
        var first = new ArtifactBuildJobKey(TENANT, "job-1");
        var second = new ArtifactBuildJobKey("tenant-2", "job-1");
        assertThat(first.executionId()).isNotEqualTo(second.executionId());
        assertThat(store.findExecutionJob(first.executionId())).contains(first);
        assertThat(store.findExecutionJob(second.executionId())).contains(second);
        var anchor = executions.takeOwnership(TENANT, first.jobId(), "coordinator", 1);
        assertThatThrownBy(() -> store.claimNextPage("tenant-2", second.jobId(), anchor, 1,
                Duration.ofMinutes(1), START)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another artifact job");
        assertThat(store.findJob("tenant-2", second.jobId()).orElseThrow().state()).isEqualTo(ArtifactBuildJobState.QUEUED);
    }

    @Test
    void anchorRowLockHoldsTheSameBackendRowAgainstTakeoverUntilTheTransactionEnds() throws Exception {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        var anchor = executions.takeOwnership(TENANT, "job-1", "coordinator", 1);
        var anchors = new org.pangrova.query.transaction.JdbcExecutionAnchorStore(dataSource);
        var executor = Executors.newSingleThreadExecutor();
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            ArtifactAuthorityTestDoubles.permissive().requireCurrent(connection, TENANT, "job-1");
            ArtifactExecutionFence.requireAnchor(connection, TENANT, "job-1", anchor);
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
                .isEqualTo(ArtifactBuildJobState.PUBLISHED);
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
                .isEqualTo(ArtifactBuildJobState.CANCELLED);
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
                        new ArtifactBuildJobKey(TENANT, "job-1"),
                        new ArtifactBuildJobKey(otherTenant, "job-1"));

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
                .isEqualTo(ArtifactBuildJobState.CANCELLED);
        assertThat(store.findJob(otherTenant, "job-1").orElseThrow().state())
                .isEqualTo(ArtifactBuildJobState.READY_TO_PUBLISH);
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
                .extracting(ArtifactBuildOutput::sourceObjectId)
                .containsOnly("source-1");
        assertThat(store.matchesPublishedSplits(TENANT, "job-1", java.util.Map.of(
                "source-0", sha256("canonical-split-0".getBytes(StandardCharsets.UTF_8)),
                "source-1", sha256("canonical-split-1".getBytes(StandardCharsets.UTF_8)))))
                .isTrue();
        assertThat(store.matchesPublishedSplits(TENANT, "job-1", java.util.Map.of(
                "source-1", "f".repeat(64))))
                .isFalse();
        assertThat(store.loadPublishedSourceSplits(TENANT, "job-1", java.util.Set.of("source-0")))
                .containsEntry("source-0", ArtifactBuildSplit.of("source-0",
                        "canonical-split-0".getBytes(StandardCharsets.UTF_8)));
        assertThat(store.loadPublishedSourceSplits(TENANT, "job-1", java.util.Set.of("missing")))
                .isEmpty();
    }

    @Test
    void releasesBoundedReductionDagOnlyAfterEveryDependencySucceeds() {
        List<ArtifactBuildPage> dag = bm25Pages();
        store.createJob(bm25Job(dag.size()), dispatchSpec(TENANT, dag), dag);

        var first = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1), START)
                .orElseThrow();
        assertThat(first.kind()).isEqualTo(ArtifactBuildPageKind.SOURCE_MAP);
        complete(first, bm25SourceOutputs(first.pageId(), "source-0"), 1);

        var second = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1),
                START.plusSeconds(2)).orElseThrow();
        assertThat(second.kind()).isEqualTo(ArtifactBuildPageKind.SOURCE_MAP);
        complete(second, bm25SourceOutputs(second.pageId(), "source-1"), 3);

        var reduce = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1),
                START.plusSeconds(4)).orElseThrow();
        assertThat(reduce.kind()).isEqualTo(ArtifactBuildPageKind.DEPENDENCY_REDUCE);
        assertThat(reduce.dependencyPageIds()).containsExactly("map-0", "map-1");
        assertThat(store.loadDependencyOutputs(
                TENANT, "job-1", reduce.dependencyPageIds()))
                .hasSize(6);
        complete(reduce, List.of(scopedOutput(
                reduce.pageId(), "BM25_STATS_SHARD", 0,
                ArtifactBuildOutputScope.WORK_UNIT, 20)), 5);

        var finalize = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 10), 10, Duration.ofMinutes(1),
                START.plusSeconds(6)).orElseThrow();
        assertThat(finalize.kind())
                .isEqualTo(ArtifactBuildPageKind.GENERATION_FINALIZE);
        assertThat(finalize.dependencyPageIds()).containsExactly("reduce-0");
        complete(finalize, List.of(scopedOutput(
                "2", "BM25_CORPUS_STATS", 0,
                ArtifactBuildOutputScope.GENERATION, 20)), 7);

        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(ArtifactBuildJobState.READY_TO_PUBLISH);
    }

    @Test
    void rejectsChangedIdempotentCreateAndWrongCoverage() {
        store.createJob(job(1), dispatchSpec(TENANT, pages(1)), pages(1));
        ArtifactBuildJob changed = new ArtifactBuildJob(
                TENANT, "job-1", "artifact-other", "DISKANN", "2",
                "catalog.schema.items", "snapshot-1",
                org.pangrova.indexing.api.ArtifactExecutionKind.BUILD,
                org.pangrova.indexing.api.ArtifactDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/job-1", "pangrova.diskann", 1, 0,
                "a".repeat(64), "writer-1", "kms/key", "request-1",
                1, 0, ArtifactBuildJobState.QUEUED, 0, START, START, START);
        assertThatThrownBy(() -> store.createJob(changed, dispatchSpec(TENANT, pages(1)), pages(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different work");

        var lease = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "coordinator", 1), 1, Duration.ofMinutes(1), START)
                .orElseThrow();
        byte[] report = "report".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> completePage(
                lease.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), List.of(new ArtifactBuildOutput(
                        "object", "s3://bucket/staging/job-1/object", "DISKANN_GRAPH",
                        "wrong-source", 0, ArtifactBuildOutputScope.SOURCE_OBJECT,
                        64, 1, "b".repeat(64), "kms/key")),
                START.plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("work-page role contract");
    }

    private static ArtifactBuildJob job(int pages) {
        return job(TENANT, pages);
    }

    private static ArtifactBuildJob job(String tenantId, int pages) {
        return new ArtifactBuildJob(
                tenantId, "job-1", "index-1", ArtifactTypeIds.VECTOR_INDEX, "2",
                "catalog.schema.items", "snapshot-1",
                org.pangrova.indexing.api.ArtifactExecutionKind.BUILD,
                org.pangrova.indexing.api.ArtifactDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/job-1", "pangrova.diskann.segment", 1, 0,
                "a".repeat(64), "writer-1", "kms/key", "request-1",
                pages, 0, ArtifactBuildJobState.QUEUED, 0, START, START, START);
    }

    private static ArtifactBuildJob bm25Job(int pages) {
        return new ArtifactBuildJob(
                TENANT, "job-1", "index-1", ArtifactTypeIds.TEXT_INDEX, "2",
                "catalog.schema.items", "snapshot-1",
                org.pangrova.indexing.api.ArtifactExecutionKind.BUILD,
                org.pangrova.indexing.api.ArtifactDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/job-1", "pangrova.bm25.segment", 1, 0,
                "a".repeat(64), "writer-1", "kms/key", "request-1",
                pages, 0, ArtifactBuildJobState.QUEUED, 0, START, START, START);
    }

    private static List<ArtifactBuildPage> bm25Pages() {
        return List.of(
                sourcePage("map-0", 0, "source-0"),
                sourcePage("map-1", 1, "source-1"),
                new ArtifactBuildPage(
                        "job-1", "reduce-0", 11, 0, 2,
                        ArtifactBuildPageKind.DEPENDENCY_REDUCE,
                        List.of(), List.of("map-0", "map-1"), 3),
                new ArtifactBuildPage(
                        "job-1", "finalize-0", 12, 0, 3,
                        ArtifactBuildPageKind.GENERATION_FINALIZE,
                        List.of(), List.of("reduce-0"), 3));
    }

    private static ArtifactBuildPage sourcePage(
            String pageId, int ordinal, String source) {
        return new ArtifactBuildPage(
                "job-1", pageId, 10, ordinal, ordinal,
                ArtifactBuildPageKind.SOURCE_MAP,
                List.of(ArtifactBuildSplit.of(
                        source, ("canonical-" + source).getBytes(StandardCharsets.UTF_8))),
                List.of(), 3);
    }

    private static List<ArtifactBuildOutput> bm25SourceOutputs(
            String pageId, String source) {
        return List.of(
                scopedOutput(source, "BM25_POSTINGS", 0,
                        ArtifactBuildOutputScope.SOURCE_OBJECT, 10),
                scopedOutput(source, "ROW_LOCATORS", 1,
                        ArtifactBuildOutputScope.SOURCE_OBJECT, 10),
                scopedOutput(pageId, "BM25_STATS_SHARD", 2,
                        ArtifactBuildOutputScope.WORK_UNIT, 10));
    }

    private static ArtifactBuildOutput scopedOutput(
            String scopeId, String role, int ordinal,
            ArtifactBuildOutputScope scope, long rows) {
        return new ArtifactBuildOutput(
                scopeId + "-" + role,
                "s3://bucket/staging/job-1/" + scopeId + "/" + role,
                role, scopeId, ordinal, scope, 256, rows,
                "b".repeat(64), "kms/key");
    }

    private void complete(
            org.pangrova.indexing.control.ArtifactBuildLease lease,
            List<ArtifactBuildOutput> pageOutputs, long second) {
        byte[] report = ("report-" + lease.pageId()).getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(
                lease.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), pageOutputs, START.plusSeconds(second)))
                .isTrue();
    }

    private static List<ArtifactBuildPage> pages(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> new ArtifactBuildPage(
                        "job-1", "page-" + i, 10, i, i,
                        ArtifactBuildPageKind.SOURCE_MAP,
                        List.of(ArtifactBuildSplit.of(
                                "source-" + i,
                                ("canonical-split-" + i).getBytes(StandardCharsets.UTF_8))),
                        List.of(),
                        3))
                .toList();
    }

    private static ArtifactBuildDispatchSpec dispatchSpec(String tenantId) {
        return dispatchSpec(tenantId, List.of());
    }

    private static ArtifactBuildDispatchSpec dispatchSpec(String tenantId, List<ArtifactBuildPage> pages) {
        return ArtifactBuildDispatchSpec.of(
                "job-1", org.pangrova.indexing.control.ArtifactStageGraph.forArtifactPages(
                        "canonical-plan".getBytes(StandardCharsets.UTF_8), "binding-template".getBytes(StandardCharsets.UTF_8), pages, java.util.List.of()),
                tenantId, "principal-" + tenantId,
                org.pangrova.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static List<ArtifactBuildOutput> outputs(int sourceOrdinal) {
        String source = "source-" + sourceOrdinal;
        return List.of(
                output(source, "DISKANN_GRAPH", 0),
                output(source, "RAW_VECTORS", 1),
                output(source, "ROW_LOCATORS", 2));
    }

    private static ArtifactBuildOutput output(String source, String role, int ordinal) {
        return new ArtifactBuildOutput(
                source + "-" + role,
                "s3://bucket/staging/job-1/" + source + "/" + role,
                role, source, ordinal, ArtifactBuildOutputScope.SOURCE_OBJECT,
                256, 10, "b".repeat(64), "kms/key");
    }

    private boolean completePage(
            String jobId, String pageId, long fencingToken, long ownerEpoch,
            byte[] reportBytes, String reportSha256,
            List<ArtifactBuildOutput> outputs, Instant now) {
        return completePage(
                TENANT, jobId, pageId, fencingToken, ownerEpoch,
                reportBytes, reportSha256, outputs, now);
    }

    private boolean completePage(
            String tenantId, String jobId, String pageId,
            long fencingToken, long ownerEpoch,
            byte[] reportBytes, String reportSha256,
            List<ArtifactBuildOutput> outputs, Instant now) {
        return store.completePage(new ArtifactPageCompletion(
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
}
