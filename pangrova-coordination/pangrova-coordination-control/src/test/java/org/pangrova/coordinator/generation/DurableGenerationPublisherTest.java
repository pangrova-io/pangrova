package org.pangrova.coordinator.generation;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.generation.submission.GenerationDispatchSpecFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class DurableGenerationPublisherTest {

    private org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    private static final Instant START = Instant.parse("2026-09-06T00:00:00Z");
    private static final String TYPE = GenerationOutputTypeIds.VECTOR_INDEX;
    private static final String TENANT = "tenant-1";

    private PostgresGenerationJobStore store;
    private MutableClock clock;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "durable_artifact_publisher");
        store = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource);
        var source = new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items");
        org.pangrova.metadata.testinfra.GenerationAuthorityFixtures.provision(
                dataSource, TENANT, "principal-1", org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                        source, java.util.Set.of("embedding"), source));
        clock = new MutableClock(START);
    }

    @Test
    void leaderPublishesReadyJobExactlyOnce() {
        createReadyJob();
        AtomicInteger calls = new AtomicInteger();
        GenerationPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            assertThat(store.markPublished(
                    TENANT, claim.jobId(), owner, epoch,
                    claim.publicationFencingToken(), now)).isTrue();
            return manifest(claim, now);
        };

        try (DurableGenerationPublisher publisher = publisher(
                "maintenance-a", adapter)) {
            publisher.publishAvailable();
            publisher.publishAvailable();
        }

        assertThat(calls).hasValue(1);
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
        assertThat(store.listReadyForPublication(10, START.plusSeconds(60))).isEmpty();
    }

    @Test
    void publicationFailureIsDurablyDeferredBeforeRetry() {
        createReadyJob();
        AtomicInteger calls = new AtomicInteger();
        GenerationPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("catalog unavailable");
        };

        try (DurableGenerationPublisher publisher = publisher(
                "maintenance-a", adapter)) {
            publisher.publishAvailable();
            assertThat(calls).hasValue(1);
            assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                    .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
            assertThat(store.listReadyForPublication(10, START.plusSeconds(14))).isEmpty();

            clock.advance(Duration.ofSeconds(15));
            publisher.publishAvailable();
        }

        assertThat(calls).hasValue(2);
    }

    @Test
    void contenderNeverPublishesWhileAnotherJobClaimIsActive() {
        createReadyJob();
        assertThat(store.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 1), 1,
                Duration.ofSeconds(30), START))
                .isPresent();
        AtomicInteger calls = new AtomicInteger();
        GenerationPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            return manifest(claim, now);
        };

        try (DurableGenerationPublisher contender = publisher(
                "maintenance-b", adapter)) {
            contender.publishAvailable();
        }

        assertThat(calls).hasValue(0);
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
    }

    @Test
    void stalePublicationAnchorCanBeReclaimedBeforeLeaseTimeout() {
        createReadyJob();
        var buildAnchor = executions.takeOwnership(TENANT, "job-1", "maintenance-a", 1);
        assertThat(store.claimReadyForPublication(
                TENANT, "job-1", buildAnchor, buildAnchor.fencingToken(),
                Duration.ofSeconds(30), START)).isPresent();
        assertThat(store.listReadyForPublication(10, START.plusSeconds(1))).isEmpty();

        executions.ownership("maintenance-a").yield(buildAnchor);
        assertThat(store.listReadyForPublication(10, START.plusSeconds(1)))
                .containsExactly(new org.pangrova.generation.api.identity.GenerationJobKey(TENANT, "job-1"));

        AtomicInteger calls = new AtomicInteger();
        GenerationPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            assertThat(store.markPublished(
                    TENANT, claim.jobId(), owner, epoch,
                    claim.publicationFencingToken(), now)).isTrue();
            return manifest(claim, now);
        };
        try (DurableGenerationPublisher publisher = publisher(
                "maintenance-b", adapter)) {
            publisher.publishAvailable();
        }
        assertThat(calls).hasValue(1);
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
    }

    private DurableGenerationPublisher publisher(
            String owner, GenerationPublicationStore adapter) {
        return new DurableGenerationPublisher(
                store, adapter, executions.ownership(owner),
                new DurableGenerationPublisher.PollSettings(Duration.ofSeconds(30), Duration.ofSeconds(15),
                        Duration.ofSeconds(1), 8, clock),
                Executors.newSingleThreadScheduledExecutor());
    }

    private void createReadyJob() {
        store.createJob(new GenerationJob(
                        TENANT, "job-1", "artifact-1", TYPE, "generation-1",
                        "catalog.schema.items", "snapshot-1",
                        org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                        org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                                "catalog.schema.items", "snapshot-1"),
                        org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                        "s3://bucket/staging/artifact-1/generation-1",
                        "pangrova.diskann.segment", 1, 0, "a".repeat(64),
                        "native-build-1", "",
                        "request-1", 1, 0, GenerationJobState.QUEUED,
                        0, START, START, START),
                GenerationDispatchSpecFactory.create(
                        "job-1", org.pangrova.generation.submission.GenerationStageGraphFactory.forArtifactPages(
                                "plan".getBytes(StandardCharsets.UTF_8), "binding".getBytes(StandardCharsets.UTF_8),
                                List.of(new GenerationPage("job-1", "page-1", 0, 0, 0,
                                        GenerationPageKind.SOURCE_MAP, List.of(GenerationSplit.of(
                                                "source-1", "canonical-split".getBytes(StandardCharsets.UTF_8))), List.of(), 3)), java.util.List.of()),
                        TENANT, "principal-1",
                        org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                                new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                                java.util.Set.of("embedding"),
                                new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                        new GenerationDispatchSpec.ResourceGrant(
                                1024, 1, 1024, 60_000)),
                List.of(new GenerationPage(
                        "job-1", "page-1", 0, 0, 0,
                        GenerationPageKind.SOURCE_MAP,
                        List.of(GenerationSplit.of(
                                "source-1", "canonical-split".getBytes(StandardCharsets.UTF_8))),
                        List.of(), 3)));
        var lease = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 1), 1,
                Duration.ofSeconds(30), START)
                .orElseThrow();
        byte[] report = "report".getBytes(StandardCharsets.UTF_8);
        assertThat(store.completePage(new GenerationPageCompletion(
                TENANT, lease.jobId(), lease.pageId(),
                lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), List.of(
                        output("DISKANN_GRAPH", 0), output("RAW_VECTORS", 1),
                        output("ROW_LOCATORS", 2)), START, executions.reportedOwner(TENANT, lease.jobId(), lease.ownerEpoch())))).isTrue();
    }

    private static GenerationOutput output(String role, int ordinal) {
        return new GenerationOutput(
                "source-1-" + role,
                "s3://bucket/staging/artifact-1/generation-1/source-1/" + role,
                role, "source-1", ordinal, GenerationOutputScope.SOURCE_OBJECT,
                128, 3, "c".repeat(64), "");
    }

    private static String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static GenerationManifest manifest(
            GenerationJob claim, Instant publishedAt) {
        return new GenerationManifest(
                claim.tenantId(), claim.jobId(), claim.artifactId(), claim.artifactType(),
                claim.stagingGeneration(), claim.sourceId(), claim.sourceSnapshot(),
                claim.executionKind(), claim.dependencySet(), claim.emptyPartitionPolicy(),
                claim.formatId(),
                claim.formatMajor(), claim.formatMinor(), claim.schemaSha256(),
                claim.writerBuildId(), claim.stagingUri() + "/manifest.pb",
                "b".repeat(64), 0, 0, 0, publishedAt);
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new IllegalArgumentException("test clock is UTC only");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
