package org.pangrova.coordinator.databuild;


import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.DataBuildOutputScope;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.build.DataBuildPageCompletion;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;
import org.pangrova.databuild.api.publication.DataBuildPublicationStore;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class DurableDataBuildPublisherTest {

    private org.pangrova.coordinator.testinfra.CoordinatorDataBuildExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    private static final Instant START = Instant.parse("2026-09-06T00:00:00Z");
    private static final String TYPE = DataBuildOutputTypeIds.VECTOR_INDEX;
    private static final String TENANT = "tenant-1";

    private JdbcDataBuildStore store;
    private MutableClock clock;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "durable_artifact_publisher");
        store = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorDataBuildExecutionFixtures(dataSource);
        var source = new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items");
        org.pangrova.coordinator.testinfra.CoordinatorDataBuildAuthorityFixtures.provision(
                dataSource, TENANT, "principal-1", org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        source, java.util.Set.of("embedding"), source));
        clock = new MutableClock(START);
    }

    @Test
    void leaderPublishesReadyJobExactlyOnce() {
        createReadyJob();
        AtomicInteger calls = new AtomicInteger();
        DataBuildPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            assertThat(store.markPublished(
                    TENANT, claim.jobId(), owner, epoch,
                    claim.publicationFencingToken(), now)).isTrue();
            return manifest(claim, now);
        };

        try (DurableDataBuildPublisher publisher = publisher(
                "maintenance-a", Map.of(TYPE, adapter))) {
            publisher.publishAvailable();
            publisher.publishAvailable();
        }

        assertThat(calls).hasValue(1);
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(DataBuildJobState.PUBLISHED);
        assertThat(store.listReadyForPublication(10, START.plusSeconds(60))).isEmpty();
    }

    @Test
    void publicationFailureIsDurablyDeferredBeforeRetry() {
        createReadyJob();
        AtomicInteger calls = new AtomicInteger();
        DataBuildPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("catalog unavailable");
        };

        try (DurableDataBuildPublisher publisher = publisher(
                "maintenance-a", Map.of(TYPE, adapter))) {
            publisher.publishAvailable();
            assertThat(calls).hasValue(1);
            assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                    .isEqualTo(DataBuildJobState.READY_TO_PUBLISH);
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
        DataBuildPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            return manifest(claim, now);
        };

        try (DurableDataBuildPublisher contender = publisher(
                "maintenance-b", Map.of(TYPE, adapter))) {
            contender.publishAvailable();
        }

        assertThat(calls).hasValue(0);
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(DataBuildJobState.READY_TO_PUBLISH);
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
                .containsExactly(new org.pangrova.databuild.api.build.DataBuildJobKey(TENANT, "job-1"));

        AtomicInteger calls = new AtomicInteger();
        DataBuildPublicationStore adapter = (claim, owner, epoch, now) -> {
            calls.incrementAndGet();
            assertThat(store.markPublished(
                    TENANT, claim.jobId(), owner, epoch,
                    claim.publicationFencingToken(), now)).isTrue();
            return manifest(claim, now);
        };
        try (DurableDataBuildPublisher publisher = publisher(
                "maintenance-b", Map.of(TYPE, adapter))) {
            publisher.publishAvailable();
        }
        assertThat(calls).hasValue(1);
        assertThat(store.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(DataBuildJobState.PUBLISHED);
    }

    private DurableDataBuildPublisher publisher(
            String owner, Map<String, DataBuildPublicationStore> adapters) {
        return new DurableDataBuildPublisher(
                store, adapters, executions.ownership(owner),
                new DurableDataBuildPublisher.PollSettings(Duration.ofSeconds(30), Duration.ofSeconds(15),
                        Duration.ofSeconds(1), 8, clock),
                Executors.newSingleThreadScheduledExecutor());
    }

    private void createReadyJob() {
        store.createJob(new DataBuildJob(
                        TENANT, "job-1", "artifact-1", TYPE, "generation-1",
                        "catalog.schema.items", "snapshot-1",
                        org.pangrova.databuild.api.DataBuildExecutionKind.BUILD,
                        org.pangrova.databuild.api.DataBuildDependencySet.sourceSnapshot(
                                "catalog.schema.items", "snapshot-1"),
                        org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy.FORBID,
                        "s3://bucket/staging/artifact-1/generation-1",
                        "pangrova.diskann.segment", 1, 0, "a".repeat(64),
                        "native-build-1", "",
                        "request-1", 1, 0, DataBuildJobState.QUEUED,
                        0, START, START, START),
                DataBuildDispatchSpecFactory.create(
                        "job-1", org.pangrova.databuild.assembly.DataBuildStageGraphFactory.forArtifactPages(
                                "plan".getBytes(StandardCharsets.UTF_8), "binding".getBytes(StandardCharsets.UTF_8),
                                List.of(new DataBuildPage("job-1", "page-1", 0, 0, 0,
                                        DataBuildPageKind.SOURCE_MAP, List.of(DataBuildSplit.of(
                                                "source-1", "canonical-split".getBytes(StandardCharsets.UTF_8))), List.of(), 3)), java.util.List.of()),
                        TENANT, "principal-1",
                        org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                                new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                                java.util.Set.of("embedding"),
                                new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                        new DataBuildDispatchSpec.ResourceGrant(
                                1024, 1, 1024, 60_000)),
                List.of(new DataBuildPage(
                        "job-1", "page-1", 0, 0, 0,
                        DataBuildPageKind.SOURCE_MAP,
                        List.of(DataBuildSplit.of(
                                "source-1", "canonical-split".getBytes(StandardCharsets.UTF_8))),
                        List.of(), 3)));
        var lease = store.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 1), 1,
                Duration.ofSeconds(30), START)
                .orElseThrow();
        byte[] report = "report".getBytes(StandardCharsets.UTF_8);
        assertThat(store.completePage(new DataBuildPageCompletion(
                TENANT, lease.jobId(), lease.pageId(),
                lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), List.of(
                        output("DISKANN_GRAPH", 0), output("RAW_VECTORS", 1),
                        output("ROW_LOCATORS", 2)), START, executions.reportedOwner(TENANT, lease.jobId(), lease.ownerEpoch())))).isTrue();
    }

    private static DataBuildOutput output(String role, int ordinal) {
        return new DataBuildOutput(
                "source-1-" + role,
                "s3://bucket/staging/artifact-1/generation-1/source-1/" + role,
                role, "source-1", ordinal, DataBuildOutputScope.SOURCE_OBJECT,
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

    private static DataBuildGenerationManifest manifest(
            DataBuildJob claim, Instant publishedAt) {
        return new DataBuildGenerationManifest(
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
