package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;

import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobKey;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.build.ArtifactExchangeFinalization;
import org.pangrova.metadata.artifact.build.ArtifactExchangeRegistration;
import org.pangrova.metadata.artifact.build.ArtifactExecutionOwner;
import org.pangrova.metadata.artifact.build.ArtifactPageCompletion;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.testinfra.ArtifactAuthorityFixtures;
import org.pangrova.metadata.testinfra.ArtifactExecutionFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcArtifactStageExchangeStoreTest {
    private static final ArtifactBuildJobKey KEY = new ArtifactBuildJobKey("stage-tenant", "stage-job");
    private static final Instant NOW = Instant.parse("2026-10-01T00:00:00Z");
    private DataSource dataSource;
    private JdbcArtifactBuildStore builds;
    private JdbcArtifactStageExchangeStore exchanges;
    private ArtifactExecutionFixtures owners;
    private ExecutionAnchor anchor;
    private ArtifactBuildDispatchSpec spec;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("durable_stage_barrier");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        builds = new JdbcArtifactBuildStore(dataSource);
        exchanges = new JdbcArtifactStageExchangeStore(dataSource);
        owners = new ArtifactExecutionFixtures(dataSource);
        spec = specification();
        ArtifactAuthorityFixtures.provision(dataSource, KEY.tenantId(), spec.principal(), spec.authorizationScope());
        builds.createJob(job(), spec, List.of(
                new ArtifactBuildPage(KEY.jobId(), "producer-0", 1, 0, 0, ArtifactBuildPageKind.RELATIONAL_COMPUTE,
                        List.of(), List.of(), 3),
                new ArtifactBuildPage(KEY.jobId(), "terminal-0", 0, 0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION,
                        List.of(), List.of("producer-0"), 3)));
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-a", 8);
    }

    @Test
    void successfulMetadataProducerDoesNotOpenConsumerUntilExternalFinalizedProofIsPersisted() {
        var allocation = registration();
        exchanges.register(KEY, owner(), allocation);
        assertThatThrownBy(() -> exchanges.finalizeExchange(KEY, owner(), proof(allocation)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("immutable producer winners");
        completeProducer();
        assertThat(builds.listRunnableJobs(10, NOW)).isEmpty();
        assertThat(builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW)).isEmpty();
        exchanges.finalizeExchange(KEY, owner(), proof(allocation));
        assertThat(builds.listRunnableJobs(10, NOW)).containsExactly(KEY);
        assertThat(builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW))
                .hasValueSatisfying(lease -> assertThat(lease.pageId()).isEqualTo("terminal-0"));
    }

    @Test
    void restartRestoresOriginalAllocationAndLostOwnerCannotPublishBarrier() {
        var original = registration();
        exchanges.register(KEY, owner(), original);
        completeProducer();
        var firstOwner = owner();
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        var restored = new JdbcArtifactStageExchangeStore(dataSource).load(KEY, owner());
        assertThat(restored).singleElement().satisfies(value -> {
            assertThat(value.registration()).isEqualTo(original);
            assertThat(value.finalization()).isEmpty();
        });
        assertThatThrownBy(() -> exchanges.finalizeExchange(KEY, firstOwner, proof(original)))
                .isInstanceOf(ArtifactExecutionFence.LostOwnerException.class);
        exchanges.finalizeExchange(KEY, owner(), proof(original));
        assertThat(new JdbcArtifactStageExchangeStore(dataSource).load(KEY, owner()))
                .singleElement().satisfies(value -> assertThat(value.finalization()).isPresent());
    }

    @Test
    void changedAllocationAndSnapshotAreRejectedWithoutReplacingOriginalEvidence() {
        var original = registration();
        exchanges.register(KEY, owner(), original);
        var changed = new ArtifactExchangeRegistration(original.exchangeId(), original.executionId(), "other-shuffle",
                original.producerStageId(), original.planEpoch(), original.graphSha256(), original.stagePlanSha256(),
                original.sourceSnapshot(), original.producerCount(), original.producerSetSha256(), original.partitionCount(),
                original.endpoints(), original.commitAddress(), owner());
        assertThatThrownBy(() -> exchanges.register(KEY, owner(), changed))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("allocation changed");
        assertThat(exchanges.load(KEY, owner())).singleElement()
                .satisfies(value -> assertThat(value.registration()).isEqualTo(original));
    }

    @Test
    void preparesOnlyExactPartitionWithCurrentPrincipalAndRealIssuedOwner() {
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        var prepared = stages.preparePartition(partitionClaim(1, 0, 0), value -> {
            assertThat(value.authority().dispatchSpec().principal()).isEqualTo("stage-principal");
            assertThat(value.authority().executionAnchor()).isEqualTo(anchor);
            assertThat(value.authority().currentDirectRoles()).isNotEmpty();
            assertThat(value.job().jobId()).isEqualTo(KEY.jobId());
            return value.lease();
        });
        assertThat(prepared).hasValueSatisfying(lease -> {
            assertThat(lease.pageId()).isEqualTo("producer-0");
            assertThat(lease.executionAnchor()).isEqualTo(anchor);
            assertThat(lease.ownerEpoch()).isEqualTo(8);
        });
        assertThat(stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease())).isEmpty();
    }

    @Test
    void preparationFailureRollsBackAttemptAndLeaseRatherThanStrandingAClaim() {
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 0), value -> {
            throw new IllegalStateException("local canonical binding failed");
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical binding failed");
        var actual = stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThat(actual.attemptId()).isZero();
        assertThat(actual.pageId()).isEqualTo("producer-0");
    }

    @Test
    void exactConsumerPreparationCannotUseAnUnfinalizedMetadataBarrier() {
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        var callbacks = new java.util.concurrent.atomic.AtomicInteger();
        assertThat(stages.preparePartition(partitionClaim(0, 0, 1), value -> callbacks.incrementAndGet())).isEmpty();
        completeProducer();
        assertThat(stages.preparePartition(partitionClaim(0, 0, 1), value -> callbacks.incrementAndGet())).isEmpty();
        var allocation = registration();
        exchanges.register(KEY, owner(), allocation);
        exchanges.finalizeExchange(KEY, owner(), proof(allocation));
        assertThat(stages.preparePartition(partitionClaim(0, 0, 1), value -> callbacks.incrementAndGet())).hasValue(1);
    }

    @Test
    void exactPartitionRejectsMissingCoordinatesAndForeignExecution() {
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 99), value -> value.lease()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exact immutable page");
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 1, 0), value -> value.lease()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("immutable stage cohort");
        assertThatThrownBy(() -> new JdbcArtifactStageExecutionStore.PartitionClaim(
                new ArtifactBuildJobKey(KEY.tenantId(), "other-job"), anchor, 1, 0, 0, 8, Duration.ofMinutes(1), NOW))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another durable execution");
    }

    @Test
    void exactPartitionPreparationRejectsOldOwnerAfterRealBackendTakeover() {
        var original = partitionClaim(1, 0, 0);
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(original, value -> value.lease()))
                .isInstanceOf(ArtifactExecutionFence.LostOwnerException.class);
        var replacement = new JdbcArtifactStageExecutionStore.PartitionClaim(
                KEY, anchor, 1, 0, 0, 9, Duration.ofMinutes(1), NOW);
        assertThat(stages.preparePartition(replacement, value -> value.lease()))
                .hasValueSatisfying(lease -> assertThat(lease.executionAnchor()).isEqualTo(anchor));
    }

    @Test
    void exactProducerCompletionStoresOriginalOpaqueNativeReportBeforeTheFinalizeBarrier() throws Exception {
        exchanges.register(KEY, owner(), registration());
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        var lease = stages.preparePartition(partitionClaim(1, 0, 0), prepared -> {
            assertThat(prepared.exchanges()).singleElement()
                    .satisfies(value -> assertThat(value.registration()).isEqualTo(registration()));
            return prepared.lease();
        }).orElseThrow();
        byte[] original = new byte[]{8, 1, 16, 4};
        var report = producerCompletion(lease, 7, original);
        assertThat(stages.completeProducer(report)).isTrue();
        assertThat(stages.completeProducer(report)).isTrue();
        assertThat(stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1}))).isFalse();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT report_bytes,report_sha256,state FROM artifact_build_pages "
                     + "WHERE tenant_id='stage-tenant' AND job_id='stage-job' AND page_id='producer-0'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getBytes(1)).isEqualTo(original);
            assertThat(rows.getString(2)).isEqualTo(DigestUtils.sha256Hex(original));
            assertThat(rows.getString(3)).isEqualTo("SUCCEEDED");
        }
        assertThat(stages.preparePartition(partitionClaim(0, 0, 1), value -> value.lease())).isEmpty();
        exchanges.finalizeExchange(KEY, owner(), proof(registration()));
        assertThat(stages.preparePartition(partitionClaim(0, 0, 1), value -> value.exchanges()))
                .hasValueSatisfying(values -> assertThat(values).singleElement()
                        .satisfies(value -> assertThat(value.finalization()).isPresent()));
    }

    @Test
    void producerCompletionRejectsAbsentAllocationWrongPlanEpochAndStaleIssuedOwner() {
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        var lease = stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1})))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("original external allocation");
        exchanges.register(KEY, owner(), registration());
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 8, new byte[]{8, 1})))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("original external allocation");
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1})))
                .isInstanceOf(ArtifactExecutionFence.LostOwnerException.class);
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId())).hasValueSatisfying(job ->
                assertThat(job.completedPages()).isZero());
    }

    private static JdbcArtifactStageExecutionStore.ProducerCompletion producerCompletion(
            org.pangrova.metadata.artifact.build.ArtifactBuildLease lease, long epoch, byte[] report) {
        return new JdbcArtifactStageExecutionStore.ProducerCompletion(KEY,
                ArtifactExecutionOwner.from(lease.executionAnchor()), lease.stageId(), lease.partitionId(), epoch,
                lease.attemptId(), lease.fencingToken(), lease.ownerEpoch(), report, NOW.plusSeconds(1));
    }

    @Test
    void obsoleteGraphFailsClosedAndLeavesActualJobsPagesAndAttemptsRecoverable() throws Exception {
        byte[] obsolete = "{\"purpose\":\"RELATIONAL_EXPORT\",\"stages\":[],\"exchanges\":[]}"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE artifact_build_dispatch_specs SET stage_graph_bytes=?,stage_graph_sha256=? "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setBytes(1, obsolete);
            statement.setString(2, DigestUtils.sha256Hex(obsolete));
            statement.setString(3, KEY.tenantId());
            statement.setString(4, KEY.jobId());
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
        var stages = new JdbcArtifactStageExecutionStore(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()))
                .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class)
                .hasMessageContaining("securely resubmit");
        assertThatThrownBy(() -> builds.findDispatchSpec(KEY.tenantId(), KEY.jobId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid")
                .hasRootCauseMessage("Durable graph requires format 2; explicitly rebuild old jobs");
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId())).isPresent();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT state,attempt_id FROM artifact_build_pages "
                     + "WHERE tenant_id='stage-tenant' AND job_id='stage-job' ORDER BY page_ordinal")) {
            int pages = 0;
            while (result.next()) {
                assertThat(result.getString(1)).isEqualTo("PENDING");
                assertThat(result.getInt(2)).isEqualTo(-1);
                pages++;
            }
            assertThat(pages).isEqualTo(2);
        }
    }

    private JdbcArtifactStageExecutionStore.PartitionClaim partitionClaim(int stage, int partition, int ordinal) {
        return new JdbcArtifactStageExecutionStore.PartitionClaim(
                KEY, anchor, stage, partition, ordinal, 8, Duration.ofMinutes(1), NOW);
    }

    private void completeProducer() {
        var lease = builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW).orElseThrow();
        byte[] report = "validated-producer-task-report".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(builds.completePage(new ArtifactPageCompletion(KEY.tenantId(), KEY.jobId(), lease.pageId(),
                lease.fencingToken(), lease.ownerEpoch(), report, DigestUtils.sha256Hex(report), List.of(), NOW, owner())))
                .isTrue();
    }

    @Test
    void missingEdgeCannotMakeAConsumerRunnableWithoutItsFinalizedBarrier() throws Exception {
        completeProducer();
        changeEdges("DELETE FROM artifact_stage_exchanges WHERE tenant_id='stage-tenant' AND job_id='stage-job'");
        assertGraphCorruptionRejected("missing");
    }

    @Test
    void unexpectedEdgeCannotChangeThePersistedConsumerGate() throws Exception {
        changeEdges("INSERT INTO artifact_stage_exchanges VALUES ('stage-tenant','stage-job','unsubmitted-edge',"
                + "1,0,'" + spec.stageGraph().sha256() + "')");
        assertGraphCorruptionRejected("differ");
    }

    @Test
    void changedGraphHashCannotReuseAnOtherwiseMatchingAllocation() throws Exception {
        changeEdges("UPDATE artifact_stage_exchanges SET graph_sha256=repeat('f',64) "
                + "WHERE tenant_id='stage-tenant' AND job_id='stage-job'");
        assertGraphCorruptionRejected("differ");
    }

    private void changeEdges(String sql) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private void assertGraphCorruptionRejected(String message) {
        assertThatThrownBy(() -> builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining(message);
        assertThatThrownBy(() -> exchanges.load(KEY, owner())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(message);
        assertThatThrownBy(() -> exchanges.register(KEY, owner(), registration())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(message);
    }

    private ArtifactExecutionOwner owner() { return ArtifactExecutionOwner.from(anchor); }

    private ArtifactExchangeRegistration registration() {
        var address = new ArtifactExchangeRegistration.Address("shuffle.internal", 9080, "shuffle.internal");
        return new ArtifactExchangeRegistration("exchange-1-0", KEY.executionId(), "actual-allocation", 1, 7,
                spec.stageGraph().sha256(), spec.stageGraph().requireStage(1).planSha256(), "41", 1, "c".repeat(64), 1,
                List.of(new ArtifactExchangeRegistration.Endpoint(address, 0, 1)), address, owner());
    }

    private ArtifactExchangeFinalization proof(ArtifactExchangeRegistration allocation) {
        // Metadata is wire-neutral. RPC authenticity/cohort decoding is exercised in Coordinator boundary tests.
        byte[] response = new byte[]{8, 1};
        return new ArtifactExchangeFinalization(allocation.exchangeId(), allocation.sha256(), allocation.shuffleId(),
                allocation.planEpoch(), owner(), 1, allocation.producerSetSha256(),
                List.of(new ArtifactExchangeFinalization.Partition(0, 0, 0, -1, "d".repeat(64))), 0, 0,
                Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
    }

    private static ArtifactBuildDispatchSpec specification() {
        var graph = new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION, ArtifactStageGraph.Purpose.RELATIONAL_EXPORT, List.of(
                ArtifactStageGraph.StagePlan.of(1, 1, ArtifactBuildPageKind.RELATIONAL_COMPUTE,
                        new byte[]{1}, ArtifactStageTemplate.of(new byte[0], List.of(), List.of()), List.of("lake.raw.items"), List.of("id"), false),
                ArtifactStageGraph.StagePlan.of(0, 1, ArtifactBuildPageKind.RELATIONAL_PARTITION,
                        new byte[]{2}, ArtifactStageTemplate.of(new byte[]{3}, List.of(), List.of()), List.of(), List.of("id"), true)), List.of(
                            new ArtifactStageGraph.Exchange("exchange-1-0", 1, 0,
                                    ArtifactStageGraph.Exchange.Distribution.SINGLETON,
                                    List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true)), List.of());
        return ArtifactBuildDispatchSpec.of(KEY.jobId(), graph, KEY.tenantId(), "stage-principal",
                org.pangrova.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "raw", "items"), Set.of("id"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "features", "output")),
                new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static ArtifactBuildJob job() {
        return new ArtifactBuildJob(KEY.tenantId(), KEY.jobId(), "feature-view/output", ArtifactTypeIds.TABULAR_EXPORT,
                "g1", "lake.raw.items", "41", ArtifactExecutionKind.EXPORT,
                ArtifactDependencySet.sourceSnapshot("lake.raw.items", "41"), ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/stage-job", "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "native", "", KEY.jobId(), 2, 0, ArtifactBuildJobState.QUEUED, 0, NOW, NOW, NOW);
    }
}
