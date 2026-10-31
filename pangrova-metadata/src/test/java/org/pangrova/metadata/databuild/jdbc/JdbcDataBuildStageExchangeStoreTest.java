package org.pangrova.metadata.databuild.jdbc;

import org.pangrova.databuild.spi.storage.DataBuildStageExchangeStore;

import org.pangrova.databuild.api.execution.DataBuildExecutionOwnerLostException;


import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.pangrova.databuild.protocol.DataBuildExchangeEvidenceCodec;

import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;

import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;
import org.pangrova.databuild.spi.storage.DataBuildStageExecutionStore;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.execution.DataBuildExchangeFinalization;
import org.pangrova.databuild.api.execution.DataBuildExchangeRegistration;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;
import org.pangrova.databuild.api.build.DataBuildPageCompletion;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures;
import org.pangrova.metadata.testinfra.DataBuildExecutionFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcDataBuildStageExchangeStoreTest {
    private static final DataBuildJobKey KEY = new DataBuildJobKey("stage-tenant", "stage-job");
    private static final Instant NOW = Instant.parse("2026-10-01T00:00:00Z");
    private DataSource dataSource;
    private JdbcDataBuildStore builds;
    private DataBuildStageExchangeStore exchanges;
    private DataBuildExecutionFixtures owners;
    private ExecutionAnchor anchor;
    private DataBuildDispatchSpec spec;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("durable_stage_barrier");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        builds = DataBuildAuthorityFixtures.artifacts().builds(dataSource);
        exchanges = DataBuildAuthorityFixtures.artifacts().exchanges(dataSource);
        owners = new DataBuildExecutionFixtures(dataSource);
        spec = specification();
        DataBuildAuthorityFixtures.provision(dataSource, KEY.tenantId(), spec.principal(), spec.authorizationScope());
        builds.createJob(job(), spec, List.of(
                new DataBuildPage(KEY.jobId(), "producer-0", 1, 0, 0, DataBuildPageKind.RELATIONAL_COMPUTE,
                        List.of(), List.of(), 3),
                new DataBuildPage(KEY.jobId(), "terminal-0", 0, 0, 1, DataBuildPageKind.RELATIONAL_PARTITION,
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
        var restored = DataBuildAuthorityFixtures.artifacts().exchanges(dataSource).load(KEY, owner());
        assertThat(restored).singleElement().satisfies(value -> {
            assertThat(value.registration()).isEqualTo(original);
            assertThat(value.finalization()).isEmpty();
        });
        assertThatThrownBy(() -> exchanges.finalizeExchange(KEY, firstOwner, proof(original)))
                .isInstanceOf(DataBuildExecutionOwnerLostException.class);
        exchanges.finalizeExchange(KEY, owner(), proof(original));
        assertThat(DataBuildAuthorityFixtures.artifacts().exchanges(dataSource).load(KEY, owner()))
                .singleElement().satisfies(value -> assertThat(value.finalization()).isPresent());
    }

    @Test
    void changedAllocationAndSnapshotAreRejectedWithoutReplacingOriginalEvidence() {
        var original = registration();
        exchanges.register(KEY, owner(), original);
        var changed = new DataBuildExchangeRegistration(original.exchangeId(), original.executionId(), "other-shuffle",
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
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
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
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 0), value -> {
            throw new IllegalStateException("local canonical binding failed");
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical binding failed");
        var actual = stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThat(actual.attemptId()).isZero();
        assertThat(actual.pageId()).isEqualTo("producer-0");
    }

    @Test
    void exactConsumerPreparationCannotUseAnUnfinalizedMetadataBarrier() {
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
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
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 99), value -> value.lease()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exact immutable page");
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 1, 0), value -> value.lease()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("immutable stage cohort");
        assertThatThrownBy(() -> new DataBuildStageExecutionStore.PartitionClaim(
                new DataBuildJobKey(KEY.tenantId(), "other-job"), anchor, 1, 0, 0, 8, Duration.ofMinutes(1), NOW))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another durable execution");
    }

    @Test
    void exactPartitionPreparationRejectsOldOwnerAfterRealBackendTakeover() {
        var original = partitionClaim(1, 0, 0);
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(original, value -> value.lease()))
                .isInstanceOf(DataBuildExecutionOwnerLostException.class);
        var replacement = new DataBuildStageExecutionStore.PartitionClaim(
                KEY, anchor, 1, 0, 0, 9, Duration.ofMinutes(1), NOW);
        assertThat(stages.preparePartition(replacement, value -> value.lease()))
                .hasValueSatisfying(lease -> assertThat(lease.executionAnchor()).isEqualTo(anchor));
    }

    @Test
    void exactProducerCompletionStoresOriginalOpaqueNativeReportBeforeTheFinalizeBarrier() throws Exception {
        exchanges.register(KEY, owner(), registration());
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
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
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
        var lease = stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1})))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("original external allocation");
        exchanges.register(KEY, owner(), registration());
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 8, new byte[]{8, 1})))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("original external allocation");
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1})))
                .isInstanceOf(DataBuildExecutionOwnerLostException.class);
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId())).hasValueSatisfying(job ->
                assertThat(job.completedPages()).isZero());
    }

    private static DataBuildStageExecutionStore.ProducerCompletion producerCompletion(
            org.pangrova.databuild.api.build.DataBuildLease lease, long epoch, byte[] report) {
        return new DataBuildStageExecutionStore.ProducerCompletion(KEY,
                DataBuildExecutionOwner.from(lease.executionAnchor()), lease.stageId(), lease.partitionId(), epoch,
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
        var stages = DataBuildAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()))
                .isInstanceOf(org.pangrova.security.api.AuthorizationDeniedException.class)
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

    private DataBuildStageExecutionStore.PartitionClaim partitionClaim(int stage, int partition, int ordinal) {
        return new DataBuildStageExecutionStore.PartitionClaim(
                KEY, anchor, stage, partition, ordinal, 8, Duration.ofMinutes(1), NOW);
    }

    private void completeProducer() {
        var lease = builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW).orElseThrow();
        byte[] report = "validated-producer-task-report".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(builds.completePage(new DataBuildPageCompletion(KEY.tenantId(), KEY.jobId(), lease.pageId(),
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
                + "1,0,'" + DataBuildStageGraphCodec.sha256(spec.stageGraph()) + "')");
        assertGraphCorruptionRejected("differ");
    }

    @Test
    void changedGraphHashCannotReuseAnOtherwiseMatchingAllocation() throws Exception {
        changeEdges("UPDATE artifact_stage_exchanges SET graph_sha256=repeat('f',64) "
                + "WHERE tenant_id='stage-tenant' AND job_id='stage-job'");
        assertGraphCorruptionRejected("differ");
    }

    @Test
    void publicGraphIdentityStillRequiresExactCanonicalRepresentation() throws Exception {
        String expected = DataBuildStageGraphCodec.sha256(spec.stageGraph());
        for (String invalid : List.of(expected.toUpperCase(java.util.Locale.ROOT), expected.substring(1),
                " " + expected.substring(1), "")) {
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "UPDATE artifact_stage_exchanges SET graph_sha256=? "
                            + "WHERE tenant_id='stage-tenant' AND job_id='stage-job'")) {
                statement.setString(1, invalid);
                statement.executeUpdate();
            }
            assertGraphCorruptionRejected("differ");
        }
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

    private DataBuildExecutionOwner owner() { return DataBuildExecutionOwner.from(anchor); }

    private DataBuildExchangeRegistration registration() {
        var address = new DataBuildExchangeRegistration.Address("shuffle.internal", 9080, "shuffle.internal");
        return new DataBuildExchangeRegistration("exchange-1-0", KEY.executionId(), "actual-allocation", 1, 7,
                DataBuildStageGraphCodec.sha256(spec.stageGraph()), spec.stageGraph().requireStage(1).planSha256(), "41", 1, "c".repeat(64), 1,
                List.of(new DataBuildExchangeRegistration.Endpoint(address, 0, 1)), address, owner());
    }

    private DataBuildExchangeFinalization proof(DataBuildExchangeRegistration allocation) {
        // Metadata is wire-neutral. RPC authenticity/cohort decoding is exercised in Coordinator boundary tests.
        byte[] response = new byte[]{8, 1};
        return new DataBuildExchangeFinalization(allocation.exchangeId(), DataBuildExchangeEvidenceCodec.sha256(allocation), allocation.shuffleId(),
                allocation.planEpoch(), owner(), 1, allocation.producerSetSha256(),
                List.of(new DataBuildExchangeFinalization.Partition(0, 0, 0, -1, "d".repeat(64))), 0, 0,
                Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
    }

    private static DataBuildDispatchSpec specification() {
        var graph = new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION, DataBuildStageGraph.Purpose.RELATIONAL_EXPORT, List.of(
                DataBuildStageGraph.StagePlan.of(1, 1, DataBuildPageKind.RELATIONAL_COMPUTE,
                        new byte[]{1}, DataBuildStageTemplate.of(new byte[0], List.of(), List.of()), List.of("lake.raw.items"), List.of("id"), false),
                DataBuildStageGraph.StagePlan.of(0, 1, DataBuildPageKind.RELATIONAL_PARTITION,
                        new byte[]{2}, DataBuildStageTemplate.of(new byte[]{3}, List.of(), List.of()), List.of(), List.of("id"), true)), List.of(
                            new DataBuildStageGraph.Exchange("exchange-1-0", 1, 0,
                                    DataBuildStageGraph.Exchange.Distribution.SINGLETON,
                                    List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true)), List.of());
        return DataBuildDispatchSpecFactory.create(KEY.jobId(), graph, KEY.tenantId(), "stage-principal",
                org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "raw", "items"), Set.of("id"),
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "features", "output")),
                new DataBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static DataBuildJob job() {
        return new DataBuildJob(KEY.tenantId(), KEY.jobId(), "feature-view/output", DataBuildOutputTypeIds.TABULAR_EXPORT,
                "g1", "lake.raw.items", "41", DataBuildExecutionKind.EXPORT,
                DataBuildDependencySet.sourceSnapshot("lake.raw.items", "41"), DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/stage-job", "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "native", "", KEY.jobId(), 2, 0, DataBuildJobState.QUEUED, 0, NOW, NOW, NOW);
    }
}
