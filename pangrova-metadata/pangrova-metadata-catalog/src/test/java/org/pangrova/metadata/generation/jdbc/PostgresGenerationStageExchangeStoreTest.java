package org.pangrova.metadata.generation.jdbc;

import org.pangrova.generation.api.enums.GenerationExchangeDistribution;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.assertFailure;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.failure;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.rollback;
import static org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions.snapshot;

import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore;
import org.pangrova.generation.api.exception.GenerationExecutionOwnerLostException;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec;
import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.testinfra.GenerationExecutionFixtures;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresGenerationStageExchangeStoreTest {
    private static final GenerationJobKey KEY = new GenerationJobKey("stage-tenant", "stage-job");
    private static final Instant NOW = Instant.parse("2026-10-01T00:00:00Z");
    private DataSource dataSource;
    private PostgresGenerationJobStore builds;
    private GenerationStageExchangeStore exchanges;
    private GenerationExecutionFixtures owners;
    private ExecutionAnchor anchor;
    private GenerationDispatchSpec spec;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("durable_stage_barrier");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        builds = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        exchanges = GenerationAuthorityFixtures.artifacts().exchanges(dataSource);
        owners = new GenerationExecutionFixtures(dataSource);
        spec = specification();
        GenerationAuthorityFixtures.provision(dataSource, KEY.tenantId(), spec.principal(), spec.authorizationScope());
        builds.createJob(job(), spec, List.of(
                new GenerationPage(KEY.jobId(), "producer-0", 1, 0, 0, GenerationPageKind.RELATIONAL_COMPUTE,
                        List.of(), List.of(), 3),
                new GenerationPage(KEY.jobId(), "terminal-0", 0, 0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                        List.of(), List.of("producer-0"), 3)));
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-a", 8);
    }

    @ParameterizedTest(name = "exchange register: primary={0}, rollback={1}")
    @MethodSource("org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions#failures")
    void failedRegistrationCannotLeaveAllocationAndPreservesPrimary(String kind, String rollbackKind) throws Exception {
        var allocation = registration();
        var before = snapshot(dataSource);
        Throwable primary = failure(kind);
        Throwable rollback = rollback(rollbackKind, primary);
        var faulty = JdbcMutationFault.afterMutation(dataSource, "INSERT INTO artifact_exchange_registrations ", primary, rollback);
        assertFailure(catchThrowable(() -> GenerationAuthorityFixtures.artifacts().exchanges(faulty)
                .register(KEY, owner(), allocation)), primary, rollback);
        assertThat(snapshot(dataSource)).isEqualTo(before);
        exchanges.register(KEY, owner(), allocation);
        assertThat(exchanges.load(KEY, owner())).singleElement().satisfies(evidence -> {
            assertThat(evidence.registration()).isEqualTo(allocation);
            assertThat(evidence.finalization()).isEmpty();
        });
    }

    @ParameterizedTest(name = "exchange finalize: primary={0}, rollback={1}")
    @MethodSource("org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions#failures")
    void failedFinalizationCannotOpenConsumerBarrierAndPreservesPrimary(String kind, String rollbackKind) throws Exception {
        var allocation = registration();
        exchanges.register(KEY, owner(), allocation);
        completeProducer();
        var finalization = proof(allocation);
        var before = snapshot(dataSource);
        Throwable primary = failure(kind);
        Throwable rollback = rollback(rollbackKind, primary);
        var faulty = JdbcMutationFault.afterMutation(dataSource, "INSERT INTO artifact_exchange_finalizations ", primary, rollback);
        assertFailure(catchThrowable(() -> GenerationAuthorityFixtures.artifacts().exchanges(faulty)
                .finalizeExchange(KEY, owner(), finalization)), primary, rollback);
        assertThat(snapshot(dataSource)).isEqualTo(before);
        assertThat(builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW)).isEmpty();
        exchanges.finalizeExchange(KEY, owner(), finalization);
        assertThat(exchanges.load(KEY, owner())).singleElement()
                .satisfies(evidence -> assertThat(evidence.finalization()).contains(finalization));
        assertThat(builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW))
                .hasValueSatisfying(lease -> assertThat(lease.pageId()).isEqualTo("terminal-0"));
    }

    @ParameterizedTest(name = "partition claim: primary={0}, rollback={1}")
    @MethodSource("org.pangrova.metadata.generation.jdbc.GenerationTransactionFailureAssertions#failures")
    void failedPartitionClaimRestoresAttemptAndPreservesPrimary(String kind, String rollbackKind) throws Exception {
        var before = snapshot(dataSource);
        Throwable primary = failure(kind);
        Throwable rollback = rollback(rollbackKind, primary);
        var faulty = JdbcMutationFault.afterMutation(dataSource, "UPDATE artifact_build_pages SET state='LEASED'", primary, rollback);
        assertFailure(catchThrowable(() -> GenerationAuthorityFixtures.artifacts().stages(faulty)
                .preparePartition(partitionClaim(1, 0, 0), value -> value.lease())), primary, rollback);
        assertThat(snapshot(dataSource)).isEqualTo(before);
        var lease = GenerationAuthorityFixtures.artifacts().stages(dataSource)
                .preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThat(lease.attemptId()).isZero();
        assertThat(lease.pageId()).isEqualTo("producer-0");
        assertThat(lease.executionAnchor()).isEqualTo(anchor);
    }

    @Test
    void restoresAllExchangesOnceButClaimsWithOnlyTheStageEvidence() throws Exception {
        var key = new GenerationJobKey(KEY.tenantId(), "many-exchanges");
        var stages = new ArrayList<GenerationStageGraph.StagePlan>();
        var edges = new ArrayList<GenerationStageGraph.Exchange>();
        var pages = new ArrayList<GenerationPage>();
        var producers = new ArrayList<String>();
        var compute = spec.stageGraph().requireStage(1);
        stages.add(spec.stageGraph().requireStage(0));
        for (int stage = 1; stage <= 9; stage++) {
            stages.add(GenerationStageGraph.StagePlan.of(stage, 1, compute.pageKind(), compute.planBytes(),
                    compute.template(), compute.sourceKeys(), compute.outputNames(), false));
            edges.add(new GenerationStageGraph.Exchange("exchange-" + stage + "-0", stage, 0,
                    GenerationExchangeDistribution.SINGLETON, List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true));
            String page = "producer-" + stage;
            producers.add(page);
            pages.add(new GenerationPage(key.jobId(), page, stage, 0, stage - 1,
                    GenerationPageKind.RELATIONAL_COMPUTE, List.of(), List.of(), 3));
        }
        pages.add(new GenerationPage(key.jobId(), "terminal-0", 0, 0, producers.size(),
                GenerationPageKind.RELATIONAL_PARTITION, List.of(), producers, 3));
        var graph = new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                GenerationGraphPurpose.RELATIONAL_EXPORT, stages, edges, List.of());
        var work = GenerationDispatchSpecFactory.create(key.jobId(), graph, key.tenantId(), spec.principal(),
                spec.authorizationScope(), new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        builds.createJob(job(key, pages.size()), work, pages);
        var graphOwner = owners.takeOwnership(key.tenantId(), key.jobId(), "process-a", 8);
        var owner = GenerationExecutionOwner.from(graphOwner);
        String graphSha = GenerationStageGraphCodec.sha256(graph);
        var expected = new ArrayList<GenerationExchangeRegistration>();
        for (var edge : graph.exchanges()) {
            var allocation = new GenerationExchangeRegistration(edge.exchangeId(), key.executionId(),
                    "allocation-" + edge.upstreamStageId(), edge.upstreamStageId(), 7, graphSha,
                    graph.requireStage(edge.upstreamStageId()).planSha256(), "41", 1, "c".repeat(64), 1,
                    ShuffleProviderDescriptor.of("test", 1, new byte[]{1, 2, 3}), owner);
            exchanges.register(key, owner, allocation);
            expected.add(allocation);
        }

        var loaded = exchanges.load(key, owner);
        assertThat(loaded).extracting(GenerationStageExchangeStore.ExchangeEvidence::registration)
                .containsExactlyElementsOf(expected);
        assertThat(loaded).allSatisfy(item -> assertThat(item.finalization()).isEmpty());
        assertThat(exchanges.load(KEY, owner())).isEmpty();

        try (var connection = dataSource.getConnection(); var corrupt = connection.prepareStatement(
                "UPDATE artifact_exchange_registrations SET registration_bytes = decode('ff', 'hex') "
                        + "WHERE tenant_id = ? AND job_id = ? AND exchange_id = ?")) {
            corrupt.setString(1, key.tenantId());
            corrupt.setString(2, key.jobId());
            corrupt.setString(3, "exchange-9-0");
            assertThat(corrupt.executeUpdate()).isOne();
        }
        var stageStore = GenerationAuthorityFixtures.artifacts().stages(dataSource);
        var first = new GenerationStageExecutionStore.PartitionClaim(
                key, graphOwner, 1, 0, 0, 8, Duration.ofMinutes(1), NOW);
        var ninth = new GenerationStageExecutionStore.PartitionClaim(
                key, graphOwner, 9, 0, 8, 8, Duration.ofMinutes(1), NOW);
        assertThat(stageStore.preparePartition(first, prepared -> prepared.exchanges()))
                .hasValueSatisfying(evidence -> assertThat(evidence)
                        .extracting(GenerationStageExchangeStore.ExchangeEvidence::registration)
                        .containsExactly(expected.get(0)));
        assertThatThrownBy(() -> stageStore.preparePartition(ninth, prepared -> prepared.lease()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
        assertThatThrownBy(() -> stageStore.prepareExecution(key, graphOwner))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
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
        var restored = GenerationAuthorityFixtures.artifacts().exchanges(dataSource).load(KEY, owner());
        assertThat(restored).singleElement().satisfies(value -> {
            assertThat(value.registration()).isEqualTo(original);
            assertThat(value.finalization()).isEmpty();
        });
        assertThatThrownBy(() -> exchanges.finalizeExchange(KEY, firstOwner, proof(original)))
                .isInstanceOf(GenerationExecutionOwnerLostException.class);
        exchanges.finalizeExchange(KEY, owner(), proof(original));
        assertThat(GenerationAuthorityFixtures.artifacts().exchanges(dataSource).load(KEY, owner()))
                .singleElement().satisfies(value -> assertThat(value.finalization()).isPresent());
    }

    @Test
    void changedAllocationAndSnapshotAreRejectedWithoutReplacingOriginalEvidence() {
        var original = registration();
        exchanges.register(KEY, owner(), original);
        for (var descriptor : List.of(ShuffleProviderDescriptor.of("other", 1, original.descriptor().payload()),
                ShuffleProviderDescriptor.of("test", 2, original.descriptor().payload()),
                ShuffleProviderDescriptor.of("test", 1, new byte[]{42}))) {
            var changedDescriptor = new GenerationExchangeRegistration(original.exchangeId(), original.executionId(),
                    original.shuffleId(), original.producerStageId(), original.planEpoch(), original.graphSha256(),
                    original.stagePlanSha256(), original.sourceSnapshot(), original.producerCount(),
                    original.producerSetSha256(), original.partitionCount(), descriptor, owner());
            assertThatThrownBy(() -> exchanges.register(KEY, owner(), changedDescriptor))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("allocation changed");
        }
        var changed = new GenerationExchangeRegistration(original.exchangeId(), original.executionId(), "other-shuffle",
                original.producerStageId(), original.planEpoch(), original.graphSha256(), original.stagePlanSha256(),
                original.sourceSnapshot(), original.producerCount(), original.producerSetSha256(), original.partitionCount(),
                original.descriptor(), owner());
        assertThatThrownBy(() -> exchanges.register(KEY, owner(), changed))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("allocation changed");
        assertThat(exchanges.load(KEY, owner())).singleElement()
                .satisfies(value -> assertThat(value.registration()).isEqualTo(original));
    }

    @Test
    void preparesOnlyExactPartitionWithCurrentPrincipalAndRealIssuedOwner() {
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
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
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 0), value -> {
            throw new IllegalStateException("local canonical binding failed");
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical binding failed");
        var actual = stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThat(actual.attemptId()).isZero();
        assertThat(actual.pageId()).isEqualTo("producer-0");
    }

    @Test
    void exactConsumerPreparationCannotUseAnUnfinalizedMetadataBarrier() {
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
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
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 99), value -> value.lease()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exact immutable page");
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 1, 0), value -> value.lease()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("immutable stage cohort");
        assertThatThrownBy(() -> new GenerationStageExecutionStore.PartitionClaim(
                new GenerationJobKey(KEY.tenantId(), "other-job"), anchor, 1, 0, 0, 8, Duration.ofMinutes(1), NOW))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("another durable execution");
    }

    @Test
    void exactPartitionPreparationRejectsOldOwnerAfterRealBackendTakeover() {
        var original = partitionClaim(1, 0, 0);
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(original, value -> value.lease()))
                .isInstanceOf(GenerationExecutionOwnerLostException.class);
        var replacement = new GenerationStageExecutionStore.PartitionClaim(
                KEY, anchor, 1, 0, 0, 9, Duration.ofMinutes(1), NOW);
        assertThat(stages.preparePartition(replacement, value -> value.lease()))
                .hasValueSatisfying(lease -> assertThat(lease.executionAnchor()).isEqualTo(anchor));
    }

    @Test
    void exactProducerCompletionStoresOriginalOpaqueNativeReportBeforeTheFinalizeBarrier() throws Exception {
        exchanges.register(KEY, owner(), registration());
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
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
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
        var lease = stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()).orElseThrow();
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1})))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("original external allocation");
        exchanges.register(KEY, owner(), registration());
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 8, new byte[]{8, 1})))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("original external allocation");
        anchor = owners.takeOwnership(KEY.tenantId(), KEY.jobId(), "process-b", 9);
        assertThatThrownBy(() -> stages.completeProducer(producerCompletion(lease, 7, new byte[]{8, 1})))
                .isInstanceOf(GenerationExecutionOwnerLostException.class);
        assertThat(builds.findJob(KEY.tenantId(), KEY.jobId())).hasValueSatisfying(job ->
                assertThat(job.completedPages()).isZero());
    }

    private static GenerationStageExecutionStore.ProducerCompletion producerCompletion(
            org.pangrova.generation.api.page.GenerationLease lease, long epoch, byte[] report) {
        return new GenerationStageExecutionStore.ProducerCompletion(KEY,
                GenerationExecutionOwner.from(lease.executionAnchor()), lease.stageId(), lease.partitionId(), epoch,
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
        var stages = GenerationAuthorityFixtures.artifacts().stages(dataSource);
        assertThatThrownBy(() -> stages.preparePartition(partitionClaim(1, 0, 0), value -> value.lease()))
                .isInstanceOf(org.pangrova.security.api.exception.AuthorizationDeniedException.class)
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

    private GenerationStageExecutionStore.PartitionClaim partitionClaim(int stage, int partition, int ordinal) {
        return new GenerationStageExecutionStore.PartitionClaim(
                KEY, anchor, stage, partition, ordinal, 8, Duration.ofMinutes(1), NOW);
    }

    private void completeProducer() {
        var lease = builds.claimNextPage(KEY.tenantId(), KEY.jobId(), anchor, 8, Duration.ofMinutes(1), NOW).orElseThrow();
        byte[] report = "validated-producer-task-report".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(builds.completePage(new GenerationPageCompletion(KEY.tenantId(), KEY.jobId(), lease.pageId(),
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
                + "1,0,'" + GenerationStageGraphCodec.sha256(spec.stageGraph()) + "')");
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
        String expected = GenerationStageGraphCodec.sha256(spec.stageGraph());
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

    private GenerationExecutionOwner owner() { return GenerationExecutionOwner.from(anchor); }

    private GenerationExchangeRegistration registration() {
        return new GenerationExchangeRegistration("exchange-1-0", KEY.executionId(), "actual-allocation", 1, 7,
                GenerationStageGraphCodec.sha256(spec.stageGraph()), spec.stageGraph().requireStage(1).planSha256(), "41", 1, "c".repeat(64), 1,
                ShuffleProviderDescriptor.of("test", 1, new byte[]{1, 2, 3}), owner());
    }

    private GenerationExchangeFinalization proof(GenerationExchangeRegistration allocation) {
        // Metadata is wire-neutral. RPC authenticity/cohort decoding is exercised in Coordinator boundary tests.
        byte[] response = new byte[]{8, 1};
        return new GenerationExchangeFinalization(allocation.exchangeId(), GenerationExchangeEvidenceCodec.sha256(allocation), allocation.shuffleId(),
                allocation.planEpoch(), owner(), 1, allocation.producerSetSha256(),
                List.of(new GenerationExchangeFinalization.Partition(0, 0, 0, -1, "d".repeat(64))), 0, 0,
                Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
    }

    private static GenerationDispatchSpec specification() {
        var graph = new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationGraphPurpose.RELATIONAL_EXPORT, List.of(
                GenerationStageGraph.StagePlan.of(1, 1, GenerationPageKind.RELATIONAL_COMPUTE,
                        new byte[]{1}, GenerationStageTemplate.of(new byte[0], List.of(), List.of()), List.of("lake.raw.items"), List.of("id"), false),
                GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                        new byte[]{2}, GenerationStageTemplate.of(new byte[]{3}, List.of(), List.of()), List.of(), List.of("id"), true)), List.of(
                            new GenerationStageGraph.Exchange("exchange-1-0", 1, 0,
                                    GenerationExchangeDistribution.SINGLETON,
                                    List.of(), 1, "BATCH_SERDE_ARROW_BATCH", true)), List.of());
        return GenerationDispatchSpecFactory.create(KEY.jobId(), graph, KEY.tenantId(), "stage-principal",
                org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "raw", "items"), Set.of("id"),
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "features", "output")),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static GenerationJob job() {
        return job(KEY, 2);
    }

    private static GenerationJob job(GenerationJobKey key, int expectedPages) {
        return new GenerationJob(key.tenantId(), key.jobId(), "feature-view/output", GenerationOutputTypeIds.TABULAR_EXPORT,
                key.equals(KEY) ? "g1" : "g2", "lake.raw.items", "41", GenerationExecutionKind.EXPORT,
                GenerationDependencySet.sourceSnapshot("lake.raw.items", "41"), GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/" + key.jobId(), "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "native", "", key.jobId(), expectedPages, 0, GenerationJobState.QUEUED, 0, NOW, NOW, NOW);
    }
}
