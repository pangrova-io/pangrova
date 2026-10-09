package org.pangrova.coordinator.artifact;

import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.metadata.artifact.build.ArtifactPageCompletion;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.GeneratedValueArtifactDescriptor;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore;
import org.pangrova.coordinator.artifact.template.FrozenStageBindings;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.metadata.artifact.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.protocol.ArtifactBindingChecksum;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedValueArtifactPublicationIntegrationTest {

    private org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    private static final Instant START = Instant.parse("2026-09-13T00:00:00Z");
    private static final String SOURCE_ID = "lake.sales.documents";
    private static final String SNAPSHOT = "branch/main@snapshot-41";
    private static final String ARTIFACT_ID = "generated-values-test";
    private static final String GENERATION = "generation-a";
    private static final String JOB_ID = "generated-values-job-a";
    private static final String CHECKSUM = "42".repeat(32);
    private static final String TENANT = "tenant-a";

    @ParameterizedTest
    @CsvSource({"1,2", "10,10"})
    void atomicallyPublishesSidecarAndBindsOnlyTheTaskSourceObject(long firstRows, long secondRows) throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "generated_value_publication");
        JdbcArtifactBuildStore buildStore = new JdbcArtifactBuildStore(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures(dataSource);
        org.pangrova.coordinator.testinfra.CoordinatorArtifactAuthorityFixtures.provision(
                dataSource, TENANT, "alice", dispatch().authorizationScope());
        JdbcGeneratedValueArtifactPublicationStore publicationStore =
                new JdbcGeneratedValueArtifactPublicationStore(dataSource);
        publicationStore.register(new GeneratedValueArtifactDescriptor(
                TENANT, ARTIFACT_ID, SOURCE_ID,
                "lake", "sales", "documents", "embedding",
                "12".repeat(32), GENERATION, SNAPSHOT, START));

        DataSplit split = DataSplit.newBuilder()
                .setSplitId("split-a")
                .setConnectorId("iceberg")
                .setSourceId(SOURCE_ID)
                .setSourceSnapshot(SNAPSHOT)
                .setDataObjectId("object-a")
                .setOpaqueLocation("s3://warehouse/object-a.parquet")
                .build();
        publishBuild(buildStore, publicationStore, split, firstRows, secondRows);
        assertPublication(buildStore, publicationStore);
        assertTaskBinding(buildStore, publicationStore, split, Math.addExact(firstRows, secondRows));
        assertClaimConnectionEvidence(dataSource, buildStore, publicationStore, split);
    }

    private void assertClaimConnectionEvidence(javax.sql.DataSource dataSource, JdbcArtifactBuildStore store,
            JdbcGeneratedValueArtifactPublicationStore publicationStore, DataSplit split) throws Exception {
        String encoded = java.util.Base64.getEncoder().encodeToString(split.toByteArray());
        var read = new GeneratedValueArtifactBindingResolver(publicationStore, store).resolveTemplate(
                TENANT, "scan-claim", SOURCE_ID, "embedding", SNAPSHOT, List.of(encoded)).orElseThrow();
        var original = dispatch();
        var sink = OperatorBindingSet.parseFrom(original.stageGraph().requireStage(0).bindingSetBytes())
                .getBindings(0);
        var output = sink.getOutputArtifact().toBuilder().setArtifactId("consumer-artifact")
                .setGeneration("consumer-generation").setStagingUri("s3://warehouse/consumer-generation").build();
        byte[] plan = original.stageGraph().requireStage(0).planBytes();
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(plan,
                List.of(read, sink.toBuilder().setOutputArtifact(output).build())), List.of());
        var stage = ArtifactStageGraph.StagePlan.of(0, 1, ArtifactBuildPageKind.SOURCE_MAP,
                plan, template, List.of(SOURCE_ID), List.of("embedding"), true);
        var graph = new ArtifactStageGraph(2, ArtifactStageGraph.Purpose.ARTIFACT_GENERATION,
                List.of(stage), List.of(), List.of());
        String consumerJob = "same-connection-consumer";
        var spec = ArtifactBuildDispatchSpec.of(consumerJob, graph, TENANT, "alice", original.authorizationScope(),
                new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        store.createJob(consumerJob(consumerJob), spec, List.of(new ArtifactBuildPage(consumerJob, "consumer-page",
                0, 0, 0, ArtifactBuildPageKind.SOURCE_MAP,
                List.of(ArtifactBuildSplit.of(split.getDataObjectId(), split.toByteArray())), List.of(), 3)));
        var owner = executions.takeOwnership(TENANT, consumerJob, "consumer-owner", 10);
        try (var pool = new com.zaxxer.hikari.HikariDataSource()) {
            pool.setDataSource(dataSource);
            pool.setMaximumPoolSize(1);
            pool.setMinimumIdle(0);
            pool.setConnectionTimeout(500);
            var claimed = new JdbcArtifactStageExecutionStore(pool).preparePartition(
                    new JdbcArtifactStageExecutionStore.PartitionClaim(
                            new org.pangrova.metadata.artifact.build.ArtifactBuildJobKey(TENANT, consumerJob), owner,
                            0, 0, 0, 10, Duration.ofSeconds(30), START), prepared -> {
                        assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isEqualTo(1);
                        var result = PublishedArtifactBindingResolver.materializePrepared(List.of(read), List.of(encoded),
                                prepared.publications());
                        ArtifactBindingChecksum.verify(result.get(0).getArtifacts(0));
                        assertThat(prepared.publications()).hasSize(1);
                        assertThat(prepared.dependencyOutputs()).isEmpty();
                        return prepared.lease();
                    });
            assertThat(claimed).isPresent();
            assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isZero();
        }
    }

    private static ArtifactBuildJob consumerJob(String id) {
        var original = job();
        return new ArtifactBuildJob(TENANT, id, "consumer-artifact", original.artifactType(), "consumer-generation",
                SOURCE_ID, SNAPSHOT, original.executionKind(), original.dependencySet(), original.emptyPartitionPolicy(),
                "s3://warehouse/consumer-generation", original.formatId(), original.formatMajor(), original.formatMinor(),
                original.schemaSha256(), original.writerBuildId(), "", id, 1, 0, ArtifactBuildJobState.QUEUED,
                0, START, START, START);
    }

    private void publishBuild(JdbcArtifactBuildStore buildStore,
                              JdbcGeneratedValueArtifactPublicationStore publicationStore, DataSplit split,
                              long firstRows, long secondRows)
            throws Exception {
        ArtifactBuildSplit buildSplit = ArtifactBuildSplit.of(
                split.getDataObjectId(), split.toByteArray());
        DataSplit second = split.toBuilder().setSplitId("split-b").setDataObjectId("object-b")
                .setOpaqueLocation("s3://warehouse/object-b.parquet").build();
        ArtifactBuildJob job = job();
        buildStore.createJob(job, dispatch(), List.of(new ArtifactBuildPage(
                JOB_ID, "page-a", 0, 0, 0, ArtifactBuildPageKind.SOURCE_MAP,
                List.of(buildSplit), List.of(), 5), new ArtifactBuildPage(
                JOB_ID, "page-b", 0, 1, 1, ArtifactBuildPageKind.SOURCE_MAP,
                List.of(ArtifactBuildSplit.of(second.getDataObjectId(), second.toByteArray())), List.of(), 5)));
        completeSourcePage(buildStore, "a", firstRows);
        completeSourcePage(buildStore, "b", secondRows);
        assertThatThrownBy(() -> buildStore.claimReadyForPublication(
                "tenant-b", JOB_ID, executions.takeOwnership("tenant-b", JOB_ID, "publisher-b", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(2)))
                .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class)
                .hasMessageContaining("no scoped dispatch specification");
        ArtifactBuildJob claim = buildStore.claimReadyForPublication(
                TENANT, JOB_ID, executions.takeOwnership(TENANT, JOB_ID, "publisher-a", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(2))
                .orElseThrow();
        publicationStore.publishClaimed(claim, "publisher-a", 9, START.plusSeconds(3));
    }

    private void completeSourcePage(JdbcArtifactBuildStore buildStore, String suffix, long rows) throws Exception {
        var page = buildStore.claimNextPage(
                TENANT, JOB_ID, executions.takeOwnership(TENANT, JOB_ID, "coordinator-a", 7), 7,
                Duration.ofMinutes(1), START)
                .orElseThrow();
        assertThat(page.pageId()).isEqualTo("page-" + suffix);
        byte[] report = ("native-work-report-" + suffix).getBytes(StandardCharsets.UTF_8);
        assertThat(buildStore.completePage(new ArtifactPageCompletion(
                TENANT, page.jobId(), page.pageId(),
                page.fencingToken(), page.ownerEpoch(),
                report, sha256(report), outputs(suffix, rows), START.plusSeconds(1),
                executions.reportedOwner(TENANT, page.jobId(), page.ownerEpoch())))).isTrue();
    }

    private static void assertPublication(JdbcArtifactBuildStore buildStore,
                                           JdbcGeneratedValueArtifactPublicationStore publicationStore) {
        assertThat(buildStore.findJob(TENANT, JOB_ID).orElseThrow().state())
                .isEqualTo(ArtifactBuildJobState.PUBLISHED);
        assertThat(publicationStore.findGeneration(
                TENANT, SOURCE_ID, "embedding", SNAPSHOT))
                .hasValueSatisfying(reference -> {
                    assertThat(reference.artifactId()).isEqualTo(ARTIFACT_ID);
                    assertThat(reference.generation()).isEqualTo(GENERATION);
                });
        publicationStore.register(new GeneratedValueArtifactDescriptor(
                "tenant-b", ARTIFACT_ID, SOURCE_ID,
                "lake", "sales", "documents", "embedding",
                "12".repeat(32), GENERATION, SNAPSHOT, START));
        assertThat(publicationStore.findGeneration(
                "tenant-b", SOURCE_ID, "embedding", SNAPSHOT)).isEmpty();
    }

    private static void assertTaskBinding(JdbcArtifactBuildStore buildStore,
                                          JdbcGeneratedValueArtifactPublicationStore publicationStore, DataSplit split,
                                          long sourceRows) {
        GeneratedValueArtifactBindingResolver resolver =
                new GeneratedValueArtifactBindingResolver(publicationStore, buildStore);
        String encodedSplit = java.util.Base64.getEncoder()
                .encodeToString(split.toByteArray());
        OperatorBinding template = resolver.resolveTemplate(
                TENANT, "scan-a", SOURCE_ID, "embedding",
                SNAPSHOT, List.of(encodedSplit))
                .orElseThrow();
        assertThat(template.getCapability().getCapabilityId())
                .isEqualTo(BuiltInCapabilityCatalog.GENERATED_VALUES_OVERLAY_ID);
        assertThat(template.getArtifacts(0).getSplitObjectsCount()).isZero();
        var manifest = buildStore.findPublishedGeneration(TENANT, ARTIFACT_ID, GENERATION, SNAPSHOT).orElseThrow();
        assertThat(manifest.objectCount()).isEqualTo(4);
        assertThat(manifest.rowCount()).isEqualTo(sourceRows);
        assertThat(template.getResources().getFillKeys()).isEqualTo(sourceRows);
        assertThat(template.getResources().getObjectStoreReadBytes()).isEqualTo(manifest.totalBytes());
        assertThat(template.getExecutionPolicy().getCoveragePolicy()).isEqualTo(
                org.pangrova.protocol.v2.CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE);
        assertThat(template.getExecutionPolicy().getFallbackPolicy()).isEqualTo(
                org.pangrova.protocol.v2.FallbackPolicy.FALLBACK_POLICY_FORBID);

        OperatorBinding taskBinding = resolver.materializeForTask(
                TENANT, List.of(template), List.of(encodedSplit)).get(0);
        assertThat(taskBinding.getArtifacts(0).getSplitObjectsList())
                .extracting(object -> object.getObjectRole())
                .containsExactly("GENERATED_VALUES", "ROW_LOCATORS");
        ArtifactBindingChecksum.verify(taskBinding.getArtifacts(0));
        assertThat(taskBinding.getArtifacts(0).getSplitObjectsList())
                .extracting(object -> object.getSourceObjectId()).containsOnly("object-a");
        assertThat(new PublishedArtifactBindingResolver(buildStore).resolveTemplate(
                new PublishedArtifactBindingResolver.ArtifactResolutionIntent(TENANT, ARTIFACT_ID,
                        ArtifactTypeIds.GENERATED_COLUMN_VALUES, GENERATION, SOURCE_ID, SNAPSHOT,
                        PublishedArtifactBindingResolver.SourceCoverage.COMPLETE_GENERATION),
                List.of(encodedSplit))).isEmpty();
        String changedSource = java.util.Base64.getEncoder().encodeToString(split.toBuilder()
                .setOpaqueLocation("s3://warehouse/replaced-object.parquet").build().toByteArray());
        assertThatThrownBy(() -> resolver.resolveTemplate(
                TENANT, "changed-scan", SOURCE_ID, "embedding", SNAPSHOT, List.of(changedSource)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exactly cover");
    }

    private static ArtifactBuildJob job() {
        var format = BuiltInCapabilityCatalog.generatedValuesArtifactFormat();
        return new ArtifactBuildJob(
                TENANT, JOB_ID, ARTIFACT_ID,
                ArtifactTypeIds.GENERATED_COLUMN_VALUES,
                GENERATION, SOURCE_ID, SNAPSHOT,
                org.pangrova.metadata.artifact.contract.ArtifactExecutionKind.BUILD,
                org.pangrova.metadata.artifact.contract.ArtifactDependencySet.sourceSnapshot(
                        SOURCE_ID, SNAPSHOT),
                org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy.FORBID,
                "s3://warehouse/system/generated-values-test", format.getFormatId(),
                format.getMajorVersion(), format.getMinorVersion(),
                HexFormat.of().formatHex(format.getSchemaSha256().toByteArray()),
                format.getWriterBuildId(), "", JOB_ID, 2, 0,
                ArtifactBuildJobState.QUEUED, 0, START, START, START);
    }

    private static ArtifactBuildDispatchSpec dispatch() {
        ArtifactWriteBinding output = ArtifactWriteBinding.newBuilder()
                .setArtifactId(ARTIFACT_ID)
                .setArtifactType(ArtifactTypeIds.GENERATED_COLUMN_VALUES)
                .setGeneration(GENERATION)
                .setSourceId(SOURCE_ID)
                .setSourceSnapshot(SNAPSHOT)
                .setStagingUri("s3://warehouse/system/generated-values-test")
                .setFormat(BuiltInCapabilityCatalog.generatedValuesArtifactFormat())
                .setRowLocator(BuiltInCapabilityCatalog.physicalRowLocatorContract())
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_BUILD)
                .addDependencies(org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT").setId(SOURCE_ID).setVersion(SNAPSHOT))
                .setDependencySetChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                HexFormat.of().parseHex(
                                        org.pangrova.metadata.artifact.contract.ArtifactDependencySet
                                                .sourceSnapshot(SOURCE_ID, SNAPSHOT).sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
        OperatorBinding sink = OperatorBinding.newBuilder()
                .setPlanNodeId("generated-values-sink")
                .setCapability(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef())
                .setOutputArtifact(output)
                .build();
        return ArtifactBuildDispatchSpec.of(
                JOB_ID, new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION,
                        ArtifactStageGraph.Purpose.ARTIFACT_GENERATION,
                        List.of(ArtifactStageGraph.StagePlan.of(
                                0, 2, ArtifactBuildPageKind.SOURCE_MAP, new byte[] {1},
                                ArtifactStageTemplate.of(OperatorBindingSet.newBuilder().addBindings(sink).build().toByteArray(), List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "alice",
                org.pangrova.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "sales", "documents"),
                        java.util.Set.of("text"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "sales", "documents")),
                new ArtifactBuildDispatchSpec.ResourceGrant(1024, 1, 1024, 10_000));
    }

    private static List<ArtifactBuildOutput> outputs(String suffix, long rows) {
        return List.of(
                new ArtifactBuildOutput(
                        "values-" + suffix, "s3://warehouse/values-" + suffix, "GENERATED_VALUES",
                        "object-" + suffix, 0, ArtifactBuildOutputScope.SOURCE_OBJECT,
                        1024, rows, CHECKSUM, ""),
                new ArtifactBuildOutput(
                        "locators-" + suffix, "s3://warehouse/locators-" + suffix, "ROW_LOCATORS",
                        "object-" + suffix, 1, ArtifactBuildOutputScope.SOURCE_OBJECT,
                        256, rows, CHECKSUM, ""));
    }

    private static String sha256(byte[] value) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(value));
    }
}
