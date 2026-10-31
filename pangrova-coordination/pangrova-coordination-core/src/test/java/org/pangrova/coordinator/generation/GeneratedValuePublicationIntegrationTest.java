package org.pangrova.coordinator.generation;


import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.generation.assembly.GenerationDispatchSpecFactory;

import org.pangrova.generation.api.execution.template.GenerationStageTemplate;
import org.pangrova.generation.api.execution.GenerationStageGraph;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.job.GenerationOutput;
import org.pangrova.generation.api.GenerationOutputScope;
import org.pangrova.generation.api.job.GenerationPage;
import org.pangrova.generation.api.GenerationPageKind;
import org.pangrova.generation.api.job.GenerationSplit;
import org.pangrova.generation.api.job.GenerationPageCompletion;
import org.pangrova.generation.api.GenerationOutputTypeIds;
import org.pangrova.metadata.generation.GeneratedValueBuildDescriptor;
import org.pangrova.generation.store.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.generation.spi.storage.GenerationStageExecutionStore;
import org.pangrova.coordinator.generation.template.FrozenStageBindings;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.metadata.generation.jdbc.JdbcGeneratedValuePublicationStore;
import org.pangrova.protocol.generation.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
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

class GeneratedValuePublicationIntegrationTest {

    private org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures executions;

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

    @Test
    void atomicallyPublishesSidecarAndBindsOnlyTheTaskSourceObject() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "generated_value_publication");
        PostgresGenerationJobStore buildStore = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource);
        org.pangrova.coordinator.testinfra.CoordinatorGenerationAuthorityFixtures.provision(
                dataSource, TENANT, "alice", dispatch().authorizationScope());
        JdbcGeneratedValuePublicationStore publicationStore =
                ModelAuthorityFixtures.artifacts().generatedPublications(dataSource);
        publicationStore.register(new GeneratedValueBuildDescriptor(
                TENANT, ARTIFACT_ID, SOURCE_ID,
                "lake", "sales", "documents", "embedding",
                "12".repeat(32), GENERATION, SNAPSHOT, START));

        DataSplit split = DataSplit.newBuilder()
                .setSplitId(org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                        "iceberg", SOURCE_ID, SNAPSHOT, "object-a",
                        "s3://warehouse/object-a.parquet", 0, 0))
                .setConnectorId("iceberg")
                .setSourceId(SOURCE_ID)
                .setSourceSnapshot(SNAPSHOT)
                .setDataObjectId("object-a")
                .setOpaqueLocation("s3://warehouse/object-a.parquet")
                .build();
        publishBuild(buildStore, publicationStore, split);
        assertPublication(buildStore, publicationStore);
        assertTaskBinding(buildStore, publicationStore, split);
        assertClaimConnectionEvidence(dataSource, buildStore, publicationStore, split);
    }

    private void assertClaimConnectionEvidence(javax.sql.DataSource dataSource, PostgresGenerationJobStore store,
            JdbcGeneratedValuePublicationStore publicationStore, DataSplit split) throws Exception {
        String encoded = java.util.Base64.getEncoder().encodeToString(split.toByteArray());
        var read = new GeneratedValueGenerationBindingResolver(publicationStore, store).resolveTemplate(
                TENANT, "scan-claim", SOURCE_ID, "embedding", SNAPSHOT, List.of(encoded)).orElseThrow();
        var original = dispatch();
        var sink = OperatorBindingSet.parseFrom(original.stageGraph().requireStage(0).bindingSetBytes())
                .getBindings(0);
        var output = sink.getOutputArtifact().toBuilder().setArtifactId("consumer-artifact")
                .setGeneration("consumer-generation").setStagingUri("s3://warehouse/consumer-generation").build();
        byte[] plan = original.stageGraph().requireStage(0).planBytes();
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(plan,
                List.of(read, sink.toBuilder().setOutputArtifact(output).build())), List.of());
        var stage = GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.SOURCE_MAP,
                plan, template, List.of(SOURCE_ID), List.of("embedding"), true);
        var graph = new GenerationStageGraph(2, GenerationStageGraph.Purpose.ARTIFACT_GENERATION,
                List.of(stage), List.of(), List.of());
        String consumerJob = "same-connection-consumer";
        var spec = GenerationDispatchSpecFactory.create(consumerJob, graph, TENANT, "alice", original.authorizationScope(),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        store.createJob(consumerJob(consumerJob), spec, List.of(new GenerationPage(consumerJob, "consumer-page",
                0, 0, 0, GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(split.getDataObjectId(), split.toByteArray())), List.of(), 3)));
        var owner = executions.takeOwnership(TENANT, consumerJob, "consumer-owner", 10);
        try (var pool = new com.zaxxer.hikari.HikariDataSource()) {
            pool.setDataSource(dataSource);
            pool.setMaximumPoolSize(1);
            pool.setMinimumIdle(0);
            pool.setConnectionTimeout(500);
            var claimed = ModelAuthorityFixtures.artifacts().stages(pool).preparePartition(
                    new GenerationStageExecutionStore.PartitionClaim(
                            new org.pangrova.generation.api.job.GenerationJobKey(TENANT, consumerJob), owner,
                            0, 0, 0, 10, Duration.ofSeconds(30), START), prepared -> {
                        assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isEqualTo(1);
                        var result = PublishedGenerationBindingResolver.materializePrepared(List.of(read), List.of(encoded),
                                prepared.publications());
                        PublishedGenerationBindingChecksum.verify(result.get(0).getArtifacts(0));
                        assertThat(prepared.publications()).hasSize(1);
                        assertThat(prepared.dependencyOutputs()).isEmpty();
                        return prepared.lease();
                    });
            assertThat(claimed).isPresent();
            assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isZero();
        }
    }

    private static GenerationJob consumerJob(String id) {
        var original = job();
        return new GenerationJob(TENANT, id, "consumer-artifact", original.artifactType(), "consumer-generation",
                SOURCE_ID, SNAPSHOT, original.executionKind(), original.dependencySet(), original.emptyPartitionPolicy(),
                "s3://warehouse/consumer-generation", original.formatId(), original.formatMajor(), original.formatMinor(),
                original.schemaSha256(), original.writerBuildId(), "", id, 1, 0, GenerationJobState.QUEUED,
                0, START, START, START);
    }

    private void publishBuild(PostgresGenerationJobStore buildStore,
                              JdbcGeneratedValuePublicationStore publicationStore, DataSplit split)
            throws Exception {
        GenerationSplit buildSplit = GenerationSplit.of(
                split.getDataObjectId(), split.toByteArray());
        DataSplit second = split.toBuilder().setSplitId(
                        org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                                "iceberg", SOURCE_ID, SNAPSHOT, "object-b",
                                "s3://warehouse/object-b.parquet", 0, 0))
                .setDataObjectId("object-b")
                .setOpaqueLocation("s3://warehouse/object-b.parquet").build();
        GenerationJob job = job();
        buildStore.createJob(job, dispatch(), List.of(new GenerationPage(
                JOB_ID, "page-a", 0, 0, 0, GenerationPageKind.SOURCE_MAP,
                List.of(buildSplit), List.of(), 5), new GenerationPage(
                JOB_ID, "page-b", 0, 1, 1, GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(second.getDataObjectId(), second.toByteArray())), List.of(), 5)));
        for (String suffix : List.of("a", "b")) completeSourcePage(buildStore, suffix);
        assertThatThrownBy(() -> buildStore.claimReadyForPublication(
                "tenant-b", JOB_ID, executions.takeOwnership("tenant-b", JOB_ID, "publisher-b", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(2)))
                .isInstanceOf(org.pangrova.security.api.AuthorizationDeniedException.class)
                .hasMessageContaining("no scoped dispatch specification");
        GenerationJob claim = buildStore.claimReadyForPublication(
                TENANT, JOB_ID, executions.takeOwnership(TENANT, JOB_ID, "publisher-a", 9), 9,
                Duration.ofMinutes(1), START.plusSeconds(2))
                .orElseThrow();
        publicationStore.publishClaimed(claim, "publisher-a", 9, START.plusSeconds(3));
    }

    private void completeSourcePage(PostgresGenerationJobStore buildStore, String suffix) throws Exception {
        var page = buildStore.claimNextPage(
                TENANT, JOB_ID, executions.takeOwnership(TENANT, JOB_ID, "coordinator-a", 7), 7,
                Duration.ofMinutes(1), START)
                .orElseThrow();
        assertThat(page.pageId()).isEqualTo("page-" + suffix);
        byte[] report = ("native-work-report-" + suffix).getBytes(StandardCharsets.UTF_8);
        assertThat(buildStore.completePage(new GenerationPageCompletion(
                TENANT, page.jobId(), page.pageId(),
                page.fencingToken(), page.ownerEpoch(),
                report, sha256(report), outputs(suffix), START.plusSeconds(1),
                executions.reportedOwner(TENANT, page.jobId(), page.ownerEpoch())))).isTrue();
    }

    private static void assertPublication(PostgresGenerationJobStore buildStore,
                                           JdbcGeneratedValuePublicationStore publicationStore) {
        assertThat(buildStore.findJob(TENANT, JOB_ID).orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
        assertThat(publicationStore.findGeneration(
                TENANT, SOURCE_ID, "embedding", SNAPSHOT))
                .hasValueSatisfying(reference -> {
                    assertThat(reference.artifactId()).isEqualTo(ARTIFACT_ID);
                    assertThat(reference.generation()).isEqualTo(GENERATION);
                });
        publicationStore.register(new GeneratedValueBuildDescriptor(
                "tenant-b", ARTIFACT_ID, SOURCE_ID,
                "lake", "sales", "documents", "embedding",
                "12".repeat(32), GENERATION, SNAPSHOT, START));
        assertThat(publicationStore.findGeneration(
                "tenant-b", SOURCE_ID, "embedding", SNAPSHOT)).isEmpty();
    }

    private static void assertTaskBinding(PostgresGenerationJobStore buildStore,
                                          JdbcGeneratedValuePublicationStore publicationStore, DataSplit split) {
        GeneratedValueGenerationBindingResolver resolver =
                new GeneratedValueGenerationBindingResolver(publicationStore, buildStore);
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
        assertThat(template.getResources().getFillKeys()).isEqualTo(manifest.rowCount());
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
        PublishedGenerationBindingChecksum.verify(taskBinding.getArtifacts(0));
        assertThat(taskBinding.getArtifacts(0).getSplitObjectsList())
                .extracting(object -> object.getSourceObjectId()).containsOnly("object-a");
        assertThat(new PublishedGenerationBindingResolver(buildStore).resolveTemplate(
                new PublishedGenerationBindingResolver.GenerationResolutionIntent(TENANT, ARTIFACT_ID,
                        GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, GENERATION, SOURCE_ID, SNAPSHOT,
                        PublishedGenerationBindingResolver.SourceCoverage.COMPLETE_GENERATION),
                List.of(encodedSplit))).isEmpty();
        String changedSource = java.util.Base64.getEncoder().encodeToString(split.toBuilder()
                .setOpaqueLocation("s3://warehouse/replaced-object.parquet")
                .setSplitId(org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                        "iceberg", SOURCE_ID, SNAPSHOT, "object-a",
                        "s3://warehouse/replaced-object.parquet", 0, 0))
                .build().toByteArray());
        assertThatThrownBy(() -> resolver.resolveTemplate(
                TENANT, "changed-scan", SOURCE_ID, "embedding", SNAPSHOT, List.of(changedSource)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exactly cover");
    }

    private static GenerationJob job() {
        var format = BuiltInCapabilityCatalog.generatedValuesArtifactFormat();
        return new GenerationJob(
                TENANT, JOB_ID, ARTIFACT_ID,
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                GENERATION, SOURCE_ID, SNAPSHOT,
                org.pangrova.generation.api.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.GenerationDependencySet.sourceSnapshot(
                        SOURCE_ID, SNAPSHOT),
                org.pangrova.generation.api.GenerationEmptyPartitionPolicy.FORBID,
                "s3://warehouse/system/generated-values-test", format.getFormatId(),
                format.getMajorVersion(), format.getMinorVersion(),
                HexFormat.of().formatHex(format.getSchemaSha256().toByteArray()),
                format.getWriterBuildId(), "", JOB_ID, 2, 0,
                GenerationJobState.QUEUED, 0, START, START, START);
    }

    private static GenerationDispatchSpec dispatch() {
        ArtifactWriteBinding output = ArtifactWriteBinding.newBuilder()
                .setArtifactId(ARTIFACT_ID)
                .setArtifactType(GenerationOutputTypeIds.GENERATED_COLUMN_VALUES)
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
                                        org.pangrova.generation.api.GenerationDependencySet
                                                .sourceSnapshot(SOURCE_ID, SNAPSHOT).sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
        OperatorBinding sink = OperatorBinding.newBuilder()
                .setPlanNodeId("generated-values-sink")
                .setCapability(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef())
                .setOutputArtifact(output)
                .build();
        return GenerationDispatchSpecFactory.create(
                JOB_ID, new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationStageGraph.Purpose.ARTIFACT_GENERATION,
                        List.of(GenerationStageGraph.StagePlan.of(
                                0, 2, GenerationPageKind.SOURCE_MAP, new byte[] {1},
                                GenerationStageTemplate.of(OperatorBindingSet.newBuilder().addBindings(sink).build().toByteArray(), List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "alice",
                org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "sales", "documents"),
                        java.util.Set.of("text"),
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "sales", "documents")),
                new GenerationDispatchSpec.ResourceGrant(1024, 1, 1024, 10_000));
    }

    private static List<GenerationOutput> outputs(String suffix) {
        return List.of(
                new GenerationOutput(
                        "values-" + suffix, "s3://warehouse/values-" + suffix, "GENERATED_VALUES",
                        "object-" + suffix, 0, GenerationOutputScope.SOURCE_OBJECT,
                        1024, 10, CHECKSUM, ""),
                new GenerationOutput(
                        "locators-" + suffix, "s3://warehouse/locators-" + suffix, "ROW_LOCATORS",
                        "object-" + suffix, 1, GenerationOutputScope.SOURCE_OBJECT,
                        256, 10, CHECKSUM, ""));
    }

    private static String sha256(byte[] value) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(value));
    }
}
