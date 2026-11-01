package org.pangrova.coordinator.generation;

import org.pangrova.generation.assembly.GenerationDispatchSpecFactory;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalGenerationIntent;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.job.GenerationJob;
import org.pangrova.spi.generation.job.GenerationJobKey;
import org.pangrova.spi.generation.job.GenerationLease;
import org.pangrova.spi.generation.job.GenerationOutput;
import org.pangrova.spi.generation.GenerationOutputScope;
import org.pangrova.spi.generation.job.GenerationPage;
import org.pangrova.persistence.api.generation.job.GenerationJobStore;
import org.pangrova.spi.generation.manifest.GenerationManifest;
import org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore.PublishedInput;
import org.pangrova.protocol.generation.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishedGenerationBindingResolverTest {

    private static final String SHA = "11".repeat(32);
    private static final Instant PUBLISHED_AT = Instant.parse("2026-09-09T00:00:00Z");
    private static final String TENANT = "tenant";

    @Test
    void resolvesAcrossQuerySpecificScanDetailThenLoadsOnlyOneTaskSourceBundle() throws Exception {
        StoreStub store = new StoreStub(outputs("object-a"));
        PublishedGenerationBindingResolver resolver =
                new PublishedGenerationBindingResolver(store);
        String split = splitWithScanDetail("object-a", "snapshot-1");
        assertThat(split).isNotEqualTo(split("object-a", "snapshot-1"));

        var resolved = resolver.resolveTemplate(intent(), List.of(split)).orElseThrow();
        assertThat(resolved.bindingTemplate().getSplitObjectsCount()).isZero();
        assertThat(resolved.sourceObjectCount()).isOne();
        assertThat(resolved.bindingTemplate().hasBindingChecksum()).isFalse();

        OperatorBinding taskBinding = resolver.materializeForTask(
                TENANT,
                List.of(OperatorBinding.newBuilder()
                        .setPlanNodeId("retrieval-1")
                        .setCapability(intent().capability())
                        .addArtifacts(resolved.bindingTemplate())
                        .build()),
                List.of(split)).get(0);
        var artifact = taskBinding.getArtifacts(0);
        assertThat(artifact.getSplitObjectsList())
                .extracting(object -> object.getObjectRole())
                .containsExactly("DISKANN_GRAPH", "RAW_VECTORS", "ROW_LOCATORS");
        assertThat(store.requestedSources).containsExactly("object-a");
        PublishedGenerationBindingChecksum.verify(artifact);
    }

    @Test
    void sourceSnapshotMismatchSelectsExactPlan() {
        PublishedGenerationBindingResolver resolver =
                new PublishedGenerationBindingResolver(new StoreStub(outputs("object-a")));
        assertThat(resolver.resolveTemplate(
                intent(), List.of(split("object-a", "snapshot-2"))))
                .isEmpty();
    }

    @Test
    void incompletePublishedBundleFailsClosedAtTaskMaterialization() {
        StoreStub store = new StoreStub(outputs("object-a").subList(0, 2));
        PublishedGenerationBindingResolver resolver =
                new PublishedGenerationBindingResolver(store);
        String split = split("object-a", "snapshot-1");
        var template = resolver.resolveTemplate(intent(), List.of(split))
                .orElseThrow().bindingTemplate();

        assertThatThrownBy(() -> resolver.materializeForTask(
                TENANT,
                List.of(OperatorBinding.newBuilder()
                        .setPlanNodeId("retrieval-1")
                        .setCapability(intent().capability())
                        .addArtifacts(template)
                        .build()),
                List.of(split)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("roles are incomplete");
    }

    @Test
    void materializesBm25SourceBundleWithOneGenerationStatsObject() {
        StoreStub store = new StoreStub(bm25Outputs("object-a"), true);
        PublishedGenerationBindingResolver resolver =
                new PublishedGenerationBindingResolver(store);
        String split = split("object-a", "snapshot-1");

        var resolved = resolver.resolveTemplate(bm25Intent(), List.of(split)).orElseThrow();
        OperatorBinding taskBinding = resolver.materializeForTask(
                TENANT,
                List.of(OperatorBinding.newBuilder()
                        .setPlanNodeId("retrieval-1")
                        .setCapability(bm25Intent().capability())
                        .addArtifacts(resolved.bindingTemplate())
                        .build()),
                List.of(split)).get(0);

        var objects = taskBinding.getArtifacts(0).getSplitObjectsList();
        assertThat(objects).extracting(object -> object.getObjectRole())
                .containsExactly("BM25_CORPUS_STATS", "BM25_POSTINGS", "ROW_LOCATORS");
        assertThat(objects.get(0).getScope())
                .isEqualTo(org.pangrova.protocol.v2.ArtifactObjectScope
                        .ARTIFACT_OBJECT_SCOPE_GENERATION);
        assertThat(store.requestedSources).containsExactly("object-a");
        PublishedGenerationBindingChecksum.verify(taskBinding.getArtifacts(0));
    }

    @Test
    void purePreparedMaterializationUsesOnlyExactReadEvidenceAndIgnoresOtherSourceLegs() {
        StoreStub store = new StoreStub(outputs("object-a"));
        String encoded = split("object-a", "snapshot-1");
        var artifact = new PublishedGenerationBindingResolver(store).resolveTemplate(intent(), List.of(encoded))
                .orElseThrow().bindingTemplate();
        var binding = OperatorBinding.newBuilder().setPlanNodeId("retrieval-1")
                .setCapability(intent().capability()).addArtifacts(artifact).build();
        var manifest = store.findPublishedGeneration(TENANT, "index-1", "7", "snapshot-1").orElseThrow();
        var evidence = new PublishedInput(manifest, outputs("object-a"), Set.of("object-a"));
        String otherSource = Base64.getEncoder().encodeToString(DataSplit.newBuilder()
                .setSplitId("other-split").setConnectorId("test").setSourceId("catalog.schema.other")
                .setSourceSnapshot("other-snapshot").setDataObjectId("other-object")
                .setOpaqueLocation("s3://bucket/other").build().toByteArray());
        var result = PublishedGenerationBindingResolver.materializePrepared(List.of(binding), List.of(encoded, otherSource),
                Map.of(PublishedGenerationBindingResolver.publicationRead(artifact), evidence));
        PublishedGenerationBindingChecksum.verify(result.get(0).getArtifacts(0));
        assertThat(result.get(0).getArtifacts(0).getSplitObjectsList())
                .extracting(object -> object.getSourceObjectId()).containsOnly("object-a");
        assertThat(store.requestedSources).isEmpty();
    }

    @Test
    void purePreparedMaterializationRejectsMissingEvidenceCoverageAndWrongSnapshot() {
        StoreStub store = new StoreStub(outputs("object-a"));
        String encoded = split("object-a", "snapshot-1");
        var artifact = new PublishedGenerationBindingResolver(store).resolveTemplate(intent(), List.of(encoded))
                .orElseThrow().bindingTemplate();
        var binding = OperatorBinding.newBuilder().setPlanNodeId("retrieval-1").addArtifacts(artifact).build();
        var manifest = store.findPublishedGeneration(TENANT, "index-1", "7", "snapshot-1").orElseThrow();
        var key = PublishedGenerationBindingResolver.publicationRead(artifact);
        assertThatThrownBy(() -> PublishedGenerationBindingResolver.materializePrepared(List.of(binding), List.of(encoded), Map.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("differs");
        assertThatThrownBy(() -> PublishedGenerationBindingResolver.materializePrepared(List.of(binding), List.of(encoded),
                Map.of(key, new PublishedInput(manifest, outputs("object-a"), Set.of("object-b")))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("coverage");
        assertThatThrownBy(() -> PublishedGenerationBindingResolver.materializePrepared(List.of(binding),
                List.of(split("object-a", "snapshot-2")),
                Map.of(key, new PublishedInput(manifest, outputs("object-a"), Set.of("object-a")))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("source snapshot");
    }

    private static RetrievalGenerationIntent intent() {
        return new RetrievalGenerationIntent(
                TENANT, "VECTOR:embedding", "index-1", "VECTOR_INDEX", "7",
                "catalog.schema.table",
                "snapshot-1", BuiltInCapabilityCatalog.diskAnnRetrievalPlanRef(),
                List.of(), 40);
    }

    private static RetrievalGenerationIntent bm25Intent() {
        return new RetrievalGenerationIntent(
                TENANT, "FULL_TEXT:body", "index-1", "TEXT_INDEX", "7",
                "catalog.schema.table",
                "snapshot-1", BuiltInCapabilityCatalog.bm25RetrievalPlanRef(),
                List.of(), 40);
    }

    private static String split(String objectId, String snapshot) {
        String location = "s3://bucket/" + objectId;
        DataSplit value = DataSplit.newBuilder()
                .setSplitId(org.pangrova.protocol.source.DataSplitProtoCodec.stableSplitId(
                        "test", "catalog.schema.table", snapshot, objectId, location, 0, 0))
                .setConnectorId("test")
                .setSourceId("catalog.schema.table")
                .setSourceSnapshot(snapshot)
                .setDataObjectId(objectId)
                .setOpaqueLocation(location)
                .build();
        return Base64.getEncoder().encodeToString(value.toByteArray());
    }

    private static String splitWithScanDetail(String objectId, String snapshot) throws Exception {
        DataSplit base = DataSplit.parseFrom(Base64.getDecoder().decode(split(objectId, snapshot)));
        var detail = org.pangrova.protocol.v2.FileSplitDetail.newBuilder()
                .setFileFormat(org.pangrova.protocol.v2.FileFormat.FILE_FORMAT_PARQUET)
                .setSplitConstraint(org.pangrova.protocol.v2.SplitConstraint.newBuilder()
                        .setPredicate(com.google.protobuf.ByteString.copyFromUtf8("query predicate")))
                .build();
        return Base64.getEncoder().encodeToString(
                org.pangrova.protocol.source.DataSplitProtoCodec.withFileDetail(base, detail).toByteArray());
    }

    private static List<GenerationOutput> outputs(String sourceObjectId) {
        return List.of(
                output(sourceObjectId, "DISKANN_GRAPH", 0),
                output(sourceObjectId, "RAW_VECTORS", 1),
                output(sourceObjectId, "ROW_LOCATORS", 2));
    }

    private static List<GenerationOutput> bm25Outputs(String sourceObjectId) {
        return List.of(
                new GenerationOutput(
                        "stats", "s3://artifacts/index-1/7/stats", "BM25_CORPUS_STATS",
                        "7", 0, GenerationOutputScope.GENERATION,
                        100, 10, SHA, ""),
                output(sourceObjectId, "BM25_POSTINGS", 0),
                output(sourceObjectId, "ROW_LOCATORS", 1));
    }

    private static GenerationOutput output(
            String sourceObjectId, String role, int ordinal) {
        return new GenerationOutput(
                sourceObjectId + "-" + role, "s3://artifacts/" + sourceObjectId
                        + "/" + role, role, sourceObjectId, ordinal,
                GenerationOutputScope.SOURCE_OBJECT, 100, 10, SHA, "");
    }

    private static final class StoreStub implements GenerationJobStore {
        private final List<GenerationOutput> outputs;
        private final boolean bm25;
        private Set<String> requestedSources = Set.of();

        private StoreStub(List<GenerationOutput> outputs) {
            this(outputs, false);
        }

        private StoreStub(List<GenerationOutput> outputs, boolean bm25) {
            this.outputs = List.copyOf(outputs);
            this.bm25 = bm25;
        }

        @Override
        public Optional<GenerationManifest> findPublishedGeneration(
                String tenantId, String artifactId,
                String generation, String sourceSnapshot) {
            if (!tenantId.equals(TENANT) || !artifactId.equals("index-1")
                    || !generation.equals("7")
                    || !sourceSnapshot.equals("snapshot-1")) {
                return Optional.empty();
            }
            return Optional.of(new GenerationManifest(
                    TENANT, "job-1", "index-1",
                    bm25 ? "TEXT_INDEX" : "VECTOR_INDEX", "7",
                    "catalog.schema.table", "snapshot-1",
                    org.pangrova.spi.generation.GenerationExecutionKind.BUILD,
                    org.pangrova.spi.generation.GenerationDependencySet.sourceSnapshot(
                            "catalog.schema.table", "snapshot-1"),
                    org.pangrova.spi.generation.GenerationEmptyPartitionPolicy.FORBID,
                    bm25 ? BuiltInCapabilityCatalog.BM25_ARTIFACT_FORMAT_ID
                            : BuiltInCapabilityCatalog.DISKANN_ARTIFACT_FORMAT_ID,
                    bm25 ? BuiltInCapabilityCatalog.BM25_ARTIFACT_FORMAT_MAJOR
                            : BuiltInCapabilityCatalog.DISKANN_ARTIFACT_FORMAT_MAJOR,
                    0,
                    java.util.HexFormat.of().formatHex(
                            (bm25 ? BuiltInCapabilityCatalog.bm25ArtifactFormat()
                                    : BuiltInCapabilityCatalog.diskAnnArtifactFormat())
                                    .getSchemaSha256().toByteArray()),
                    BuiltInCapabilityCatalog.NATIVE_BUILD_ID,
                    "metadata://artifact-generation/" + SHA, SHA,
                    3, 10, 300, PUBLISHED_AT));
        }

        @Override
        public Optional<GenerationDispatchSpec> findDispatchSpec(
                String tenantId, String jobId) {
            if (!TENANT.equals(tenantId)) {
                return Optional.empty();
            }
            ArtifactWriteBinding output = ArtifactWriteBinding.newBuilder()
                    .setArtifactId("index-1")
                    .setArtifactType(bm25 ? "TEXT_INDEX" : "VECTOR_INDEX")
                    .setGeneration("7")
                    .setSourceId("catalog.schema.table")
                    .setSourceSnapshot("snapshot-1")
                    .setStagingUri("s3://artifacts/index-1/7")
                    .setFormat(bm25 ? BuiltInCapabilityCatalog.bm25ArtifactFormat()
                            : BuiltInCapabilityCatalog.diskAnnArtifactFormat())
                    .setRowLocator(BuiltInCapabilityCatalog.physicalRowLocatorContract())
                    .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                            .ARTIFACT_EXECUTION_KIND_BUILD)
                    .addDependencies(org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                            .setKind("SOURCE_SNAPSHOT")
                            .setId("catalog.schema.table")
                            .setVersion("snapshot-1"))
                    .setDependencySetChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                            .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm
                                    .ALGORITHM_SHA256)
                            .setDigest(com.google.protobuf.ByteString.copyFrom(
                                    java.util.HexFormat.of().parseHex(
                                            org.pangrova.spi.generation.GenerationDependencySet
                                                    .sourceSnapshot("catalog.schema.table",
                                                            "snapshot-1").sha256()))))
                    .setEmptyPartitionPolicy(org.pangrova.protocol.v2
                            .ArtifactEmptyPartitionPolicy
                            .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                    .build();
            OperatorBinding binding = OperatorBinding.newBuilder()
                    .setPlanNodeId("build-1")
                    .setCapability(bm25
                            ? BuiltInCapabilityCatalog.bm25ArtifactBuildPlanRef()
                            : BuiltInCapabilityCatalog.diskAnnArtifactBuildPlanRef())
                    .setOutputArtifact(output)
                    .build();
            return Optional.of(GenerationDispatchSpecFactory.create(
                    jobId, org.pangrova.generation.assembly.GenerationStageGraphFactory.forArtifactPages(
                            new byte[] {1}, OperatorBindingSet.newBuilder().addBindings(binding).build().toByteArray(), List.of(), java.util.List.of()),
                    TENANT, "principal",
                    org.pangrova.spi.security.authz.DeferredAuthorizationScope.raw(
                            new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "table"),
                            java.util.Set.of("embedding"),
                            new org.pangrova.spi.connector.metadata.table.QualifiedTableName("catalog", "schema", "table")),
                    new GenerationDispatchSpec.ResourceGrant(1, 1, 1, 1)));
        }

        @Override
        public List<GenerationOutput> loadPublishedOutputs(
                String tenantId, String jobId, Set<String> sourceObjectIds) {
            requestedSources = Set.copyOf(sourceObjectIds);
            return outputs.stream().filter(output ->
                    output.scope() == GenerationOutputScope.GENERATION
                            || sourceObjectIds.contains(output.sourceObjectId())).toList();
        }

        @Override
        public List<GenerationOutput> loadDependencyOutputs(
                String tenantId, String jobId, List<String> dependencyPageIds) {
            throw unsupported();
        }

        @Override
        public boolean matchesPublishedSplits(
                String tenantId, String jobId,
                java.util.Map<String, String> sourceSplitSha256) {
            return sourceSplitSha256.keySet().equals(Set.of("object-a"));
        }

        @Override
        public java.util.Map<String, org.pangrova.spi.generation.job.GenerationSplit>
                loadPublishedSourceSplits(String tenantId, String jobId,
                        java.util.Set<String> sourceObjectIds) {
            if (!sourceObjectIds.equals(Set.of("object-a"))) return java.util.Map.of();
            byte[] bytes = java.util.Base64.getDecoder().decode(split("object-a", "snapshot-1"));
            return java.util.Map.of("object-a",
                    org.pangrova.spi.generation.job.GenerationSplit.of("object-a", bytes));
        }

        @Override public void createJob(GenerationJob job, GenerationDispatchSpec spec,
                List<GenerationPage> pages) { throw unsupported(); }
        @Override public List<GenerationJobKey> listRunnableJobs(
                int limit, Instant now) { throw unsupported(); }
        @Override public List<GenerationJobKey> listReadyForPublication(
                int limit, Instant now) { throw unsupported(); }
        @Override public Optional<GenerationLease> claimNextPage(
                String tenantId, String jobId, org.pangrova.spi.query.coordination.ExecutionAnchor owner,
                long epoch, Duration duration, Instant now) { throw unsupported(); }
        @Override public boolean renewPage(GenerationLease lease, Duration duration,
                Instant now) { throw unsupported(); }
        @Override public boolean completePage(
                org.pangrova.spi.generation.job.GenerationPageCompletion completion) {
            throw unsupported();
        }
        @Override public boolean retryPage(GenerationLease lease, Instant available,
                String reason, Instant now) { throw unsupported(); }
        @Override public boolean failPage(GenerationLease lease, String reason,
                Instant now) { throw unsupported(); }
        @Override public Optional<GenerationJob> findJob(
                String tenantId, String jobId) { throw unsupported(); }
        @Override public Optional<GenerationJobKey> findExecutionJob(String executionId) { throw unsupported(); }
        @Override public List<GenerationOutput> loadCompleteOutputs(
                String tenantId, String jobId) { throw unsupported(); }
        @Override public Optional<GenerationJob> claimReadyForPublication(
                String tenantId, String jobId,
                org.pangrova.spi.query.coordination.ExecutionAnchor owner, long epoch, Duration duration, Instant now) { throw unsupported(); }
        @Override public boolean deferPublication(
                GenerationJob claim, String owner, long epoch,
                Instant available, String reason, Instant now) { throw unsupported(); }
        @Override public boolean markPublished(
                String tenantId, String jobId, String owner, long epoch,
                long token, Instant now) { throw unsupported(); }
        @Override public int cancelArtifactBuilds(
                String tenantId, String artifactId, Instant now) { throw unsupported(); }
        @Override public boolean cancelJob(
                String tenantId, String jobId, String reason, Instant now) { throw unsupported(); }
        @Override public GenerationDispatchSpec loadAuthorizedDispatchSpec(
                String tenantId, String jobId) { throw unsupported(); }

        private static UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("not used by test");
        }
    }
}
