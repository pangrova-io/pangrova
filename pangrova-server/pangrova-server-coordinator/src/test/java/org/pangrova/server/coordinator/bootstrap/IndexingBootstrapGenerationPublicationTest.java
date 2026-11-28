package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.graph.GenerationStageGraph;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.coordinator.generation.DurableGenerationPublisher;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.generation.publication.GenerationPublicationRouter;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class IndexingBootstrapGenerationPublicationTest extends ContainerTestBase {

    private org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    @Test
    void registersEachArtifactFamilyWithItsOwningAdapter() {
        var called = new java.util.ArrayList<String>();
        GenerationPublicationStore indexes = (claim, owner, epoch, now) -> { called.add("indexes"); return null; };
        GenerationPublicationStore values = (claim, owner, epoch, now) -> { called.add("values"); return null; };
        GenerationPublicationStore exports = (claim, owner, epoch, now) -> { called.add("exports"); return null; };
        var router = IndexingBootstrap.publicationRouter(indexes, values, exports);
        for (String type : List.of(GenerationOutputTypeIds.VECTOR_INDEX, GenerationOutputTypeIds.TEXT_INDEX,
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, GenerationOutputTypeIds.TABULAR_EXPORT)) {
            var job = exportJobWithType(type);
            router.publishClaimed(job, "test-owner", 1, job.createdAt());
        }
        assertThat(called).containsExactly("indexes", "indexes", "values", "exports");
    }

    @Test
    void unknownClaimTypeIsRejectedByThePublicationRouter() {
        GenerationPublicationStore unused = (claim, owner, epoch, now) -> {
            throw new AssertionError("invoked wrong publication adapter");
        };
        var router = GenerationPublicationRouter.route(Map.of(GenerationOutputTypeIds.TEXT_INDEX, unused));
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> router.publishClaimed(emptyExportJob(), "owner", 1, Instant.now()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining(GenerationOutputTypeIds.TABULAR_EXPORT);
    }

    @Test
    void productionPublicationLoopRoutesTabularExportToItsRealPostgresPublisher() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("bootstrap_export_publication");
        var scope = DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "events"),
                Set.of("value"), new QualifiedTableName("lake", "features", "event_features"));
        GenerationAuthorityFixtures.provision(dataSource, "export-tenant", "alice", scope);
        var builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource);
        var job = emptyExportJob();
        var spec = GenerationDispatchSpecFactory.create(job.jobId(),
                new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationGraphPurpose.RELATIONAL_EXPORT,
                        List.of(GenerationStageGraph.StagePlan.of(0, 1,
                                org.pangrova.generation.api.enums.GenerationPageKind.RELATIONAL_PARTITION,
                                new byte[] {1}, GenerationStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                job.tenantId(), "alice", scope, new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        builds.createJob(job, spec, List.of(new org.pangrova.generation.api.page.GenerationPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.generation.api.enums.GenerationPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = builds.claimNextPage(job.tenantId(), job.jobId(), executions.takeOwnership(job.tenantId(), job.jobId(), "dispatcher", 1), 1,
                Duration.ofSeconds(30), job.createdAt()).orElseThrow();
        byte[] report = new byte[] {1, 2};
        assertThat(builds.completePage(new org.pangrova.generation.api.page.GenerationPageCompletion(
                job.tenantId(), job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.crypto.Sha256Digests.sha256Hex(report),
                List.of(new org.pangrova.generation.api.output.GenerationOutput("object-terminal-0",
                        job.stagingUri() + "/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                        org.pangrova.generation.api.enums.GenerationOutputScope.WORK_UNIT, 7, 0,
                        "a".repeat(64), "")), Instant.now(), executions.reportedOwner(job.tenantId(), job.jobId(), lease.ownerEpoch())))).isTrue();
        var router = IndexingBootstrap.publicationRouter(
                ModelAuthorityFixtures.artifacts().indexPublications(dataSource),
                ModelAuthorityFixtures.artifacts().generatedPublications(dataSource),
                ModelAuthorityFixtures.artifacts().tabularPublications(dataSource));
        executions.takeOwnership(job.tenantId(), job.jobId(), "coordinator-publication", 1);
        try (var publisher = new DurableGenerationPublisher(builds,
                router, executions.ownership("coordinator-publication"), ignored -> {})) {
            publisher.start();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(builds.findJob(job.tenantId(), job.jobId()).orElseThrow().state())
                            .isEqualTo(GenerationJobState.PUBLISHED));
        }
        assertThat(builds.findPublishedGeneration(job.tenantId(), job.artifactId(),
                job.stagingGeneration(), job.sourceSnapshot())).hasValueSatisfying(manifest -> {
                    assertThat(manifest.artifactType()).isEqualTo(GenerationOutputTypeIds.TABULAR_EXPORT);
                    assertThat(manifest.objectCount()).isEqualTo(1);
                    assertThat(manifest.rowCount()).isZero();
                    assertThat(manifest.manifestSha256()).hasSize(64);
                });
    }

    private static GenerationJob emptyExportJob() {
        return exportJobWithType(GenerationOutputTypeIds.TABULAR_EXPORT);
    }

    private static GenerationJob exportJobWithType(String type) {
        var now = Instant.now();
        return new GenerationJob("export-tenant", "export-job", "feature-view/event_features",
                type, "generation-empty", "lake.raw.events", "snapshot-empty",
                GenerationExecutionKind.EXPORT, GenerationDependencySet.sourceSnapshot("lake.raw.events", "snapshot-empty"),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://warehouse/staging/export-job",
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "native-build", "", "export-job",
                1, 0, GenerationJobState.QUEUED, 0, now, now, now);
    }
}
