package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.generation.core.submission.GenerationDispatchSpecFactory;

import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.graph.GenerationStageGraph;

import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.coordinator.generation.DurableGenerationPublisher;
import org.pangrova.coordinator.testinfra.CoordinatorGenerationAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.generation.core.submission.GenerationPublicationFactory;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.execution.GenerationExecutionKind;
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
    void registersEveryDurableArtifactTypeForPublication() {
        GenerationPublicationStore store = (claim, owner, epoch, now) ->
                new GenerationManifest(
                        claim.tenantId(), claim.jobId(), claim.artifactId(), claim.artifactType(),
                        claim.stagingGeneration(), claim.sourceId(), claim.sourceSnapshot(),
                        claim.executionKind(), claim.dependencySet(), claim.emptyPartitionPolicy(),
                        claim.formatId(), claim.formatMajor(), claim.formatMinor(),
                        claim.schemaSha256(), claim.writerBuildId(), claim.stagingUri(),
                        "a".repeat(64), 0, 0, 0, now);

        assertThat(IndexingBootstrap.artifactPublicationStores(store))
                .containsOnlyKeys(
                        GenerationOutputTypeIds.VECTOR_INDEX,
                        GenerationOutputTypeIds.TEXT_INDEX,
                        GenerationOutputTypeIds.TABULAR_EXPORT,
                        GenerationOutputTypeIds.GENERATED_COLUMN_VALUES)
                .allSatisfy((type, registered) -> assertThat(registered).isSameAs(store));
    }

    @Test
    void productionPublicationLoopRoutesTabularExportToItsRealPostgresPublisher() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("bootstrap_export_publication");
        var scope = DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "events"),
                Set.of("value"), new QualifiedTableName("lake", "features", "event_features"));
        CoordinatorGenerationAuthorityFixtures.provision(dataSource, "export-tenant", "alice", scope);
        var builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorGenerationExecutionFixtures(dataSource);
        var job = emptyExportJob();
        var spec = GenerationDispatchSpecFactory.create(job.jobId(),
                new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(GenerationStageGraph.StagePlan.of(0, 1,
                                org.pangrova.generation.api.execution.GenerationPageKind.RELATIONAL_PARTITION,
                                new byte[] {1}, GenerationStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                job.tenantId(), "alice", scope, new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        builds.createJob(job, spec, List.of(new org.pangrova.generation.api.page.GenerationPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.generation.api.execution.GenerationPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = builds.claimNextPage(job.tenantId(), job.jobId(), executions.takeOwnership(job.tenantId(), job.jobId(), "dispatcher", 1), 1,
                Duration.ofSeconds(30), job.createdAt()).orElseThrow();
        byte[] report = new byte[] {1, 2};
        assertThat(builds.completePage(new org.pangrova.generation.api.page.GenerationPageCompletion(
                job.tenantId(), job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report),
                List.of(new org.pangrova.generation.api.output.GenerationOutput("object-terminal-0",
                        job.stagingUri() + "/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                        org.pangrova.generation.api.output.GenerationOutputScope.WORK_UNIT, 7, 0,
                        "a".repeat(64), "")), Instant.now(), executions.reportedOwner(job.tenantId(), job.jobId(), lease.ownerEpoch())))).isTrue();
        var router = GenerationPublicationFactory.route(Map.of(
                GenerationOutputTypeIds.VECTOR_INDEX, ModelAuthorityFixtures.artifacts().indexPublications(dataSource),
                GenerationOutputTypeIds.TEXT_INDEX, ModelAuthorityFixtures.artifacts().indexPublications(dataSource),
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, ModelAuthorityFixtures.artifacts().generatedPublications(dataSource),
                GenerationOutputTypeIds.TABULAR_EXPORT, ModelAuthorityFixtures.artifacts().tabularPublications(dataSource)));
        executions.takeOwnership(job.tenantId(), job.jobId(), "coordinator-publication", 1);
        try (var publisher = new DurableGenerationPublisher(builds,
                IndexingBootstrap.artifactPublicationStores(router), executions.ownership("coordinator-publication"))) {
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
        var now = Instant.now();
        return new GenerationJob("export-tenant", "export-job", "feature-view/event_features",
                GenerationOutputTypeIds.TABULAR_EXPORT, "generation-empty", "lake.raw.events", "snapshot-empty",
                GenerationExecutionKind.EXPORT, GenerationDependencySet.sourceSnapshot("lake.raw.events", "snapshot-empty"),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://warehouse/staging/export-job",
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "native-build", "", "export-job",
                1, 0, GenerationJobState.QUEUED, 0, now, now, now);
    }
}
