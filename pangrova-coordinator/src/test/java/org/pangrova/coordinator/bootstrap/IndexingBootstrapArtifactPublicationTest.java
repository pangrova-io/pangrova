package org.pangrova.coordinator.bootstrap;

import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.artifact.ArtifactGenerationManifest;
import org.pangrova.metadata.artifact.ArtifactPublicationStore;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.coordinator.artifact.DurableArtifactPublisher;
import org.pangrova.coordinator.testinfra.CoordinatorArtifactAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.artifact.ArtifactPublicationRouter;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.metadata.artifact.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.metadata.artifact.jdbc.JdbcIndexArtifactPublicationStore;
import org.pangrova.metadata.artifact.jdbc.JdbcTabularExportPublicationStore;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.testing.ContainerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class IndexingBootstrapArtifactPublicationTest extends ContainerTestBase {

    private org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    @Test
    void registersEveryDurableArtifactTypeForPublication() {
        ArtifactPublicationStore store = (claim, owner, epoch, now) ->
                new ArtifactGenerationManifest(
                        claim.tenantId(), claim.jobId(), claim.artifactId(), claim.artifactType(),
                        claim.stagingGeneration(), claim.sourceId(), claim.sourceSnapshot(),
                        claim.executionKind(), claim.dependencySet(), claim.emptyPartitionPolicy(),
                        claim.formatId(), claim.formatMajor(), claim.formatMinor(),
                        claim.schemaSha256(), claim.writerBuildId(), claim.stagingUri(),
                        "a".repeat(64), 0, 0, 0, now);

        assertThat(IndexingBootstrap.artifactPublicationStores(store))
                .containsOnlyKeys(
                        ArtifactTypeIds.VECTOR_INDEX,
                        ArtifactTypeIds.TEXT_INDEX,
                        ArtifactTypeIds.TABULAR_EXPORT,
                        ArtifactTypeIds.GENERATED_COLUMN_VALUES)
                .allSatisfy((type, registered) -> assertThat(registered).isSameAs(store));
    }

    @Test
    void productionPublicationLoopRoutesTabularExportToItsRealPostgresPublisher() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("bootstrap_export_publication");
        var scope = DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "events"),
                Set.of("value"), new QualifiedTableName("lake", "features", "event_features"));
        CoordinatorArtifactAuthorityFixtures.provision(dataSource, "export-tenant", "alice", scope);
        var builds = new JdbcArtifactBuildStore(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures(dataSource);
        var job = emptyExportJob();
        var spec = ArtifactBuildDispatchSpec.of(job.jobId(),
                new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION,
                        ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(ArtifactStageGraph.StagePlan.of(0, 1,
                                org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind.RELATIONAL_PARTITION,
                                new byte[] {1}, ArtifactStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                job.tenantId(), "alice", scope, new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        builds.createJob(job, spec, List.of(new org.pangrova.metadata.artifact.build.ArtifactBuildPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = builds.claimNextPage(job.tenantId(), job.jobId(), executions.takeOwnership(job.tenantId(), job.jobId(), "dispatcher", 1), 1,
                Duration.ofSeconds(30), job.createdAt()).orElseThrow();
        byte[] report = new byte[] {1, 2};
        assertThat(builds.completePage(new org.pangrova.metadata.artifact.build.ArtifactPageCompletion(
                job.tenantId(), job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report),
                List.of(new org.pangrova.metadata.artifact.build.ArtifactBuildOutput("object-terminal-0",
                        job.stagingUri() + "/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                        org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope.WORK_UNIT, 7, 0,
                        "a".repeat(64), "")), Instant.now(), executions.reportedOwner(job.tenantId(), job.jobId(), lease.ownerEpoch())))).isTrue();
        var router = new ArtifactPublicationRouter(Map.of(
                ArtifactTypeIds.VECTOR_INDEX, new JdbcIndexArtifactPublicationStore(dataSource),
                ArtifactTypeIds.TEXT_INDEX, new JdbcIndexArtifactPublicationStore(dataSource),
                ArtifactTypeIds.GENERATED_COLUMN_VALUES, new JdbcGeneratedValueArtifactPublicationStore(dataSource),
                ArtifactTypeIds.TABULAR_EXPORT, new JdbcTabularExportPublicationStore(dataSource)));
        executions.takeOwnership(job.tenantId(), job.jobId(), "coordinator-publication", 1);
        try (var publisher = new DurableArtifactPublisher(builds,
                IndexingBootstrap.artifactPublicationStores(router), executions.ownership("coordinator-publication"))) {
            publisher.start();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(builds.findJob(job.tenantId(), job.jobId()).orElseThrow().state())
                            .isEqualTo(ArtifactBuildJobState.PUBLISHED));
        }
        assertThat(builds.findPublishedGeneration(job.tenantId(), job.artifactId(),
                job.stagingGeneration(), job.sourceSnapshot())).hasValueSatisfying(manifest -> {
                    assertThat(manifest.artifactType()).isEqualTo(ArtifactTypeIds.TABULAR_EXPORT);
                    assertThat(manifest.objectCount()).isEqualTo(1);
                    assertThat(manifest.rowCount()).isZero();
                    assertThat(manifest.manifestSha256()).hasSize(64);
                });
    }

    private static ArtifactBuildJob emptyExportJob() {
        var now = Instant.now();
        return new ArtifactBuildJob("export-tenant", "export-job", "feature-view/event_features",
                ArtifactTypeIds.TABULAR_EXPORT, "generation-empty", "lake.raw.events", "snapshot-empty",
                ArtifactExecutionKind.EXPORT, ArtifactDependencySet.sourceSnapshot("lake.raw.events", "snapshot-empty"),
                ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://warehouse/staging/export-job",
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "native-build", "", "export-job",
                1, 0, ArtifactBuildJobState.QUEUED, 0, now, now, now);
    }
}
