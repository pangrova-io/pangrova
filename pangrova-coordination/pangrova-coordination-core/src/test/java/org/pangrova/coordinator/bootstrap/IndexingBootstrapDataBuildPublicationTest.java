package org.pangrova.coordinator.bootstrap;


import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;

import org.junit.jupiter.api.Test;
import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;
import org.pangrova.databuild.api.publication.DataBuildPublicationStore;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.coordinator.databuild.DurableDataBuildPublisher;
import org.pangrova.coordinator.testinfra.CoordinatorDataBuildAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.databuild.assembly.DataBuildPublicationFactory;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class IndexingBootstrapDataBuildPublicationTest extends ContainerTestBase {

    private org.pangrova.coordinator.testinfra.CoordinatorDataBuildExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }

    @Test
    void registersEveryDurableArtifactTypeForPublication() {
        DataBuildPublicationStore store = (claim, owner, epoch, now) ->
                new DataBuildGenerationManifest(
                        claim.tenantId(), claim.jobId(), claim.artifactId(), claim.artifactType(),
                        claim.stagingGeneration(), claim.sourceId(), claim.sourceSnapshot(),
                        claim.executionKind(), claim.dependencySet(), claim.emptyPartitionPolicy(),
                        claim.formatId(), claim.formatMajor(), claim.formatMinor(),
                        claim.schemaSha256(), claim.writerBuildId(), claim.stagingUri(),
                        "a".repeat(64), 0, 0, 0, now);

        assertThat(IndexingBootstrap.artifactPublicationStores(store))
                .containsOnlyKeys(
                        DataBuildOutputTypeIds.VECTOR_INDEX,
                        DataBuildOutputTypeIds.TEXT_INDEX,
                        DataBuildOutputTypeIds.TABULAR_EXPORT,
                        DataBuildOutputTypeIds.GENERATED_COLUMN_VALUES)
                .allSatisfy((type, registered) -> assertThat(registered).isSameAs(store));
    }

    @Test
    void productionPublicationLoopRoutesTabularExportToItsRealPostgresPublisher() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("bootstrap_export_publication");
        var scope = DeferredAuthorizationScope.raw(new QualifiedTableName("lake", "raw", "events"),
                Set.of("value"), new QualifiedTableName("lake", "features", "event_features"));
        CoordinatorDataBuildAuthorityFixtures.provision(dataSource, "export-tenant", "alice", scope);
        var builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorDataBuildExecutionFixtures(dataSource);
        var job = emptyExportJob();
        var spec = DataBuildDispatchSpecFactory.create(job.jobId(),
                new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION,
                        DataBuildStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(DataBuildStageGraph.StagePlan.of(0, 1,
                                org.pangrova.databuild.api.DataBuildPageKind.RELATIONAL_PARTITION,
                                new byte[] {1}, DataBuildStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                job.tenantId(), "alice", scope, new DataBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
        builds.createJob(job, spec, List.of(new org.pangrova.databuild.api.build.DataBuildPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.databuild.api.DataBuildPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = builds.claimNextPage(job.tenantId(), job.jobId(), executions.takeOwnership(job.tenantId(), job.jobId(), "dispatcher", 1), 1,
                Duration.ofSeconds(30), job.createdAt()).orElseThrow();
        byte[] report = new byte[] {1, 2};
        assertThat(builds.completePage(new org.pangrova.databuild.api.build.DataBuildPageCompletion(
                job.tenantId(), job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report),
                List.of(new org.pangrova.databuild.api.build.DataBuildOutput("object-terminal-0",
                        job.stagingUri() + "/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                        org.pangrova.databuild.api.DataBuildOutputScope.WORK_UNIT, 7, 0,
                        "a".repeat(64), "")), Instant.now(), executions.reportedOwner(job.tenantId(), job.jobId(), lease.ownerEpoch())))).isTrue();
        var router = DataBuildPublicationFactory.route(Map.of(
                DataBuildOutputTypeIds.VECTOR_INDEX, ModelAuthorityFixtures.artifacts().indexPublications(dataSource),
                DataBuildOutputTypeIds.TEXT_INDEX, ModelAuthorityFixtures.artifacts().indexPublications(dataSource),
                DataBuildOutputTypeIds.GENERATED_COLUMN_VALUES, ModelAuthorityFixtures.artifacts().generatedPublications(dataSource),
                DataBuildOutputTypeIds.TABULAR_EXPORT, ModelAuthorityFixtures.artifacts().tabularPublications(dataSource)));
        executions.takeOwnership(job.tenantId(), job.jobId(), "coordinator-publication", 1);
        try (var publisher = new DurableDataBuildPublisher(builds,
                IndexingBootstrap.artifactPublicationStores(router), executions.ownership("coordinator-publication"))) {
            publisher.start();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(builds.findJob(job.tenantId(), job.jobId()).orElseThrow().state())
                            .isEqualTo(DataBuildJobState.PUBLISHED));
        }
        assertThat(builds.findPublishedGeneration(job.tenantId(), job.artifactId(),
                job.stagingGeneration(), job.sourceSnapshot())).hasValueSatisfying(manifest -> {
                    assertThat(manifest.artifactType()).isEqualTo(DataBuildOutputTypeIds.TABULAR_EXPORT);
                    assertThat(manifest.objectCount()).isEqualTo(1);
                    assertThat(manifest.rowCount()).isZero();
                    assertThat(manifest.manifestSha256()).hasSize(64);
                });
    }

    private static DataBuildJob emptyExportJob() {
        var now = Instant.now();
        return new DataBuildJob("export-tenant", "export-job", "feature-view/event_features",
                DataBuildOutputTypeIds.TABULAR_EXPORT, "generation-empty", "lake.raw.events", "snapshot-empty",
                DataBuildExecutionKind.EXPORT, DataBuildDependencySet.sourceSnapshot("lake.raw.events", "snapshot-empty"),
                DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://warehouse/staging/export-job",
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "native-build", "", "export-job",
                1, 0, DataBuildJobState.QUEUED, 0, now, now, now);
    }
}
