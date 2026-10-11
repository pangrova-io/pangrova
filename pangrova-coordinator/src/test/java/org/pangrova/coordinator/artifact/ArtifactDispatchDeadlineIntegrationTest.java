package org.pangrova.coordinator.artifact;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.coordinator.testinfra.CoordinatorArtifactAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.planner.artifact.ArtifactBuildPlan;
import org.pangrova.planner.artifact.VectorArtifactBuildPlanBuilder;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.ArtifactDependency;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PostgreSQL claims and canonical planner/binding contracts, without a Worker substitute. */
class ArtifactDispatchDeadlineIntegrationTest extends ContainerTestBase {

    private org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures executions;

    @org.junit.jupiter.api.AfterEach
    void closeExecutionProcesses() {
        if (executions != null) executions.close();
    }
    private static final Instant NOW = Instant.parse("2026-09-30T00:00:00Z");
    private static final String TENANT = "deadline-tenant";
    private static final String SOURCE = "lake.raw.items";
    private static final String SNAPSHOT = "snapshot-1";
    private static final String STAGING = "s3://warehouse/staging/deadline";
    private static final Duration INITIAL_LEASE = Duration.ofMinutes(2);
    private static final DeferredAuthorizationScope SCOPE = DeferredAuthorizationScope.raw(
            new QualifiedTableName("lake", "raw", "items"), Set.of("embedding"),
            new QualifiedTableName("lake", "raw", "items"));
    private DataSource dataSource;
    private JdbcArtifactBuildStore builds;
    private ArtifactDispatchRequestFactory factory;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("artifact_dispatch_deadline");
        CoordinatorArtifactAuthorityFixtures.provision(dataSource, TENANT, "alice", SCOPE);
        builds = new JdbcArtifactBuildStore(dataSource);
        executions = new org.pangrova.coordinator.testinfra.CoordinatorArtifactExecutionFixtures(dataSource);
        factory = new ArtifactDispatchRequestFactory(
                new org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore(dataSource));
    }

    @ParameterizedTest
    @ValueSource(longs = {30_000, 600_000})
    void canonicalDeadlineCannotExceedInitialLeaseEvenAfterDatabaseRenewal(long timeoutMillis) {
        var lease = claim(timeoutMillis);
        var request = factory.prepare(lease, NOW).request().taskRequest();
        long expected = Math.min(lease.expiresAt().toEpochMilli(), NOW.toEpochMilli() + timeoutMillis);
        assertThat(request.getDeadlineEpochMs()).isEqualTo(expected);
        assertThat(request.getOperatorBindingsList()).allSatisfy(binding ->
                assertThat(binding.getExecutionPolicy().getDeadlineEpochMs()).isEqualTo(expected));
        assertThat(request.getSecurity().getPrincipal()).isEqualTo("alice");
        assertThat(request.getTraceContext().getTraceId()).matches("[0-9a-f]{32}")
                .isNotEqualTo("00000000000000000000000000000000");
        assertThat(request.getTraceContext().getSpanId()).matches("[0-9a-f]{16}")
                .isNotEqualTo("0000000000000000");
        assertThat(org.pangrova.protocol.task.CanonicalTaskRequests.callbackFor(request).getTraceContext())
                .isEqualTo(request.getTraceContext());
        assertThat(request.getAttempt().getFencingToken()).isEqualTo(lease.fencingToken());
        assertThat(request.getAttempt().getExecutionOwner().getOwnerId()).isEqualTo(lease.leaseOwner());
        assertThat(request.getAttempt().getQueryOrJobId()).isEqualTo(lease.executionAnchor().executionId());
        assertThat(request.getAttempt().getExecutionOwner().getOwnerIncarnation())
                .isEqualTo(lease.executionAnchor().ownerIncarnation());
        assertThat(request.getAttempt().getExecutionOwner().getFencingToken())
                .isEqualTo(lease.executionAnchor().fencingToken());
        assertThat(builds.renewPage(lease, Duration.ofMinutes(10), NOW.plusSeconds(10))).isTrue();
        var rebound = factory.prepare(lease, NOW.plusSeconds(10)).request().taskRequest();
        assertThat(rebound.getDeadlineEpochMs()).isLessThanOrEqualTo(lease.expiresAt().toEpochMilli());
        assertThat(request.getDeadlineEpochMs()).isEqualTo(expected);
        assertThatThrownBy(() -> factory.prepare(lease, lease.expiresAt()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("lease expired");
    }

    @Test
    void revocationBlocksNewDispatchAndRenewalOfTheRealFencedPage() throws Exception {
        var lease = claim(Duration.ofMinutes(10).toMillis());
        factory.prepare(lease, NOW);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM security_grants WHERE privilege='SELECT'");
        }
        assertThatThrownBy(() -> factory.prepare(lease, NOW.plusSeconds(1)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThatThrownBy(() -> builds.renewPage(lease, INITIAL_LEASE, NOW.plusSeconds(1)))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void durableCancellationRetiresRetryResponsibilityWithoutRestoringRevokedPublication() throws Exception {
        var lease = claim(Duration.ofMinutes(10).toMillis());
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM security_grants WHERE privilege='SELECT'");
        }
        assertThat(builds.cancelJob(TENANT, lease.jobId(), "real authorization revoked", NOW.plusSeconds(1))).isTrue();
        ArtifactLeaseRetry.retry(builds, lease, NOW.plusSeconds(20), "Native exit observed", NOW.plusSeconds(2));
        assertThat(builds.findJob(TENANT, lease.jobId()).orElseThrow().state()).isEqualTo(ArtifactBuildJobState.CANCELLED);
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT state,lease_owner FROM artifact_build_pages WHERE tenant_id=? AND job_id=? AND page_id=?")) {
            statement.setString(1, TENANT);
            statement.setString(2, lease.jobId());
            statement.setString(3, lease.pageId());
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("state")).isEqualTo("CANCELLED");
                assertThat(rows.getString("lease_owner")).isNull();
            }
        }
    }

    @Test
    void liveRevokedJobCannotTurnAnExecutionExitIntoRequeuePermission() throws Exception {
        var lease = claim(Duration.ofMinutes(10).toMillis());
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM security_grants WHERE privilege='SELECT'");
        }
        assertThatThrownBy(() -> ArtifactLeaseRetry.retry(builds, lease, NOW.plusSeconds(20),
                "Native exit observed", NOW.plusSeconds(2))).isInstanceOf(AuthorizationDeniedException.class);
        assertThat(builds.findJob(TENANT, lease.jobId()).orElseThrow().state()).isEqualTo(ArtifactBuildJobState.RUNNING);
    }

    private ArtifactBuildLease claim(long timeoutMillis) {
        var plan = VectorArtifactBuildPlanBuilder.build(List.of("lake", "raw", "items"), "embedding",
                BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef(), BuiltInCapabilityCatalog.hnswArtifactFormat());
        var dependencies = ArtifactDependencySet.sourceSnapshot(SOURCE, SNAPSHOT);
        var format = plan.format();
        var job = new ArtifactBuildJob(TENANT, "deadline-job", "deadline-index", ArtifactTypeIds.VECTOR_INDEX,
                "generation-1", SOURCE, SNAPSHOT, ArtifactExecutionKind.BUILD, dependencies,
                ArtifactEmptyPartitionPolicy.FORBID, STAGING, format.getFormatId(), format.getMajorVersion(),
                format.getMinorVersion(), HexFormat.of().formatHex(format.getSchemaSha256().toByteArray()),
                format.getWriterBuildId(), "", "deadline-job", 1, 0, ArtifactBuildJobState.QUEUED,
                0, NOW, NOW, NOW);
        var pages = List.of(new ArtifactBuildPage(job.jobId(), "page-1", 0, 0, 0,
                ArtifactBuildPageKind.SOURCE_MAP, List.of(split()), List.of(), 3));
        var spec = ArtifactBuildDispatchSpec.of(job.jobId(),
                org.pangrova.metadata.artifact.build.ArtifactStageGraph.forArtifactPages(plan.planBytes(), bindings(plan, dependencies), pages, java.util.List.of()),
                TENANT, "alice", SCOPE, new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, timeoutMillis));
        builds.createJob(job, spec, pages);
        return builds.claimNextPage(TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "coordinator-1", 1), 1, INITIAL_LEASE, NOW).orElseThrow();
    }

    private static byte[] bindings(ArtifactBuildPlan plan, ArtifactDependencySet dependencies) {
        var output = ArtifactWriteBinding.newBuilder().setArtifactId("deadline-index")
                .setArtifactType(ArtifactTypeIds.VECTOR_INDEX).setGeneration("generation-1")
                .setSourceId(SOURCE).setSourceSnapshot(SNAPSHOT).setStagingUri(STAGING)
                .setFormat(plan.format()).setRowLocator(plan.rowLocator())
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind.ARTIFACT_EXECUTION_KIND_BUILD)
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .addDependencies(ArtifactDependency.newBuilder().setKind("SOURCE_SNAPSHOT")
                        .setId(SOURCE).setVersion(SNAPSHOT))
                .setDependencySetChecksum(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(dependencies.sha256()))));
        return OperatorBindingSet.newBuilder().addBindings(OperatorBinding.newBuilder()
                .setPlanNodeId(plan.planNodeId()).setCapability(plan.capability()).setOutputArtifact(output))
                .build().toByteArray();
    }

    private static ArtifactBuildSplit split() {
        var split = DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "iceberg", SOURCE, SNAPSHOT, "object-1", "s3://warehouse/items.parquet", 0, 1024, Map.of(),
                FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_PARQUET).build()));
        return ArtifactBuildSplit.of(split.getDataObjectId(), split.toByteArray());
    }
}
