package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph;

import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.store.SchemaMigrationRunner;
import org.pangrova.metadata.testinfra.ArtifactAuthorityFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real PostgreSQL current authority, direct SQL revocation and atomic publication ordering. */
class ArtifactCurrentAuthorityIntegrationTest extends ContainerTestBase {

    private org.pangrova.metadata.testinfra.ArtifactExecutionFixtures executions;
    private static final String TENANT = "tenant-current-authority";
    private static final String PRINCIPAL = "alice";
    private static final Instant NOW = Instant.parse("2026-09-30T00:00:00Z");
    private static final QualifiedTableName SOURCE = new QualifiedTableName("lake", "raw", "events");
    private static final QualifiedTableName TARGET = new QualifiedTableName("lake", "features", "event_features");
    private static final DeferredAuthorizationScope RAW = DeferredAuthorizationScope.raw(SOURCE, Set.of("value"), TARGET);
    private DataSource dataSource;
    private java.util.UUID sourceTableId;
    private JdbcArtifactBuildStore builds;
    private JdbcTabularExportPublicationStore publisher;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("current_artifact_authority");
        SchemaMigrationRunner.migrate(dataSource);
        ArtifactAuthorityFixtures.provision(dataSource, TENANT, PRINCIPAL, RAW);
        var metadata = new org.pangrova.metadata.store.JdbcMetadataStore(dataSource);
        var descriptor = new org.pangrova.metadata.catalog.CatalogDescriptor(SOURCE.catalog(),
                org.pangrova.metadata.catalog.CatalogType.CONNECTOR, "test",
                org.pangrova.metadata.catalog.CatalogState.ACTIVE, Map.of(), "", Set.of());
        metadata.saveCatalogDescriptor(descriptor);
        var control = new org.pangrova.metadata.control.TableControlService(
                new org.pangrova.metadata.control.JdbcTableControlStore(dataSource), name ->
                        new org.pangrova.metadata.control.CatalogDeploymentIdentity(
                                descriptor.incarnationId(), descriptor.revision()));
        sourceTableId = control.registerCreated(SOURCE.catalog(), SOURCE.schema(), SOURCE.table(), PRINCIPAL,
                Optional.of(new org.pangrova.spi.connector.metadata.table.TableIdentity(
                        "TEST", "authority-source", "file:/authority-source", "schema")), Optional.empty())
                .orElseThrow().tableId();
        builds = new JdbcArtifactBuildStore(dataSource);
        executions = new org.pangrova.metadata.testinfra.ArtifactExecutionFixtures(dataSource);
        publisher = new JdbcTabularExportPublicationStore(dataSource);
    }

    @Test
    void revokedGrantBlocksReplayAndPublicationButDoesNotHideMetadata() throws Exception {
        var claim = createClaim("revoked", RAW);
        sql("DELETE FROM security_grants WHERE privilege='SELECT'");
        assertThat(builds.findDispatchSpec(TENANT, claim.jobId())).isPresent();
        assertThatThrownBy(() -> builds.loadAuthorizedDispatchSpec(TENANT, claim.jobId()))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThatThrownBy(() -> publisher.publishClaimed(claim, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(builds.findJob(TENANT, claim.jobId()).orElseThrow().state())
                .isEqualTo(ArtifactBuildJobState.READY_TO_PUBLISH);
        assertThat(builds.findPublishedGeneration(TENANT, claim.artifactId(), "generation-1", "snapshot-1")).isEmpty();
    }

    @Test
    void changedMembershipAndInactiveTenantCannotPublish() throws Exception {
        var claim = createClaim("membership", RAW);
        sql("UPDATE tenant_user_memberships SET tenant_id='another-tenant' WHERE user_principal='alice'");
        assertThatThrownBy(() -> publisher.publishClaimed(claim, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class);
        sql("UPDATE tenant_user_memberships SET tenant_id='" + TENANT + "' WHERE user_principal='alice'");
        sql("UPDATE tenants SET status='DELETED' WHERE tenant_id='" + TENANT + "'");
        assertThatThrownBy(() -> builds.loadAuthorizedDispatchSpec(TENANT, claim.jobId()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void newPolicyBlocksRawBuildAndChangedPolicyBlocksTransformedBuild() throws Exception {
        var transformed = new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(SOURCE, Set.of("value"), Privilege.SELECT,
                        DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new DeferredAuthorizationScope.PolicySnapshot(Optional.empty(), Map.of("value", Optional.empty()))),
                new DeferredAuthorizationScope.TablePermission(TARGET, Set.of(), Privilege.ALTER,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())), Optional.empty());
        var raw = createClaim("raw-policy", RAW);
        var secure = createClaim("transformed-policy", transformed);
        new PersistentRowPolicyStore(dataSource).savePolicy(new RowPolicy(0, "scope_filter",
                SOURCE.catalog(), SOURCE.schema(), SOURCE.table(), "value > 10", List.of("artifact_builder"), true, 1, sourceTableId));
        assertThatThrownBy(() -> publisher.publishClaimed(raw, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("Raw artifact reads");
        assertThatThrownBy(() -> publisher.publishClaimed(secure, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("policy content changed");
    }

    @Test
    void observedPolicyContentCanPublishAndLaterRevokeDoesNotHidePublishedArtifact() throws Exception {
        new PersistentRowPolicyStore(dataSource).savePolicy(new RowPolicy(0, "scope_filter",
                SOURCE.catalog(), SOURCE.schema(), SOURCE.table(), "value > 10", List.of("artifact_builder"), true, 1, sourceTableId));
        var scope = new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(SOURCE, Set.of("value"), Privilege.SELECT,
                        DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new DeferredAuthorizationScope.PolicySnapshot(Optional.of("(value > 10)"),
                                Map.of("value", Optional.empty()))),
                new DeferredAuthorizationScope.TablePermission(TARGET, Set.of(), Privilege.ALTER,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())), Optional.empty());
        var claim = createClaim("observed-policy", scope);
        publisher.publishClaimed(claim, "publisher", 1, NOW.plusSeconds(2));
        sql("DELETE FROM pangrova_user_roles WHERE user_id='alice'");
        assertThat(builds.findPublishedGeneration(TENANT, claim.artifactId(), "generation-1", "snapshot-1")).isPresent();
    }

    @Test
    void publicationAndDirectSqlRevocationAreLinearlyOrderedByOneTransactionGate() throws Exception {
        var claim = createClaim("concurrent", RAW);
        var executor = Executors.newFixedThreadPool(2);
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            int blockerPid;
            try (var statement = blocker.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                result.next();
                blockerPid = result.getInt(1);
            }
            try (var lock = blocker.prepareStatement("SELECT job_id FROM artifact_build_jobs WHERE job_id=? FOR UPDATE")) {
                lock.setString(1, claim.jobId());
                lock.executeQuery().close();
            }
            var publishing = executor.submit(() -> publisher.publishClaimed(claim, "publisher", 1, NOW.plusSeconds(2)));
            await().atMost(Duration.ofSeconds(10)).until(() -> blockedBy(blockerPid) != 0);
            int publicationPid = blockedBy(blockerPid);
            var revoke = executor.submit(() -> {
                sql("DELETE FROM security_grants WHERE privilege='SELECT'");
                return true;
            });
            await().atMost(Duration.ofSeconds(10)).until(() -> blockedBy(publicationPid) != 0);
            assertThat(publishing.isDone()).isFalse();
            assertThat(revoke.isDone()).isFalse();
            blocker.commit();
            assertThat(publishing.get(10, TimeUnit.SECONDS).jobId()).isEqualTo(claim.jobId());
            assertThat(revoke.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(builds.findJob(TENANT, claim.jobId()).orElseThrow().state()).isEqualTo(ArtifactBuildJobState.PUBLISHED);
        } finally {
            executor.shutdownNow();
        }
    }

    private int blockedBy(int backendPid) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid)) LIMIT 1")) {
            statement.setInt(1, backendPid);
            try (var result = statement.executeQuery()) {
                return result.next() ? result.getInt(1) : 0;
            }
        }
    }

    private ArtifactBuildJob createClaim(String id, DeferredAuthorizationScope scope) {
        String jobId = "authority-" + id;
        var job = new ArtifactBuildJob(TENANT, jobId, "artifact-" + id, ArtifactTypeIds.TABULAR_EXPORT,
                "generation-1", "lake.raw.events", "snapshot-1", ArtifactExecutionKind.EXPORT,
                ArtifactDependencySet.sourceSnapshot("lake.raw.events", "snapshot-1"),
                ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://warehouse/staging/" + id,
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "native-build", "", jobId,
                1, 0, ArtifactBuildJobState.QUEUED, 0, NOW, NOW, NOW);
        var spec = ArtifactBuildDispatchSpec.of(jobId,
                new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION,
                        ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(ArtifactStageGraph.StagePlan.of(0, 1,
                                org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind.RELATIONAL_PARTITION,
                                new byte[] {1}, ArtifactStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, PRINCIPAL, scope,
                new ArtifactBuildDispatchSpec.ResourceGrant(1024, 1, 1024, 10_000));
        builds.createJob(job, spec, List.of(new org.pangrova.metadata.artifact.build.ArtifactBuildPage(
                jobId, jobId + ":terminal:0", 0, 0, 0,
                org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = builds.claimNextPage(TENANT, jobId, executions.takeOwnership(TENANT, jobId, "dispatcher", 1), 1,
                Duration.ofSeconds(30), NOW).orElseThrow();
        byte[] report = new byte[] {1, 2};
        assertThat(builds.completePage(new org.pangrova.metadata.artifact.build.ArtifactPageCompletion(
                TENANT, jobId, lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report),
                List.of(new org.pangrova.metadata.artifact.build.ArtifactBuildOutput("terminal-object-" + id,
                        job.stagingUri() + "/terminal-0", "TABULAR_DATA", lease.pageId(), 0,
                        org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope.WORK_UNIT,
                        7, 0, "a".repeat(64), "")), NOW, executions.reportedOwner(TENANT, jobId, lease.ownerEpoch())))).isTrue();
        return builds.claimReadyForPublication(TENANT, jobId, executions.takeOwnership(TENANT, jobId, "publisher", 1), 1,
                Duration.ofMinutes(1), NOW.plusSeconds(1)).orElseThrow();
    }

    private void sql(String command) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(command);
        }
    }
}
