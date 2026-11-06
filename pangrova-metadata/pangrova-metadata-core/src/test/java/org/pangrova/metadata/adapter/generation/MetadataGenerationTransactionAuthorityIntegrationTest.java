package org.pangrova.metadata.adapter.generation;

import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.metadata.generation.jdbc.JdbcTabularExportPublicationStore;

import org.pangrova.generation.core.submission.GenerationDispatchSpecFactory;

import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.core.ownership.TableControlService;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.execution.GenerationExecutionKind;
import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real PostgreSQL current authority, direct SQL revocation and atomic publication ordering. */
class MetadataGenerationTransactionAuthorityIntegrationTest extends ContainerTestBase {

    private org.pangrova.metadata.testinfra.GenerationExecutionFixtures executions;
    private static final String TENANT = "tenant-current-authority";
    private static final String PRINCIPAL = "alice";
    private static final Instant NOW = Instant.parse("2026-09-30T00:00:00Z");
    private static final QualifiedTableName SOURCE = new QualifiedTableName("lake", "raw", "events");
    private static final QualifiedTableName TARGET = new QualifiedTableName("lake", "features", "event_features");
    private static final DeferredAuthorizationScope RAW = DeferredAuthorizationScope.raw(SOURCE, Set.of("value"), TARGET);
    private DataSource dataSource;
    private java.util.UUID sourceTableId;
    private PostgresGenerationJobStore builds;
    private JdbcTabularExportPublicationStore publisher;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("current_artifact_authority");
        SchemaMigrationRunner.migrate(dataSource);
        GenerationAuthorityFixtures.provision(dataSource, TENANT, PRINCIPAL, RAW);
        var metadata = new org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository(dataSource);
        var descriptor = new org.pangrova.metadata.api.catalog.CatalogDescriptor(SOURCE.catalog(),
                org.pangrova.metadata.api.catalog.CatalogType.CONNECTOR, "test",
                org.pangrova.metadata.api.catalog.CatalogState.ACTIVE, Map.of(), "", Set.of());
        metadata.saveCatalogDescriptor(descriptor);
        var control = new org.pangrova.table.core.ownership.TableControlService(
                new org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore(dataSource), name ->
                        new org.pangrova.table.api.identity.CatalogDeploymentIdentity(
                                descriptor.incarnationId(), descriptor.revision()));
        sourceTableId = control.registerCreated(SOURCE.catalog(), SOURCE.schema(), SOURCE.table(), PRINCIPAL,
                Optional.of(new org.pangrova.connector.api.metadata.table.TableIdentity(
                        "TEST", "authority-source", "file:/authority-source", "schema")), Optional.empty())
                .orElseThrow().tableId();
        builds = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.metadata.testinfra.GenerationExecutionFixtures(dataSource);
        publisher = GenerationAuthorityFixtures.artifacts().tabularPublications(dataSource);
    }

    @Test
    void injectedInputPortUsesTheAuthorityConnectionBeforeCurrentTenantValidation() throws Exception {
        var claim = createClaim("input-port", RAW);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var realInputs = new PostgresGenerationQueries();
        try (var caller = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            try (var statement = caller.prepareStatement("UPDATE tenants SET status='DELETED' WHERE tenant_id=?")) {
                statement.setString(1, TENANT);
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            GenerationTransactionInputs inputs = (GenerationTransactionInputs) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{GenerationTransactionInputs.class},
                    (proxy, method, arguments) -> {
                        assertThat(method.getName()).isEqualTo("findDispatchSpec");
                        assertThat(arguments[0]).isSameAs(caller);
                        calls.incrementAndGet();
                        return realInputs.findDispatchSpec(caller, (String) arguments[1], (String) arguments[2]);
                    });
            var authority = new MetadataGenerationTransactionAuthority(
                    inputs, GenerationAuthorityFixtures.modelAuthority());
            assertThatThrownBy(() -> authority.requireCurrent(caller, TENANT, claim.jobId()))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThat(calls).hasValue(1);
            assertThat(caller.getAutoCommit()).isFalse();
            caller.rollback();
            assertThat(authority.requireCurrent(caller, TENANT, claim.jobId()).jobId()).isEqualTo(claim.jobId());
            assertThat(calls).hasValue(2);
            caller.rollback();
        }
    }

    @Test
    void trailingScopePayloadBlocksReplayAndPublicationWithoutChangingTheClaim() throws Exception {
        var claim = createClaim("trailing-scope", RAW);
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE artifact_build_dispatch_specs SET authorization_scope_bytes="
                        + "authorization_scope_bytes || convert_to(' {}','UTF8') WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, TENANT);
            statement.setString(2, claim.jobId());
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
        assertThatThrownBy(() -> builds.loadAuthorizedDispatchSpec(TENANT, claim.jobId()))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("invalid scope metadata");
        assertThatThrownBy(() -> publisher.publishClaimed(claim, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("invalid scope metadata");
        assertThat(builds.findJob(TENANT, claim.jobId()).orElseThrow().state())
                .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
        assertThat(builds.findPublishedGeneration(TENANT, claim.artifactId(), "generation-1", "snapshot-1")).isEmpty();
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
                .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
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
        new JdbcRowPolicyStore(dataSource).savePolicy(new RowPolicy(0, "scope_filter",
                SOURCE.catalog(), SOURCE.schema(), SOURCE.table(), "value > 10", List.of("artifact_builder"), true, 1, sourceTableId));
        assertThatThrownBy(() -> publisher.publishClaimed(raw, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("Raw artifact reads");
        assertThatThrownBy(() -> publisher.publishClaimed(secure, "publisher", 1, NOW.plusSeconds(2)))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("policy content changed");
    }

    @Test
    void observedPolicyContentCanPublishAndLaterRevokeDoesNotHidePublishedArtifact() throws Exception {
        new JdbcRowPolicyStore(dataSource).savePolicy(new RowPolicy(0, "scope_filter",
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
            assertThat(builds.findJob(TENANT, claim.jobId()).orElseThrow().state()).isEqualTo(GenerationJobState.PUBLISHED);
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

    @Test
    void modelAuthorityReceivesBothExactSnapshotSetsOnTheCallerTransaction() throws Exception {
        var spec = specWithBothModelScopes();
        List<List<ModelSnapshot>> calls = new ArrayList<>();
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("UPDATE tenants SET name=? WHERE tenant_id=?")) {
                statement.setString(1, "caller-uncommitted-name");
                statement.setString(2, TENANT);
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            var models = checkingModels((actual, principal, snapshots) -> {
                assertThat(actual).isSameAs(connection);
                assertThat(principal).isEqualTo(PRINCIPAL);
                assertThat(actual.getAutoCommit()).isFalse();
                try (var statement = actual.prepareStatement("SELECT name FROM tenants WHERE tenant_id=?")) {
                    statement.setString(1, TENANT);
                    try (var rows = statement.executeQuery()) {
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getString(1)).isEqualTo("caller-uncommitted-name");
                    }
                }
                calls.add(snapshots);
            });
            new GenerationStores(models).authority().requireCurrent(connection, spec);
            assertThat(calls).containsExactly(spec.stageGraph().modelSnapshots(),
                    spec.authorizationScope().definitionSnapshot().orElseThrow().models());
            assertThat(connection.isClosed()).isFalse();
            connection.rollback();
        }
    }

    @Test
    void modelDenialFromEitherSnapshotSetPropagatesWithoutReplacingOrClosingTheTransaction() throws Exception {
        var spec = specWithBothModelScopes();
        for (int rejectedCall : List.of(1, 2)) {
            List<List<ModelSnapshot>> calls = new ArrayList<>();
            var denial = new AuthorizationDeniedException("Current model authority rejected the scope");
            try (var connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                var models = checkingModels((actual, principal, snapshots) -> {
                    assertThat(actual).isSameAs(connection);
                    assertThat(principal).isEqualTo(PRINCIPAL);
                    calls.add(snapshots);
                    if (calls.size() == rejectedCall) throw denial;
                });
                assertThatThrownBy(() -> new GenerationStores(models).authority().requireCurrent(connection, spec))
                        .isSameAs(denial);
                assertThat(calls).hasSize(rejectedCall);
                assertThat(calls.get(0)).isEqualTo(spec.stageGraph().modelSnapshots());
                if (rejectedCall == 2) {
                    assertThat(calls.get(1))
                            .isEqualTo(spec.authorizationScope().definitionSnapshot().orElseThrow().models());
                }
                assertThat(connection.isClosed()).isFalse();
                assertThat(connection.getAutoCommit()).isFalse();
                connection.rollback();
            }
        }
    }

    private GenerationDispatchSpec specWithBothModelScopes() {
        var declaration = new WriteDeclarationSnapshot(TARGET, "1".repeat(64), "2".repeat(64), true);
        var definitionModel = new ModelSnapshot("write-model", "write-incarnation", 2, "deployment-2", "b".repeat(64));
        var scope = new DeferredAuthorizationScope(List.of(RAW.permissions().get(0),
                new DeferredAuthorizationScope.TablePermission(TARGET, Set.of(), Privilege.INSERT,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())),
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, List.of(definitionModel))));
        new JdbcGrantStore(dataSource).addGrant(PrivilegeGrant.of("artifact_builder", Privilege.INSERT,
                "TABLE", TARGET.catalog(), TARGET.schema(), TARGET.table(), null));
        var graphModel = new ModelSnapshot("graph-model", "graph-incarnation", 1, "deployment-1", "a".repeat(64));
        var stage = GenerationStageGraph.StagePlan.of(0, 1,
                org.pangrova.generation.api.execution.GenerationPageKind.RELATIONAL_PARTITION,
                new byte[]{1}, GenerationStageTemplate.of(new byte[]{2}, List.of(), List.of()),
                List.of(), List.of(), true);
        return GenerationDispatchSpecFactory.create("model-forwarding",
                new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION, GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(stage), List.of(), List.of(graphModel)), TENANT, PRINCIPAL, scope,
                new GenerationDispatchSpec.ResourceGrant(1024, 1, 1024, 10_000));
    }

    private CurrentModelAuthority checkingModels(ModelCheck check) {
        return new CurrentModelAuthority() {
            @Override
            public List<ModelSnapshot> observe(Connection connection, String principal, Set<String> names) {
                throw new AssertionError("Publication must validate captured model snapshots, not observe replacements");
            }

            @Override
            public void requireCurrent(Connection connection, String principal, List<ModelSnapshot> models)
                    throws SQLException {
                check.requireCurrent(connection, principal, models);
            }
        };
    }

    @FunctionalInterface
    private interface ModelCheck {
        void requireCurrent(Connection connection, String principal, List<ModelSnapshot> models) throws SQLException;
    }

    private GenerationJob createClaim(String id, DeferredAuthorizationScope scope) {
        String jobId = "authority-" + id;
        var job = new GenerationJob(TENANT, jobId, "artifact-" + id, GenerationOutputTypeIds.TABULAR_EXPORT,
                "generation-1", "lake.raw.events", "snapshot-1", GenerationExecutionKind.EXPORT,
                GenerationDependencySet.sourceSnapshot("lake.raw.events", "snapshot-1"),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://warehouse/staging/" + id,
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "native-build", "", jobId,
                1, 0, GenerationJobState.QUEUED, 0, NOW, NOW, NOW);
        var spec = GenerationDispatchSpecFactory.create(jobId,
                new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(GenerationStageGraph.StagePlan.of(0, 1,
                                org.pangrova.generation.api.execution.GenerationPageKind.RELATIONAL_PARTITION,
                                new byte[] {1}, GenerationStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, PRINCIPAL, scope,
                new GenerationDispatchSpec.ResourceGrant(1024, 1, 1024, 10_000));
        builds.createJob(job, spec, List.of(new org.pangrova.generation.api.page.GenerationPage(
                jobId, jobId + ":terminal:0", 0, 0, 0,
                org.pangrova.generation.api.execution.GenerationPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = builds.claimNextPage(TENANT, jobId, executions.takeOwnership(TENANT, jobId, "dispatcher", 1), 1,
                Duration.ofSeconds(30), NOW).orElseThrow();
        byte[] report = new byte[] {1, 2};
        assertThat(builds.completePage(new org.pangrova.generation.api.page.GenerationPageCompletion(
                TENANT, jobId, lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report),
                List.of(new org.pangrova.generation.api.output.GenerationOutput("terminal-object-" + id,
                        job.stagingUri() + "/terminal-0", "TABULAR_DATA", lease.pageId(), 0,
                        org.pangrova.generation.api.output.GenerationOutputScope.WORK_UNIT,
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
