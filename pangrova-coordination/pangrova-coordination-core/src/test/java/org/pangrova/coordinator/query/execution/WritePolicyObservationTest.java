package org.pangrova.coordinator.query.execution;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.parser.core.parse.AntlrSqlParser;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.security.ColumnMaskApplier;
import org.pangrova.planner.core.security.RowFilterApplier;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ReadMode;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.security.api.error.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.ContainerTestBase;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Actual SQL compilation and PostgreSQL policies, not Native execution or a synthetic observed plan. */
class WritePolicyObservationTest extends ContainerTestBase {
    private SqlCompiler compiler;
    private SecurityPolicyApplier security;
    private JdbcRowPolicyStore rowPolicies;
    private JdbcColumnMaskStore masks;
    private JdbcGrantStore grants;
    @TempDir private java.nio.file.Path warehouse;
    private org.pangrova.coordinator.metadata.MetadataBootstrap metadata;
    private org.pangrova.coordinator.metadata.PluginConnectorHandleFactory connectorFactory;
    private final QuerySession actor = new QuerySession("write-observation",
            Map.of("catalog", "default", "schema", "default"),
            new UserIdentity("alice", Set.of("external-group"), Map.of(), Instant.EPOCH), "tenant-a", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

    @BeforeEach
    void initializeCurrentAuthority() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_observation");
        var roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of("writer"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "writer"));
        grants = new JdbcGrantStore(dataSource);
        for (String table : List.of("target_rows", "source_rows")) {
            grants.addGrant(PrivilegeGrant.of("writer", Privilege.SELECT, "TABLE", "default", "default", table, null));
        }
        rowPolicies = new JdbcRowPolicyStore(dataSource);
        masks = new JdbcColumnMaskStore(dataSource);
        var authority = new org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(rowPolicies), new ColumnMaskingProcessor(masks));
        compiler = compiler(dataSource);
        security = new SecurityPolicyApplier(compiler, new AccessControlEnforcer(authority),
                new ColumnMaskApplier(authority), new RowFilterApplier(authority));
    }

    @AfterEach
    void closeCatalogResources() {
        try {
            if (metadata != null) metadata.shutdown();
        } finally {
            if (connectorFactory != null) connectorFactory.shutdown();
        }
    }

    @Test
    void insertValuesHasNoInventedReadsAndOnlyTheOriginalInsertPrivilege() {
        var observation = capture("INSERT INTO target_rows VALUES (1, 1, 7)");

        assertThat(observation.reads()).isEmpty();
        assertThat(observation.writePrivileges()).containsExactly(Privilege.INSERT);
    }

    @Test
    void updateObservesRealPolicyExpressionsWhilePreservingRawPhysicalInput() {
        installTargetPolicies();
        String sql = "UPDATE target_rows SET tenant = 7 WHERE score = 0";
        var observed = observed(sql);
        var observation = WritePolicyObservation.capture(observed, compiler, new AntlrSqlParser().parse(sql));

        assertThat(observation.writePrivileges()).containsExactly(Privilege.UPDATE);
        assertThat(observation.reads()).singleElement().satisfies(permission -> {
            assertThat(permission.table().toString()).isEqualTo("default.default.target_rows");
            assertThat(permission.columns()).containsExactlyInAnyOrder("id", "tenant", "score");
            assertThat(permission.readMode()).isEqualTo(ReadMode.SECURITY_TRANSFORMED);
            assertThat(permission.observedPolicy().rowFilter()).contains("(tenant = 1)");
            assertThat(permission.observedPolicy().columnMasks()).containsEntry("score", Optional.of("0"))
                    .containsEntry("id", Optional.empty()).containsEntry("tenant", Optional.empty());
        });
        var modify = (LogicalTableModify) observed.plan().plan();
        var project = (LogicalProject) modify.getInput();
        assertThat(project.getProjects().subList(0, 3)).allMatch(RexInputRef.class::isInstance);
        var filter = (LogicalFilter) project.getInput();
        assertThat(filter.getInput()).isInstanceOf(TableScan.class);
        assertThat(observed.plan().serializeValidated(compiler)).isNotEmpty();
    }

    @Test
    void deleteObservesAbsenceAsExactEvidenceNotAsMissingProof() {
        var observation = capture("DELETE FROM target_rows WHERE score > 7");

        assertThat(observation.writePrivileges()).containsExactly(Privilege.DELETE);
        assertThat(observation.reads()).singleElement().satisfies(permission -> {
            assertThat(permission.observedPolicy().rowFilter()).isEmpty();
            assertThat(permission.observedPolicy().columnMasks()).containsOnlyKeys("id", "tenant", "score");
            assertThat(permission.observedPolicy().columnMasks().values()).allMatch(Optional::isEmpty);
        });
    }

    @Test
    void insertJoinCapturesBothActualSourcesIncludingPolicyDependenciesAndAbsences() {
        installTargetPolicies();
        var observation = capture("INSERT INTO target_rows "
                + "SELECT s.id, t.tenant, s.score FROM source_rows s JOIN target_rows t ON s.id = t.id");

        assertThat(observation.writePrivileges()).containsExactly(Privilege.INSERT);
        assertThat(observation.reads()).hasSize(2).allSatisfy(permission -> {
            assertThat(permission.columns()).containsExactlyInAnyOrder("id", "tenant", "score");
            assertThat(permission.privilege()).isEqualTo(Privilege.SELECT);
            assertThat(permission.readMode()).isEqualTo(ReadMode.SECURITY_TRANSFORMED);
        });
        var target = observation.reads().stream().filter(read -> read.table().table().equals("target_rows"))
                .findFirst().orElseThrow();
        var source = observation.reads().stream().filter(read -> read.table().table().equals("source_rows"))
                .findFirst().orElseThrow();
        assertThat(target.observedPolicy().rowFilter()).contains("(tenant = 1)");
        assertThat(target.observedPolicy().columnMasks()).containsEntry("score", Optional.of("0"));
        assertThat(source.observedPolicy().rowFilter()).isEmpty();
        assertThat(source.observedPolicy().columnMasks().values()).allMatch(Optional::isEmpty);
    }

    @Test
    void mergeUpdateOnlyDoesNotInventInsertOrDeleteWritePrivileges() {
        installTargetPolicies();
        var observation = capture("MERGE INTO target_rows t USING source_rows s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET score = s.score");

        assertThat(observation.writePrivileges()).containsExactly(Privilege.UPDATE);
        assertThat(observation.reads()).extracting(permission -> permission.table().table())
                .containsExactlyInAnyOrder("target_rows", "source_rows");
    }

    @Test
    void removingActualObservedPolicyEvidenceFailsClosed() {
        var actual = observed("DELETE FROM target_rows WHERE id = 1");
        var missing = new SecurityPolicyApplier.ObservedPlan(actual.plan(), Map.of());

        assertThatThrownBy(() -> WritePolicyObservation.capture(missing, compiler,
                new AntlrSqlParser().parse("DELETE FROM target_rows WHERE id = 1")))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("observations are incomplete");
    }

    @Test
    void aSubsequentCompilationUsesCurrentPolicyAndRejectsRealSelectRevocation() {
        var first = capture("DELETE FROM target_rows");
        installTargetPolicies();
        var second = capture("DELETE FROM target_rows");

        assertThat(first.reads().get(0).observedPolicy().rowFilter()).isEmpty();
        assertThat(second.reads().get(0).observedPolicy().rowFilter()).contains("(tenant = 1)");
        grants.revokeGrants("writer", Privilege.SELECT, "default", "default", "target_rows");
        assertThatThrownBy(() -> capture("DELETE FROM target_rows"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void readCompilationAppliesCurrentPoliciesBeforeAnyRelationalOptimization() {
        installTargetPolicies();
        var query = security.compileAndApplyPolicies("SELECT id, score FROM target_rows", actor);

        assertThat(query.planExplanation()).contains("LogicalTableScan", "LogicalFilter", "0:BIGINT");
        assertThat(query.serializeValidated(compiler)).isNotEmpty();
        grants.revokeGrants("writer", Privilege.SELECT, "default", "default", "target_rows");
        assertThatThrownBy(() -> security.compileAndApplyPolicies("SELECT id FROM target_rows", actor))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void readCompilationRejectsMissingPolicyEnforcementInsteadOfReturningAnUnprotectedPlan() {
        var incomplete = new SecurityPolicyApplier(compiler, null, null, null);

        assertThatThrownBy(() -> incomplete.compileAndApplyPolicies("SELECT id FROM target_rows", actor))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("complete authorization");
    }

    private WritePolicyObservation capture(String sql) {
        return WritePolicyObservation.capture(observed(sql), compiler, new AntlrSqlParser().parse(sql));
    }

    private SecurityPolicyApplier.ObservedPlan observed(String sql) {
        return security.applyPoliciesObserved(compiler.compileToPlannedQuery(sql), actor);
    }

    private void installTargetPolicies() {
        rowPolicies.savePolicy(new RowPolicy(0, "tenant_rows", "default", "default", "target_rows",
                "tenant = 1", List.of("writer"), true, 1, null));
        masks.saveColumnMask(new ColumnMask(0, "score_mask", "default", "default", "target_rows",
                "score", "0", List.of("writer"), List.of(), true, null));
    }

    private SqlCompiler compiler(javax.sql.DataSource dataSource) throws java.io.IOException {
        try (var catalog = new org.apache.iceberg.hadoop.HadoopCatalog(
                new org.apache.hadoop.conf.Configuration(), warehouse.toString())) {
            catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("default"));
            var columns = new org.apache.iceberg.Schema(
                    org.apache.iceberg.types.Types.NestedField.required(1, "id",
                            org.apache.iceberg.types.Types.LongType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(2, "tenant",
                            org.apache.iceberg.types.Types.LongType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(3, "score",
                            org.apache.iceberg.types.Types.LongType.get()));
            for (String name : List.of("target_rows", "source_rows")) {
                catalog.createTable(org.apache.iceberg.catalog.TableIdentifier.of("default", name), columns);
            }
        }
        var directory = new org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository(dataSource);
        directory.saveCatalogDescriptor(new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                "default", org.pangrova.metadata.api.catalog.CatalogType.CONNECTOR, "iceberg",
                org.pangrova.metadata.api.catalog.CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString()),
                "", Set.of("tenant-a")));
        connectorFactory = new org.pangrova.coordinator.metadata.PluginConnectorHandleFactory(
                getClass().getClassLoader(), new org.pangrova.security.core.secret.SecretResolverRegistry(),
                ignored -> Optional.empty());
        metadata = org.pangrova.coordinator.metadata.MetadataBootstrap.initialize(directory, new PostgresTableProjectionRepository(dataSource), new PostgresMetadataProjectionTransactions(dataSource), new PostgresMetadataSyncRepository(dataSource), connectorFactory);
        var root = metadata.buildCalciteSchema(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(actor));
        return new SqlCompiler(new PangrovaPlanner(root, List.of("default", "default")),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }
}
