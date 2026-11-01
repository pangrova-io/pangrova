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
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.parser.SqlParser;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.security.authz.DeferredAuthorizationScope.ReadMode;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.ContainerTestBase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SQL compilation and PostgreSQL policies, not Native execution or a synthetic observed plan. */
class WritePolicyObservationTest extends ContainerTestBase {
    private SqlCompiler compiler;
    private SecurityPolicyApplier security;
    private PersistentRowPolicyStore rowPolicies;
    private PersistentColumnMaskStore masks;
    private PersistentGrantStore grants;
    @TempDir private java.nio.file.Path warehouse;
    private org.pangrova.coordinator.metadata.MetadataBootstrap metadata;
    private org.pangrova.coordinator.metadata.PluginConnectorHandleFactory connectorFactory;
    private final CoordinatorSessionContext actor = new CoordinatorSessionContext("write-observation",
            Map.of("catalog", "default", "schema", "default"),
            new UserIdentity("alice", Set.of("external-group"), Map.of(), Instant.EPOCH), "tenant-a");

    @BeforeEach
    void initializeCurrentAuthority() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_observation");
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("writer"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "writer"));
        grants = new PersistentGrantStore(dataSource);
        for (String table : List.of("target_rows", "source_rows")) {
            grants.addGrant(PrivilegeGrant.of("writer", Privilege.SELECT, "TABLE", "default", "default", table, null));
        }
        rowPolicies = new PersistentRowPolicyStore(dataSource);
        masks = new PersistentColumnMaskStore(dataSource);
        var authority = new org.pangrova.security.authz.DynamicRbacAccessControl(grants, roles,
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
        var observation = WritePolicyObservation.capture(observed, compiler, new SqlParser().parse(sql));

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
                new SqlParser().parse("DELETE FROM target_rows WHERE id = 1")))
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
        return WritePolicyObservation.capture(observed(sql), compiler, new SqlParser().parse(sql));
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
        var directory = new org.pangrova.metadata.store.jdbc.JdbcMetadataStore(dataSource);
        directory.saveCatalogDescriptor(new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                "default", org.pangrova.metadata.api.catalog.CatalogType.CONNECTOR, "iceberg",
                org.pangrova.metadata.api.catalog.CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString()),
                "", Set.of("tenant-a")));
        connectorFactory = new org.pangrova.coordinator.metadata.PluginConnectorHandleFactory(
                getClass().getClassLoader(), new org.pangrova.security.secret.SecretResolver(),
                ignored -> Optional.empty());
        metadata = org.pangrova.coordinator.metadata.MetadataBootstrap.initialize(directory, connectorFactory);
        var root = metadata.buildCalciteSchema(
                org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(actor));
        return new SqlCompiler(new PangrovaPlanner(root, List.of("default", "default")),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }
}
