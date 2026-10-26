package org.pangrova.coordinator.query.execution;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.audit.QueryLineageCapture;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.security.authz.DynamicRbacAccessControl;
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
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.ContainerTestBase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual planning and PostgreSQL policy authority; not a Native execution test. */
class ProtectedRelationCompilationIntegrationTest extends ContainerTestBase {

    private PersistentRowPolicyStore rowPolicies;
    private PersistentColumnMaskStore masks;
    private PersistentGrantStore grants;
    private QueryPipelineExecutor pipeline;
    private final CoordinatorSessionContext actor = new CoordinatorSessionContext(
            "protected-compilation", Map.of("catalog", "default", "schema", "default"),
            new UserIdentity("alice", Set.of("external-group"), Map.of(), Instant.EPOCH), "tenant-a");

    @BeforeEach
    void initializeAuthority() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("protected_compile");
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("feature-reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "feature-reader"));
        grants = new PersistentGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of("feature-reader", Privilege.SELECT,
                "TABLE", "default", "default", "orders", null));
        rowPolicies = new PersistentRowPolicyStore(dataSource);
        masks = new PersistentColumnMaskStore(dataSource);
        var authority = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(rowPolicies), new ColumnMaskingProcessor(masks));
        var security = new SecurityPolicyApplier(compiler(), new AccessControlEnforcer(authority),
                new ColumnMaskApplier(authority), new RowFilterApplier(authority));
        pipeline = new QueryPipelineExecutor(security, null, null, new QueryLineageCapture(), null, null);
    }

    @Test
    void capturesThePoliciesActuallySerializedForTheAuthenticatedRole() throws Exception {
        rowPolicies.savePolicy(policy("department = 'sales'"));
        masks.saveColumnMask(new ColumnMask(0, "department-mask", "default", "default", "orders",
                "department", "'hidden'", List.of("feature-reader"), List.of(), true, null));

        AuthorizedRelation relation = pipeline.compileAuthorizedRelation(
                "SELECT id, department FROM orders", actor);

        assertThat(relation.splitIndependent()).isTrue();
        assertThat(relation.sources()).singleElement()
                .satisfies(source -> assertThat(source.qualifiedName()).isEqualTo("default.default.orders"));
        var observed = relation.observedPolicies().get("default.default.orders");
        assertThat(observed.rowFilter()).contains("(department = 'sales')");
        assertThat(observed.columnMasks()).containsEntry("department", java.util.Optional.of("'hidden'"))
                .containsEntry("id", java.util.Optional.empty());
        var wire = io.substrait.proto.Plan.parseFrom(relation.planBytes());
        assertThat(wire.toString()).contains("sales", "hidden");
        assertThat(relation.sourceSchemas()).containsOnlyKeys("default.default.orders");
    }

    @Test
    void subsequentCompilationObservesPolicyChangesAndRevocationImmediately() {
        AuthorizedRelation first = pipeline.compileAuthorizedRelation("SELECT id FROM orders", actor);
        rowPolicies.savePolicy(policy("department = 'sales'"));
        AuthorizedRelation second = pipeline.compileAuthorizedRelation("SELECT id FROM orders", actor);
        assertThat(second.effectivePlanDigest()).isNotEqualTo(first.effectivePlanDigest());
        assertThat(second.observedPolicies().get("default.default.orders").rowFilter()).isPresent();
        rowPolicies.getPolicies("default", "default", "orders")
                .forEach(policy -> rowPolicies.deletePolicy(policy.policyId()));
        rowPolicies.savePolicy(policy("department = 'legal'"));
        AuthorizedRelation third = pipeline.compileAuthorizedRelation("SELECT id FROM orders", actor);
        assertThat(third.effectivePlanDigest()).isNotEqualTo(second.effectivePlanDigest());
        grants.revokeGrants("feature-reader", Privilege.SELECT, "default", "default", "orders");
        assertThatThrownBy(() -> pipeline.compileAuthorizedRelation("SELECT id FROM orders", actor))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class);
    }

    @Test
    void globalRelationsAreNotMisclassifiedAsIndependentMaps() {
        assertThat(pipeline.compileAuthorizedRelation("SELECT COUNT(*) AS total FROM orders", actor)
                .splitIndependent()).isFalse();
        assertThat(pipeline.compileAuthorizedRelation("SELECT id FROM orders ORDER BY id LIMIT 1", actor)
                .splitIndependent()).isFalse();
        assertThat(pipeline.compileAuthorizedRelation(
                "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id", actor).splitIndependent()).isFalse();
    }

    @Test
    void rejectsAnonymousAndWriteIntents() {
        assertThatThrownBy(() -> pipeline.compileAuthorizedRelation("SELECT id FROM orders", null))
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class);
        assertThatThrownBy(() -> pipeline.compileAuthorizedRelation("DELETE FROM orders", actor))
                .hasMessageContaining("read-only query");
    }

    private static RowPolicy policy(String expression) {
        return new RowPolicy(0, "department-filter", "default", "default", "orders",
                expression, List.of("feature-reader"), true, 1, null);
    }

    private static SqlCompiler compiler() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        AbstractTable orders = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT)
                        .add("department", SqlTypeName.VARCHAR, 100).build();
            }
        };
        schema.add("default", new AbstractSchema()).add("default", new AbstractSchema()).add("orders", orders);
        return new SqlCompiler(new PangrovaPlanner(schema, List.of("default", "default")),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }
}
