package org.pangrova.testing.integration.e2e.write;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.table.core.ownership.TableControlService;
import org.pangrova.table.core.config.TableControlSettings;
import org.pangrova.metadata.service.RegistryTablePolicyAuthority;
import org.pangrova.metadata.adapter.security.TableControlColumnMaskStore;
import org.pangrova.metadata.service.TableControlRowPolicyStore;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
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
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Public authenticated SQL must select through policy but preserve untouched physical CoW values. */
@E2ETest
class DmlCopyOnWritePolicyE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "cow_policy";
    private static final String SCHEMA = "default";
    private static final List<PhysicalRow> ORIGINAL = List.of(
            new PhysicalRow(101, 1, 11L, null, "original-101"),
            new PhysicalRow(205, 2, 22L, 7L, "original-205"),
            new PhysicalRow(309, 1, 33L, 5L, "original-309"),
            new PhysicalRow(413, 2, 44L, null, "original-413"),
            new PhysicalRow(517, 1, null, null, "original-517"));

    private String table;
    private String role;
    private JdbcRoleStore roles;
    private TableControlRowPolicyStore policies;
    private TableControlColumnMaskStore masks;
    private TableControlService previousControl;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void createRealNativeWrittenTarget() {
        var registry = metadataBootstrap.catalogRegistry();
        previousControl = registry.tableControl();
        registry.setTableControlService(new TableControlService(new JdbcTableControlStore(stateStoreDataSource()),
                registry::requireCurrentCatalogIdentity, new TableControlSettings(true, true, Set.of())));
        table = "target_" + UUID.randomUUID().toString().replace("-", "");
        executeDdl("CREATE TABLE " + target()
                + " (id BIGINT, tenant BIGINT, score BIGINT, nullable_probe BIGINT, note VARCHAR)"
                + " TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write',"
                + " 'write.delete.mode'='copy-on-write', 'write.merge.mode'='copy-on-write')");
        executeDml("INSERT INTO " + target() + " VALUES"
                + " (101,1,11,NULL,'original-101'), (205,2,22,7,'original-205'),"
                + " (309,1,33,5,'original-309'), (413,2,44,NULL,'original-413'),"
                + " (517,1,NULL,NULL,'original-517')");
        assertThat(readRows()).containsExactlyElementsOf(ORIGINAL);
    }

    @AfterEach
    void releasePersistentScenarioPolicies() {
        try {
            if (policies != null) removePolicies();
            if (roles != null) {
                clearFeatureSecurity();
                roles.dropRole(role);
            }
        } finally {
            if (previousControl != null) {
                metadataBootstrap.catalogRegistry().setTableControlService(previousControl);
            }
        }
    }

    @Test
    void updatePredicateUsesMaskAndNullSemanticsWithoutRewritingSurvivors() {
        installPolicies();
        executeDml("UPDATE " + target() + " SET note='updated' WHERE score=0 AND nullable_probe>0");
        removePolicies();
        assertThat(readRows()).containsExactly(ORIGINAL.get(0), ORIGINAL.get(1),
                ORIGINAL.get(2).withNote("updated"), ORIGINAL.get(3), ORIGINAL.get(4));
    }

    @Test
    void updateWithoutWhereChangesOnlyVisibleRowsAndPreservesUnassignedRawScores() {
        installPolicies();
        executeDml("UPDATE " + target() + " SET note='visible-update'");
        removePolicies();
        assertThat(readRows()).containsExactly(ORIGINAL.get(0).withNote("visible-update"), ORIGINAL.get(1),
                ORIGINAL.get(2).withNote("visible-update"), ORIGINAL.get(3), ORIGINAL.get(4).withNote("visible-update"));
    }

    @Test
    void deletePredicateKeepsOutsidePolicyAndUnknownNullablePredicateRows() {
        installPolicies();
        executeDml("DELETE FROM " + target() + " WHERE score=0 AND nullable_probe>0");
        removePolicies();
        assertThat(readRows()).containsExactly(ORIGINAL.get(0), ORIGINAL.get(1), ORIGINAL.get(3), ORIGINAL.get(4));
    }

    @Test
    void deleteWithoutWhereKeepsEveryOutsidePolicyPhysicalRow() {
        installPolicies();
        executeDml("DELETE FROM " + target());
        removePolicies();
        assertThat(readRows()).containsExactly(ORIGINAL.get(1), ORIGINAL.get(3));
    }

    @Test
    void mergeMatchedPredicateUsesProtectedReadButRetainsOriginalUnassignedColumns() {
        String source = target() + "_source";
        executeDdl("CREATE TABLE " + source + " (id BIGINT, note VARCHAR)");
        executeDml("INSERT INTO " + source + " VALUES (101,'merge-101'), (205,'must-not-update-hidden'),"
                + " (309,'merge-309'), (517,'must-not-update-null-predicate')");
        installPolicies();
        executeDml("MERGE INTO " + target() + " t USING " + source + " s ON t.id=s.id"
                + " WHEN MATCHED AND t.score=0 AND t.nullable_probe>0 THEN UPDATE SET note=s.note");
        removePolicies();
        assertThat(readRows()).containsExactly(ORIGINAL.get(0), ORIGINAL.get(1),
                ORIGINAL.get(2).withNote("merge-309"), ORIGINAL.get(3), ORIGINAL.get(4));
    }

    @Test
    void mergeMatchedDeleteRetainsUnknownPredicatesAndPolicyHiddenPhysicalRows() {
        String source = target() + "_source";
        executeDdl("CREATE TABLE " + source + " (id BIGINT)");
        executeDml("INSERT INTO " + source + " VALUES (101), (205), (309), (517)");
        installPolicies();
        executeDml("MERGE INTO " + target() + " t USING " + source + " s ON t.id=s.id"
                + " WHEN MATCHED AND t.score=0 AND t.nullable_probe>0 THEN DELETE");
        removePolicies();
        assertThat(readRows()).containsExactly(ORIGINAL.get(0), ORIGINAL.get(1), ORIGINAL.get(3), ORIGINAL.get(4));
    }

    private void installPolicies() {
        var session = authenticatedCatalogSession(CATALOG);
        String tenant = session.getProperties().get("tenant_id");
        var tenants = new JdbcTenantStore(stateStoreDataSource());

        assertThat(tenants.getTenant(tenant)).hasValueSatisfying(current ->
                assertThat(current.status()).isEqualTo(TenantStatus.ACTIVE));
        assertThat(tenants.getTenantForUser(session.getUser())).hasValueSatisfying(membership ->
                assertThat(membership.tenant().tenantId()).isEqualTo(tenant));
        roles = new JdbcRoleStore(stateStoreDataSource());
        role = "cow_policy_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(session.getUser(), role));
        var grants = new JdbcGrantStore(stateStoreDataSource());
        grants.addGrant(PrivilegeGrant.of(role, Privilege.ALL, "SCHEMA", CATALOG, SCHEMA, null, null));
        var registry = metadataBootstrap.catalogRegistry();
        policies = new TableControlRowPolicyStore(new RegistryTablePolicyAuthority(registry),
                new JdbcRowPolicyStore(stateStoreDataSource()));
        masks = new TableControlColumnMaskStore(new RegistryTablePolicyAuthority(registry),
                new JdbcColumnMaskStore(stateStoreDataSource()));
        policies.savePolicy(new RowPolicy(0, "cow_tenant_rows", CATALOG, SCHEMA, table,
                "tenant=1", List.of(role), true, 1, null));
        masks.saveColumnMask(new ColumnMask(0, "cow_score_zero", CATALOG, SCHEMA, table, "score",
                "CAST(0 AS BIGINT)", List.of(role), List.of(), true, null));
        UUID tableId = registry.tableControl().find(CATALOG, SCHEMA, table).orElseThrow().tableId();
        assertThat(policies.getPolicies(CATALOG, SCHEMA, table)).singleElement()
                .satisfies(policy -> assertThat(policy.tableId()).isEqualTo(tableId));
        assertThat(masks.getColumnMasks(CATALOG, SCHEMA, table, "score")).singleElement()
                .satisfies(mask -> assertThat(mask.tableId()).isEqualTo(tableId));
        configureFeatureSecurity(new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks)));
        var visible = executeQuery("SELECT id,score FROM " + target() + " ORDER BY id");
        assertThat(visible).extracting(row -> row.getLong("id")).containsExactly(101L, 309L, 517L);
        assertThat(visible).extracting(row -> row.getLong("score")).containsExactly(0L, 0L, 0L);
    }

    private void removePolicies() {
        policies.getPolicies(CATALOG, SCHEMA, table).forEach(policy -> policies.deletePolicy(policy.policyId()));
        masks.getColumnMasks(CATALOG, SCHEMA, table, "score").forEach(mask -> masks.deleteMask(mask.maskId()));
        assertThat(policies.getPolicies(CATALOG, SCHEMA, table)).isEmpty();
        assertThat(masks.getColumnMasks(CATALOG, SCHEMA, table, "score")).isEmpty();
        // Keep the same authenticated actor, direct role and grants for raw verification.
    }

    private List<PhysicalRow> readRows() {
        return executeQuery("SELECT id,tenant,score,nullable_probe,note FROM " + target() + " ORDER BY id")
                .stream().map(row -> new PhysicalRow(row.getLong("id"), row.getLong("tenant"),
                        nullableLong(row.getRaw("score")), nullableLong(row.getRaw("nullable_probe")),
                        row.getString("note"))).toList();
    }

    private static Long nullableLong(Object value) {
        return value == null ? null : ((Number) value).longValue();
    }

    private String target() {
        return CATALOG + "." + SCHEMA + "." + table;
    }

    private record PhysicalRow(long id, long tenant, Long score, Long nullableProbe, String note) {
        PhysicalRow withNote(String replacement) {
            return new PhysicalRow(id, tenant, score, nullableProbe, replacement);
        }
    }
}
