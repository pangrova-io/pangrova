package org.pangrova.testing.integration.e2e.security;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.table.core.ownership.TableControlService;
import org.pangrova.table.core.config.TableControlSettings;
import org.pangrova.metadata.service.RegistryTablePolicyAuthority;
import org.pangrova.metadata.adapter.security.TableControlColumnMaskStore;
import org.pangrova.metadata.service.TableControlRowPolicyStore;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Authenticated HTTP/JDBC authority and real Native reads inside scalar and correlated subqueries. */
class SubqueryAuthorizationE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "subquery_authority";
    private static final String SCHEMA = "default";
    private String visible;
    private String hidden;
    private String role;
    private JdbcRoleStore roles;
    private JdbcGrantStore grants;
    private TableControlRowPolicyStore policies;
    private TableControlColumnMaskStore masks;
    private TableControlService previousControl;
    private boolean rewired;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void createSourcesAndScopedReader() {
        var registry = metadataBootstrap.catalogRegistry();
        previousControl = registry.tableControl();
        registry.setTableControlService(new TableControlService(new JdbcTableControlStore(stateStoreDataSource()),
                registry::requireCurrentCatalogIdentity, new TableControlSettings(true, true, java.util.Set.of())));
        String unique = UUID.randomUUID().toString().replace("-", "");
        visible = "visible_" + unique;
        hidden = "hidden_" + unique;
        for (String table : List.of(visible, hidden)) {
            sqlRunner.executeDdl("CREATE TABLE " + qualified(table) + " (id BIGINT, amount BIGINT)");
            sqlRunner.execute("INSERT INTO " + qualified(table) + " VALUES (1, 10), (2, 20)");
        }
        roles = new JdbcRoleStore(stateStoreDataSource());
        role = "subquery_reader_" + unique;
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        grants = new JdbcGrantStore(stateStoreDataSource());
        policies = new TableControlRowPolicyStore(new RegistryTablePolicyAuthority(registry),
                new JdbcRowPolicyStore(stateStoreDataSource()));
        masks = new TableControlColumnMaskStore(new RegistryTablePolicyAuthority(registry),
                new JdbcColumnMaskStore(stateStoreDataSource()));
        grant(visible);
    }

    @AfterEach
    void restoreAuthorityAndDropSources() {
        try {
            awaitQueriesDrained();
        } finally {
            try {
                clearPolicies();
                if (rewired) clearFeatureSecurity();
                rewired = false;
                if (roles != null) {
                    if (grants != null) grants.listGrantsForRole(role).forEach(grant -> grants.revokeGrant(grant.id()));
                    roles.dropRole(role);
                }
                for (String table : List.of(visible, hidden)) {
                    sqlRunner.executeDdl("DROP TABLE IF EXISTS " + qualified(table));
                }
                awaitQueriesDrained();
            } finally {
                if (previousControl != null) {
                    metadataBootstrap.catalogRegistry().setTableControlService(previousControl);
                }
            }
        }
    }

    private static void awaitQueriesDrained() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(queryManager.getActiveQueryIds()).isEmpty();
            assertThat(queryManager.getActiveQueryCount()).isZero();
        });
    }

    @Test
    void scalarAndExistsInputsRequireTheirOwnGrantAndObserveRevocation() {
        bindAuthority();
        for (String query : queries()) {
            assertThat(sqlRunner.executeExpectingError(query).message()).contains("SECURITY-0002", hidden);
        }
        grant(hidden);
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isEqualTo(30));
        assertThat(sqlRunner.execute(queries().get(1))).hasSize(2);
        assertThat(grants.revokeGrants(role, Privilege.SELECT, CATALOG, SCHEMA, hidden)).isEqualTo(1);
        for (String query : queries()) {
            assertThat(sqlRunner.executeExpectingError(query).message()).contains("SECURITY-0002", hidden);
        }
    }

    @Test
    void innerRowFilterAndMaskApplyBeforeSubqueryAggregationAndMembership() {
        grant(hidden);
        policies.savePolicy(new RowPolicy(0, "first_row", CATALOG, SCHEMA, hidden,
                "id = 1", List.of(role), true, 1, null));
        bindAuthority();
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isEqualTo(10));
        assertThat(sqlRunner.execute(queries().get(1))).singleElement()
                .satisfies(row -> assertThat(row.getLong("id")).isEqualTo(1));
        masks.saveColumnMask(new ColumnMask(0, "zero_amount", CATALOG, SCHEMA, hidden, "amount",
                "CAST(0 AS BIGINT)", List.of(role), List.of(), true, null));
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isZero());
        clearPolicies();
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isEqualTo(30));
        assertThat(sqlRunner.execute(queries().get(1))).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"id = 1", "id < 0"})
    void constantCorrelationKeysPreserveDuplicatesNullsAndEmptyAggregates(String policy) {
        sqlRunner.execute("INSERT INTO " + qualified(hidden) + " VALUES (1, 5), (NULL, 7)");
        sqlRunner.execute("INSERT INTO " + qualified(visible) + " VALUES (3, 30), (NULL, 40)");
        grant(hidden);
        policies.savePolicy(new RowPolicy(0, "selected_rows", CATALOG, SCHEMA, hidden,
                policy, List.of(role), true, 1, null));
        bindAuthority();
        boolean populated = policy.equals("id = 1");
        assertThat(sqlRunner.execute(queries().get(1))).hasSize(populated ? 1 : 0);
        String absence = "SELECT v.id FROM " + qualified(visible)
                + " v WHERE NOT EXISTS (SELECT 1 FROM " + qualified(hidden) + " h WHERE h.id = v.id)";
        assertThat(sqlRunner.execute(absence)).hasSize(populated ? 3 : 4);
        for (String aggregate : List.of("COUNT(*)", "COUNT(*) + 1", "SUM(amount)")) {
            String query = "SELECT v.id, (SELECT " + aggregate + " FROM " + qualified(hidden)
                    + " h WHERE h.id = v.id) AS total FROM " + qualified(visible) + " v";
            assertThat(sqlRunner.execute(query)).hasSize(4).allSatisfy(row -> {
                boolean matched = populated && Long.valueOf(1).equals(row.getRaw("id"));
                if (aggregate.equals("SUM(amount)")) {
                    assertThat(row.getRaw("total")).isEqualTo(matched ? 15L : null);
                } else {
                    long count = matched ? 2 : 0;
                    assertThat(row.getLong("total")).isEqualTo(
                            aggregate.equals("COUNT(*)") ? count : count + 1);
                }
            });
        }
        masks.saveColumnMask(new ColumnMask(0, "masked_key", CATALOG, SCHEMA, hidden, "id",
                "CAST(9 AS BIGINT)", List.of(role), List.of(), true, null));
        assertThat(sqlRunner.execute(queries().get(1))).isEmpty();
        assertThat(sqlRunner.execute(absence)).hasSize(4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CAST(1 AS BIGINT)", "CAST(NULL AS BIGINT)"})
    void computedCorrelationKeysUseMaskedInnerValues(String mask) {
        grant(hidden);
        bindAuthority();
        String inner = " FROM " + qualified(hidden) + " h WHERE h.id + 1 = v.id";
        String exists = "SELECT v.id FROM " + qualified(visible)
                + " v WHERE EXISTS (SELECT 1" + inner + ")";
        assertThat(sqlRunner.execute(exists)).singleElement()
                .satisfies(row -> assertThat(row.getLong("id")).isEqualTo(2));
        masks.saveColumnMask(new ColumnMask(0, "computed_key", CATALOG, SCHEMA, hidden, "id",
                mask, List.of(role), List.of(), true, null));
        boolean nullKey = mask.contains("NULL");
        assertThat(sqlRunner.execute(exists)).hasSize(nullKey ? 0 : 1);
        assertThat(sqlRunner.execute("SELECT v.id FROM " + qualified(visible)
                + " v WHERE NOT EXISTS (SELECT 1" + inner + ")")).hasSize(nullKey ? 2 : 1);
        for (String aggregate : List.of("COUNT(*)", "COUNT(*) + 1", "SUM(amount)")) {
            assertThat(sqlRunner.execute("SELECT v.id, (SELECT " + aggregate + inner + ") AS total FROM "
                    + qualified(visible) + " v")).hasSize(2).allSatisfy(row -> {
                boolean matched = !nullKey && row.getLong("id") == 2;
                if (aggregate.equals("SUM(amount)")) {
                    assertThat(row.getRaw("total")).isEqualTo(matched ? 30L : null);
                } else {
                    long count = matched ? 2 : 0;
                    assertThat(row.getLong("total")).isEqualTo(
                            aggregate.equals("COUNT(*)") ? count : count + 1);
                }
            });
        }
    }

    private List<String> queries() {
        return List.of("SELECT (SELECT SUM(amount) FROM " + qualified(hidden)
                        + ") AS total FROM " + qualified(visible),
                "SELECT v.id FROM " + qualified(visible) + " v WHERE EXISTS (SELECT 1 FROM "
                        + qualified(hidden) + " h WHERE h.id = v.id)");
    }

    private void grant(String table) {
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", CATALOG, SCHEMA, table, null));
    }

    private void bindAuthority() {
        rewired = true;
        configureFeatureSecurity(new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(policies), new ColumnMaskingProcessor(masks)));
    }

    private void clearPolicies() {
        if (policies != null) policies.getPolicies(CATALOG, SCHEMA, hidden)
                .forEach(policy -> policies.deletePolicy(policy.policyId()));
        if (masks != null) masks.getTableMasks(CATALOG, SCHEMA, hidden)
                .forEach(mask -> masks.deleteMask(mask.maskId()));
    }

    private static String qualified(String table) { return CATALOG + "." + SCHEMA + "." + table; }
}
