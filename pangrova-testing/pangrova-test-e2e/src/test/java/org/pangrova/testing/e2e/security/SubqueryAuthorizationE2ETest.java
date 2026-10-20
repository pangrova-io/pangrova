package org.pangrova.testing.e2e.security;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Authenticated HTTP/JDBC authority and real Native reads inside scalar and correlated subqueries. */
class SubqueryAuthorizationE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "subquery_authority";
    private static final String SCHEMA = "default";
    private String visible;
    private String hidden;
    private String role;
    private PersistentRoleStore roles;
    private PersistentGrantStore grants;
    private PersistentRowPolicyStore policies;
    private PersistentColumnMaskStore masks;
    private boolean rewired;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void createSourcesAndScopedReader() {
        String unique = UUID.randomUUID().toString().replace("-", "");
        visible = "visible_" + unique;
        hidden = "hidden_" + unique;
        for (String table : List.of(visible, hidden)) {
            sqlRunner.executeDdl("CREATE TABLE " + qualified(table) + " (id BIGINT, amount BIGINT)");
            sqlRunner.execute("INSERT INTO " + qualified(table) + " VALUES (1, 10), (2, 20)");
        }
        roles = new PersistentRoleStore(stateStoreDataSource());
        roles.initSchema();
        role = "subquery_reader_" + unique;
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        grants = new PersistentGrantStore(stateStoreDataSource());
        policies = new PersistentRowPolicyStore(stateStoreDataSource());
        masks = new PersistentColumnMaskStore(stateStoreDataSource());
        grant(visible);
    }

    @AfterEach
    void restoreAuthorityAndDropSources() {
        try {
            awaitQueriesDrained();
        } finally {
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
                "id = 1", List.of(role), true, 1));
        bindAuthority();
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isEqualTo(10));
        assertThat(sqlRunner.execute(queries().get(1))).singleElement()
                .satisfies(row -> assertThat(row.getLong("id")).isEqualTo(1));
        masks.saveColumnMask(new ColumnMask(0, "zero_amount", CATALOG, SCHEMA, hidden, "amount",
                "CAST(0 AS BIGINT)", List.of(role), List.of(), true));
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isZero());
        clearPolicies();
        assertThat(sqlRunner.execute(queries().get(0))).hasSize(2)
                .allSatisfy(row -> assertThat(row.getLong("total")).isEqualTo(30));
        assertThat(sqlRunner.execute(queries().get(1))).hasSize(2);
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
