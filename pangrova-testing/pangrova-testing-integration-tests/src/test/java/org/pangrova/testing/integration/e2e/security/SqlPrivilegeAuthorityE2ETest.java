package org.pangrova.testing.integration.e2e.security;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Authenticated HTTP SQL → production privilege task → PostgreSQL → actual Iceberg/Native query execution. */
class SqlPrivilegeAuthorityE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Test
    void grantAndRevokeControlNativeReadsAndReceiptsThroughAuthenticatedHttp() {
        installPrivilegeSqlRuntime();
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String table = "privilege_" + suffix;
        String adminRole = "privilege_admin_" + suffix;
        String readRole = "privilege_reader_" + suffix;
        String qualified = CATALOG + "." + SCHEMA + "." + table;
        String principal = e2eUserIdentity().principal();
        var roles = new JdbcRoleStore(stateStoreDataSource());
        var grants = new JdbcGrantStore(stateStoreDataSource());
        dataSetup.createTestTable(CATALOG, SCHEMA, table, "id BIGINT, value BIGINT");
        try {
            sqlRunner.execute("INSERT INTO " + qualified + " VALUES (1, 10), (2, 20)");
            roles.createRole(Role.of(adminRole));
            roles.createRole(Role.of(readRole));
            roles.grantRoleToUser(UserRoleAssignment.of(principal, adminRole));
            roles.grantRoleToUser(UserRoleAssignment.of(principal, readRole));
            grants.addGrant(PrivilegeGrant.of(adminRole, Privilege.OWNER, "CATALOG", CATALOG, null, null, null));
            var receipt = sqlRunner.execute("GRANT SELECT ON TABLE " + qualified + " TO ROLE " + readRole);
            assertThat(receipt).hasSize(1);
            assertThat(receipt.get(0).getLong("changed_grants")).isEqualTo(1L);
            String requestId = receipt.get(0).getString("request_id");

            // Remove all broad fixture privileges before proving the SQL-issued grant actually enables Native reads.
            restrictDefaultSqlAuthority();
            roles.revokeRoleFromUser(principal, adminRole);
            String query = "SELECT SUM(value) AS total FROM " + qualified;
            assertThat(sqlRunner.execute(query).get(0).getLong("total")).isEqualTo(30L);
            assertThat(sqlRunner.execute("SHOW PRIVILEGE MUTATION '" + requestId + "'").get(0).getString("status"))
                    .isEqualTo("CONFIRMED");
            assertThat(sqlRunner.executeExpectingError("GRANT SELECT ON TABLE " + qualified + " TO ROLE " + adminRole)
                    .message()).contains("WITH GRANT OPTION");

            // Both standalone branch/tag forms pass the same exact target authorization as ALTER TABLE.
            assertThat(sqlRunner.executeExpectingError("CREATE TAG forbidden_tag ON " + qualified).message())
                    .contains("ALTER");
            assertThat(sqlRunner.executeExpectingError("CREATE BRANCH forbidden_branch ON " + qualified).message())
                    .contains("ALTER");
            assertThat(sqlRunner.executeExpectingError("CALL " + CATALOG + ".warmup_table('" + SCHEMA + "."
                    + table + "', 'victim', 'ignored')").message()).contains("warmup_table requires");
            var warmup = sqlRunner.execute("CALL " + CATALOG + ".warmup_table('" + SCHEMA + "', '" + table + "')");
            assertThat(warmup.get(0).getLong("rows_touched")).isEqualTo(2L);

            String view = qualified + "_view";
            assertThat(sqlRunner.executeExpectingError("CREATE VIEW " + view + " AS SELECT 1 AS id").message())
                    .contains("CREATE_VIEW");
            roles.grantRoleToUser(UserRoleAssignment.of(principal, adminRole));
            sqlRunner.execute("GRANT CREATE_VIEW ON SCHEMA " + CATALOG + "." + SCHEMA + " TO ROLE " + readRole);
            roles.revokeRoleFromUser(principal, adminRole);
            sqlRunner.execute("CREATE VIEW " + view + " AS SELECT 1 AS id");
            try {
                assertThat(sqlRunner.executeExpectingError("CREATE OR REPLACE VIEW " + view
                        + " AS SELECT 2 AS id").message()).contains("ALTER");
                assertThat(sqlRunner.executeExpectingError("DROP VIEW " + view).message()).contains("DROP");
            } finally {
                roles.grantRoleToUser(UserRoleAssignment.of(principal, adminRole));
                sqlRunner.execute("DROP VIEW " + view);
                sqlRunner.execute("REVOKE CREATE_VIEW ON SCHEMA " + CATALOG + "." + SCHEMA + " FROM ROLE " + readRole + " RESTRICT");
            }
            sqlRunner.execute("REVOKE SELECT ON TABLE " + qualified + " FROM ROLE " + readRole + " RESTRICT");
            roles.revokeRoleFromUser(principal, adminRole);
            assertThat(sqlRunner.executeExpectingError(query).message()).contains("lacks SELECT");
            assertThat(grants.listGrantsForRole(readRole)).isEmpty();
            var unconfirmed = sqlRunner.execute("SHOW PRIVILEGE MUTATION '" + UUID.randomUUID() + "'").get(0);
            assertThat(unconfirmed.getString("status")).isEqualTo("UNCONFIRMED");
            assertThat(unconfirmed.getRaw("changed_grants")).isNull();
        } finally {
            restoreDefaultSqlAuthority();
            roles.dropRole(adminRole);
            roles.dropRole(readRole);
            sqlRunner.execute("DROP TABLE " + qualified);
        }
    }

    @Test
    void missingBearerCredentialCannotSubmitPrivilegeSql() throws Exception {
        installPrivilegeSqlRuntime();
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + coordinatorHttpPort()
                        + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"sql\":\"GRANT SELECT ON TABLE iceberg_catalog.default.orders TO reader\"}"))
                .build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(401);
    }
}
