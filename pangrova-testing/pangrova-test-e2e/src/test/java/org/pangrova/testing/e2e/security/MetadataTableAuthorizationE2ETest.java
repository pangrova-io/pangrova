package org.pangrova.testing.e2e.security;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual HTTP SQL and Native metadata reads must use the base table's current JDBC grants. */
class MetadataTableAuthorizationE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "metadata_authority";
    private static final String SCHEMA = "default";
    private static final List<String> SUFFIXES = List.of("snapshots", "history", "files", "manifests", "entries");
    private PersistentRoleStore roles;
    private PersistentGrantStore grants;
    private String role;
    private boolean restricted;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @AfterEach
    void restoreAuthorityAndDrainQueries() {
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(queryManager.getActiveQueryIds()).isEmpty();
                assertThat(queryManager.getActiveQueryCount()).isZero();
            });
        } finally {
            if (restricted) restoreDefaultSqlAuthority();
            if (roles != null) {
                if (grants != null) {
                    grants.listGrantsForRole(role).forEach(grant -> grants.revokeGrant(grant.id()));
                }
                roles.dropRole(role);
            }
        }
    }

    @Test
    void metadataSuffixCannotGrantReadOrSurviveBaseRevocation() {
        String table = "orders_" + UUID.randomUUID().toString().replace("-", "");
        String qualified = CATALOG + "." + SCHEMA + "." + table;
        sqlRunner.executeDdl("CREATE TABLE " + qualified + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)");
        sqlRunner.execute("INSERT INTO " + qualified + " VALUES (1, 'private'), (2, 'public')");
        roles = new PersistentRoleStore(stateStoreDataSource());
        roles.initSchema();
        role = "metadata_reader_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        grants = new PersistentGrantStore(stateStoreDataSource());
        for (String suffix : SUFFIXES) {
            grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE",
                    CATALOG, SCHEMA, table + "$" + suffix, null));
        }
        restrictDefaultSqlAuthority();
        restricted = true;
        assertMetadataDenied(qualified);

        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", CATALOG, SCHEMA, table, null));
        for (String suffix : SUFFIXES) {
            assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + qualified + "$" + suffix))
                    .singleElement().satisfies(row -> assertThat(row.getLong("n")).isPositive());
        }

        assertThat(grants.revokeGrants(role, Privilege.SELECT, CATALOG, SCHEMA, table)).isEqualTo(1);
        assertMetadataDenied(qualified);
    }

    private void assertMetadataDenied(String qualified) {
        for (String suffix : SUFFIXES) {
            assertThat(sqlRunner.executeExpectingError("SELECT COUNT(*) AS n FROM " + qualified + "$" + suffix)
                    .message()).as("metadata suffix %s", suffix).contains("SECURITY-0002");
        }
    }
}
