package org.pangrova.security.store;

import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates PersistentGrantStore CRUD operations against a PostgreSQL TestContainer.
 */
class PersistentGrantStoreTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private PersistentGrantStore store;

    @BeforeEach
    void setUp() throws Exception {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        // Drop and recreate so each test starts clean
        try (var conn = ds.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS security_grants");
        }
        store = new PersistentGrantStore(ds);
        store.initSchema();
    }

    @Test
    void addAndListGrants() {
        PrivilegeGrant grant = new PrivilegeGrant(
                null, "analyst", Privilege.SELECT, "TABLE",
                "iceberg", "default", "orders", null,
                false, false, "admin", Instant.now());

        store.addGrant(grant);

        List<PrivilegeGrant> all = store.listGrants();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).roleName()).isEqualTo("analyst");
        assertThat(all.get(0).privilege()).isEqualTo(Privilege.SELECT);
        assertThat(all.get(0).catalog()).isEqualTo("iceberg");
        assertThat(all.get(0).schema()).isEqualTo("default");
        assertThat(all.get(0).table()).isEqualTo("orders");
        assertThat(all.get(0).id()).isNotNull();
    }

    @Test
    void listGrantsForRole() {
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));
        store.addGrant(PrivilegeGrant.of("admin", Privilege.ALL, "TABLE", "*", "*", "*", null));
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.INSERT, "TABLE", "iceberg", "default", "events", null));

        List<PrivilegeGrant> analystGrants = store.listGrantsForRole("analyst");
        assertThat(analystGrants).hasSize(2);
        assertThat(analystGrants).allMatch(g -> g.roleName().equals("analyst"));
    }

    @Test
    void revokeGrantById() {
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));
        List<PrivilegeGrant> grants = store.listGrants();
        assertThat(grants).hasSize(1);

        String grantId = grants.get(0).id();
        boolean revoked = store.revokeGrant(grantId);
        assertThat(revoked).isTrue();
        assertThat(store.listGrants()).isEmpty();
    }

    @Test
    void revokeGrantByIdNotFoundReturnsFalse() {
        boolean revoked = store.revokeGrant("99999");
        assertThat(revoked).isFalse();
    }

    @Test
    void revokeGrantsByRoleAndPrivilege() {
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "events", null));
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.INSERT, "TABLE", "iceberg", "default", "orders", null));

        int count = store.revokeGrants("analyst", Privilege.SELECT, "iceberg", "default", null);
        assertThat(count).isEqualTo(2);
        // INSERT grant remains
        List<PrivilegeGrant> remaining = store.listGrants();
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get(0).privilege()).isEqualTo(Privilege.INSERT);
    }

    @Test
    void revokeGrantsByExactTable() {
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "events", null));

        int count = store.revokeGrants("analyst", Privilege.SELECT, "iceberg", "default", "orders");
        assertThat(count).isEqualTo(1);
        List<PrivilegeGrant> remaining = store.listGrants();
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get(0).table()).isEqualTo("events");
    }

    @Test
    void grantWithColumnScope() {
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "COLUMN", "iceberg", "default", "orders", "amount"));

        List<PrivilegeGrant> grants = store.listGrants();
        assertThat(grants).hasSize(1);
        assertThat(grants.get(0).column()).isEqualTo("amount");
        assertThat(grants.get(0).objectType()).isEqualTo("COLUMN");
    }
}
