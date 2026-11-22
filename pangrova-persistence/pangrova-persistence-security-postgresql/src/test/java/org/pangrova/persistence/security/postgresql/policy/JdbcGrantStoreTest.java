package org.pangrova.persistence.security.postgresql.policy;

import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates JdbcGrantStore CRUD operations against a PostgreSQL TestContainer.
 */
class JdbcGrantStoreTest extends ContainerTestBase {

    private JdbcGrantStore store;

    @BeforeEach
    void setUp() throws Exception {
        var ds = MigratedPostgres.create();
        store = new JdbcGrantStore(ds);
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
    void listGrantsForRoles() {
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));
        store.addGrant(PrivilegeGrant.of("admin", Privilege.ALL, "TABLE", "*", "*", "*", null));
        store.addGrant(PrivilegeGrant.of("analyst", Privilege.INSERT, "TABLE", "iceberg", "default", "events", null));
        store.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "TABLE", "iceberg", "default", "events", null));

        assertThat(store.listGrantsForRoles(Set.of("reader", "analyst", "unknown")))
                .extracting(PrivilegeGrant::roleName)
                .containsExactly("analyst", "analyst", "reader");
        assertThat(store.listGrantsForRoles(Set.of())).isEmpty();
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
