package org.pangrova.testing.integration.e2e.security;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.security.api.tenant.*;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

/** Real authenticated HTTP SQL, production adapter, canonical membership and current authorization. */
class TenantMembershipSqlE2ETest extends SqlE2ETestBase {
    @Override
    protected List<org.pangrova.testing.integration.e2e.config.ConnectorTestConfig> connectorConfigs() {
        return List.of(org.pangrova.testing.integration.e2e.config.ConnectorTestConfig.icebergLocal(
                "iceberg_catalog", "default", icebergWarehouse().toString()));
    }

    @Test
    void tenantSqlChangesMembershipAndNeverNamespaceRestrictions() {
        installTenantSqlRuntime();
        var store = new JdbcTenantStore(stateStoreDataSource());
        String owner = e2eUserIdentity().principal();
        var previous = store.getTenantForUser(owner);
        String tenant = "Tenant." + UUID.randomUUID().toString().replace("-", "");
        String principal = "User." + UUID.randomUUID().toString().replace("-", "");
        new org.pangrova.security.core.tenant.TenantLifecycleManager(store).onboard(tenant, tenant, owner);
        var definition = store.getTenant(tenant).orElseThrow();
        var persisted = definition;
        store.addUserToTenant(tenant, owner, TenantRole.OWNER);
        String grant = "GRANT TENANT \"" + tenant + "\" TO \"" + principal + "\"";
        String revoke = "REVOKE TENANT \"" + tenant + "\" FROM \"" + principal + "\"";
        try {
            assertThat(sqlRunner.execute(grant).get(0).getBoolean("changed")).isTrue();
            assertThat(sqlRunner.execute(grant).get(0).getBoolean("changed")).isFalse();
            assertThat(store.getTenantForUser(principal)).hasValueSatisfying(value -> {
                assertThat(value.tenantId()).isEqualTo(tenant);
                assertThat(value.role()).isEqualTo(TenantRole.MEMBER);
            });
            assertThat(store.getTenant(tenant)).contains(persisted);
            store.addUserToTenant(tenant, owner, TenantRole.MEMBER);
            assertThat(sqlRunner.executeExpectingError(revoke).message()).contains("Current OWNER or ADMIN");
            assertThat(store.getTenantForUser(principal)).isPresent();
            store.addUserToTenant(tenant, owner, TenantRole.OWNER);
            assertThat(sqlRunner.execute(revoke).get(0).getBoolean("changed")).isTrue();
            assertThat(sqlRunner.execute(revoke).get(0).getBoolean("changed")).isFalse();
            assertThat(store.getTenantForUser(principal)).isEmpty();
            assertThat(store.getTenant(tenant)).contains(persisted);
        } finally {
            store.removeUserFromTenant(tenant, principal);
            if (previous.isPresent()) {
                var old = previous.orElseThrow();
                store.addUserToTenant(old.tenantId(), owner, old.role());
            } else store.removeUserFromTenant(tenant, owner);
            store.updateTenant(new Tenant(tenant, tenant, TenantStatus.DELETED, null, List.of(), List.of(),
                    null, Map.of(), definition.createdAt(), owner));
        }
    }
}
