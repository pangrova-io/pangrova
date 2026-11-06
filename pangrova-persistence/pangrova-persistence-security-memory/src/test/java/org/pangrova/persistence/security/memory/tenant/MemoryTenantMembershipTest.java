package org.pangrova.persistence.security.memory.tenant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.security.api.tenant.*;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemoryTenantMembershipTest {
    private TenantStore store;
    private static final String TENANT = "Case.Tenant";
    private static final UserIdentity ADMIN = actor("owner");

    @BeforeEach
    void setup() {
        store = new InMemoryTenantStore();
        store.createTenant(tenant(TENANT, TenantStatus.ACTIVE));
        store.addUserToTenant(TENANT, "owner", TenantRole.OWNER);
    }

    @Test
    void changesCanonicalMembershipWithoutMutatingNamespacesOrQuota() {
        Tenant before = store.getTenant(TENANT).orElseThrow();
        var grant = mutation("Member.Name", TenantMembershipMutation.Operation.GRANT);
        assertThat(store.mutateMembership(ADMIN, grant)).isTrue();
        assertThat(store.mutateMembership(ADMIN, grant)).isFalse();
        assertThat(store.getTenantForUser("Member.Name")).hasValueSatisfying(current -> {
            assertThat(current.tenantId()).isEqualTo(TENANT);
            assertThat(current.role()).isEqualTo(TenantRole.MEMBER);
        });
        assertThat(store.getTenant(TENANT)).contains(before);
        var revoke = mutation("Member.Name", TenantMembershipMutation.Operation.REVOKE);
        assertThat(store.mutateMembership(ADMIN, revoke)).isTrue();
        assertThat(store.mutateMembership(ADMIN, revoke)).isFalse();
        assertThat(store.getTenantForUser("Member.Name")).isEmpty();
        assertThat(store.getTenant(TENANT)).contains(before);
    }

    @Test
    void checksCurrentAuthorityInsteadOfClaimsAndRechecksIdempotentCalls() {
        var request = mutation("member", TenantMembershipMutation.Operation.GRANT);
        assertThatThrownBy(() -> store.mutateMembership(new UserIdentity("stranger", Set.of("ADMIN", "OWNER"),
                Map.of("tenant", TENANT), Instant.EPOCH), request)).isInstanceOf(AuthorizationDeniedException.class);
        store.addUserToTenant(TENANT, "admin", TenantRole.ADMIN);
        assertThat(store.mutateMembership(actor("admin"), request)).isTrue();
        store.removeUserFromTenant(TENANT, "admin");
        assertThatThrownBy(() -> store.mutateMembership(actor("admin"), request))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThatThrownBy(() -> store.mutateMembership(actor("member"), request))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThatThrownBy(() -> store.mutateMembership(null, request))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void refusesCrossTenantTransferAndPrivilegedRevocation() {
        store.createTenant(tenant("other", TenantStatus.ACTIVE));
        store.addUserToTenant("other", "outsider", TenantRole.MEMBER);
        for (var operation : TenantMembershipMutation.Operation.values()) {
            assertThatThrownBy(() -> store.mutateMembership(ADMIN, mutation("outsider", operation)))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
        assertThat(store.getTenantForUser("outsider").orElseThrow().tenantId()).isEqualTo("other");
        assertThat(store.mutateMembership(ADMIN, mutation("owner", TenantMembershipMutation.Operation.GRANT))).isFalse();
        assertThatThrownBy(() -> store.mutateMembership(ADMIN, mutation("owner", TenantMembershipMutation.Operation.REVOKE)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(store.getTenantForUser("owner").orElseThrow().role()).isEqualTo(TenantRole.OWNER);
        assertThatThrownBy(() -> store.mutateMembership(ADMIN,
                new TenantMembershipMutation("other", "member", TenantMembershipMutation.Operation.GRANT)))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void inactiveTenantCannotChangeMembers() {
        for (var status : List.of(TenantStatus.SUSPENDED, TenantStatus.DELETED)) {
            store.updateTenant(tenant(TENANT, status));
            assertThatThrownBy(() -> store.mutateMembership(ADMIN, mutation("member", TenantMembershipMutation.Operation.GRANT)))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
        assertThat(store.getTenantForUser("member")).isEmpty();
    }

    @Test
    void concurrentSameMembershipHasOneChange() throws Exception {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        var barrier = new java.util.concurrent.CyclicBarrier(2);
        try {
            java.util.concurrent.Callable<Boolean> grant = () -> {
                barrier.await(10, java.util.concurrent.TimeUnit.SECONDS);
                return store.mutateMembership(ADMIN, mutation("member", TenantMembershipMutation.Operation.GRANT));
            };
            var a = pool.submit(grant);
            var b = pool.submit(grant);
            assertThat(List.of(a.get(15, java.util.concurrent.TimeUnit.SECONDS), b.get(15, java.util.concurrent.TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private static TenantMembershipMutation mutation(String principal, TenantMembershipMutation.Operation operation) {
        return new TenantMembershipMutation(TENANT, principal, operation);
    }
    private static UserIdentity actor(String principal) { return new UserIdentity(principal, Set.of(), Map.of(), Instant.EPOCH); }
    private static Tenant tenant(String id, TenantStatus status) {
        return new Tenant(id, id, status, null, List.of("catalog"), List.of("schema"), null, Map.of(), Instant.EPOCH, "creator");
    }
}
