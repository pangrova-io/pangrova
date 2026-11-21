package org.pangrova.persistence.security.postgresql.tenant;

import org.pangrova.security.api.enums.TenantMembershipOperation;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.exception.TenantMembershipOutcomeUnknownException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.security.api.tenant.*;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcTenantMembershipTest {
    private TenantStore store;
    private javax.sql.DataSource source;
    private static final String TENANT = "Case.Tenant";
    private static final UserIdentity ADMIN = actor("owner");

    @BeforeEach
    void setup() throws java.sql.SQLException {
        source = org.pangrova.testing.testkit.postgres.MigratedPostgres.create();
        store = new JdbcTenantStore(source);
        store.createTenant(tenant(TENANT, TenantStatus.ACTIVE));
        store.addUserToTenant(TENANT, "owner", TenantRole.OWNER);
    }

    @Test
    void changesCanonicalMembershipWithoutMutatingNamespacesOrQuota() {
        Tenant before = store.getTenant(TENANT).orElseThrow();
        var grant = mutation("Member.Name", TenantMembershipOperation.GRANT);
        assertThat(store.mutateMembership(ADMIN, grant)).isTrue();
        assertThat(store.mutateMembership(ADMIN, grant)).isFalse();
        assertThat(store.getTenantForUser("Member.Name")).hasValueSatisfying(current -> {
            assertThat(current.tenantId()).isEqualTo(TENANT);
            assertThat(current.role()).isEqualTo(TenantRole.MEMBER);
        });
        assertThat(store.getTenant(TENANT)).contains(before);
        var revoke = mutation("Member.Name", TenantMembershipOperation.REVOKE);
        assertThat(store.mutateMembership(ADMIN, revoke)).isTrue();
        assertThat(store.mutateMembership(ADMIN, revoke)).isFalse();
        assertThat(store.getTenantForUser("Member.Name")).isEmpty();
        assertThat(store.getTenant(TENANT)).contains(before);
    }

    @Test
    void checksCurrentAuthorityInsteadOfClaimsAndRechecksIdempotentCalls() {
        var request = mutation("member", TenantMembershipOperation.GRANT);
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
        for (var operation : TenantMembershipOperation.values()) {
            assertThatThrownBy(() -> store.mutateMembership(ADMIN, mutation("outsider", operation)))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
        assertThat(store.getTenantForUser("outsider").orElseThrow().tenantId()).isEqualTo("other");
        assertThat(store.mutateMembership(ADMIN, mutation("owner", TenantMembershipOperation.GRANT))).isFalse();
        assertThatThrownBy(() -> store.mutateMembership(ADMIN, mutation("owner", TenantMembershipOperation.REVOKE)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(store.getTenantForUser("owner").orElseThrow().role()).isEqualTo(TenantRole.OWNER);
        assertThatThrownBy(() -> store.mutateMembership(ADMIN,
                new TenantMembershipMutation("other", "member", TenantMembershipOperation.GRANT)))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void inactiveTenantCannotChangeMembers() {
        for (var status : List.of(TenantStatus.SUSPENDED, TenantStatus.DELETED)) {
            store.updateTenant(tenant(TENANT, status));
            assertThatThrownBy(() -> store.mutateMembership(ADMIN, mutation("member", TenantMembershipOperation.GRANT)))
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
                return store.mutateMembership(ADMIN, mutation("member", TenantMembershipOperation.GRANT));
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

    @Test
    void failedMutationRollsBackMembershipAndPreservesOriginalFailure() {
        var broken = org.pangrova.testing.testkit.postgres.JdbcMutationFault.afterMutation(source,
                "INSERT INTO tenant_user_memberships", new java.sql.SQLException("injected after insert"), null);
        assertThatThrownBy(() -> new JdbcTenantStore(broken).mutateMembership(ADMIN,
                mutation("member", TenantMembershipOperation.GRANT)))
                .hasRootCauseMessage("injected after insert");
        assertThat(store.getTenantForUser("member")).isEmpty();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void commitAcknowledgementLossIsUnknownAndCurrentMembershipCanBeConfirmed(boolean uncheckedDriverFailure) {
        var intercepted = intercept(source, connection -> proxy(java.sql.Connection.class, connection, (method, args) -> {
            Object result = invoke(connection, method, args);
            if (method.getName().equals("commit")) {
                if (uncheckedDriverFailure) throw new IllegalStateException("pool lost acknowledgement");
                throw new java.sql.SQLException("lost acknowledgement", "08006");
            }
            return result;
        }));
        var request = mutation("member", TenantMembershipOperation.GRANT);
        assertThatThrownBy(() -> new JdbcTenantStore(intercepted).mutateMembership(ADMIN, request))
                .isInstanceOf(TenantMembershipOutcomeUnknownException.class);
        assertThat(store.getTenantForUser("member").orElseThrow().tenantId()).isEqualTo(TENANT);
        assertThat(store.mutateMembership(ADMIN, request)).isFalse();
    }

    @Test
    void administratorRevokedDuringGateWaitCannotChangeMembers() throws Exception {
        var reachedGate = new java.util.concurrent.CountDownLatch(1);
        var intercepted = intercept(source, connection -> proxy(java.sql.Connection.class, connection, (method, args) -> {
            Object result = invoke(connection, method, args);
            if (method.getName().equals("createStatement")) {
                java.sql.Statement statement = (java.sql.Statement) result;
                return proxy(java.sql.Statement.class, statement, (operation, parameters) -> {
                    if (operation.getName().equals("execute")
                            && parameters[0].toString().contains("pg_advisory_xact_lock")) reachedGate.countDown();
                    return invoke(statement, operation, parameters);
                });
            }
            return result;
        }));
        var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var holder = source.getConnection()) {
            holder.setAutoCommit(false);
            org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate.lockMutation(holder);
            var pending = pool.submit(() -> new JdbcTenantStore(intercepted).mutateMembership(ADMIN,
                    mutation("member", TenantMembershipOperation.GRANT)));
            assertThat(reachedGate.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            try (var update = holder.createStatement()) {
                update.executeUpdate("DELETE FROM tenant_user_memberships WHERE user_principal='owner'");
            }
            holder.commit();
            assertThatThrownBy(() -> pending.get(10, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(AuthorizationDeniedException.class);
            assertThat(store.getTenantForUser("member")).isEmpty();
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private static javax.sql.DataSource intercept(javax.sql.DataSource source,
            java.util.function.Function<java.sql.Connection, java.sql.Connection> wrapper) {
        return proxy(javax.sql.DataSource.class, source, (method, args) -> {
            Object result = invoke(source, method, args);
            return method.getName().equals("getConnection") ? wrapper.apply((java.sql.Connection) result) : result;
        });
    }
    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
    }
    private static <T> T proxy(Class<T> type, T target, Invocation invocation) {
        return type.cast(java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> invocation.call(method, args)));
    }
    @FunctionalInterface
    private interface Invocation { Object call(java.lang.reflect.Method method, Object[] args) throws Throwable; }

    private static TenantMembershipMutation mutation(String principal, TenantMembershipOperation operation) {
        return new TenantMembershipMutation(TENANT, principal, operation);
    }
    private static UserIdentity actor(String principal) { return new UserIdentity(principal, Set.of(), Map.of(), Instant.EPOCH); }
    private static Tenant tenant(String id, TenantStatus status) {
        return new Tenant(id, id, status, null, List.of("catalog"), List.of("schema"), null, Map.of(), Instant.EPOCH, "creator");
    }
}
