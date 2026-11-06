package org.pangrova.persistence.security.postgresql.grant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.grant.PrivilegeMutation;
import org.pangrova.security.api.grant.PrivilegeMutationOutcomeUnknownException;
import org.pangrova.security.api.grant.PrivilegeScope;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.security.api.grant.PrivilegeMutation.Operation.*;
import static org.pangrova.security.api.grant.PrivilegeMutation.RevokeBehavior.*;
import static org.pangrova.security.api.grant.PrivilegeScope.ObjectType.*;

/** Real canonical-schema tests: no in-memory authorization or mocked persistence. */
class JdbcPrivilegeMutationRepositoryTest extends ContainerTestBase {
    private static final PrivilegeScope TABLE_SCOPE = new PrivilegeScope(TABLE, "lake", "sales", "orders");
    private DataSource source;
    private JdbcPrivilegeMutationRepository repository;
    private JdbcGrantStore grants;
    private JdbcRoleStore roles;

    @BeforeEach
    void initialize() throws Exception {
        source = MigratedPostgres.create();
        repository = new JdbcPrivilegeMutationRepository(source);
        grants = new JdbcGrantStore(source);
        roles = new JdbcRoleStore(source);
        for (String role : Set.of("owners", "delegator", "analyst", "reader", "independent")) {
            roles.createRole(Role.of(role));
        }
        roles.grantRoleToUser(UserRoleAssignment.of("admin", "owners"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "delegator"));
        roles.grantRoleToUser(UserRoleAssignment.of("bob", "independent"));
        grants.addGrant(PrivilegeGrant.of("owners", Privilege.OWNER, "CATALOG", "lake", null, null, null));
    }

    @Test
    void atomicMultiRoleGrantAndCanonicalReplay() {
        var command = command(GRANT, Set.of(Privilege.SELECT, Privilege.INSERT), TABLE_SCOPE,
                Set.of("analyst", "reader"), false, RESTRICT);
        var first = repository.apply(actor("admin"), command);
        assertThat(first.changedGrants()).isEqualTo(4);
        assertThat(first.authorizationRevision()).isPositive();
        var replay = new JdbcPrivilegeMutationRepository(source).apply(actor("admin"), command);
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.recordedAt()).isEqualTo(first.recordedAt());
        assertThat(replay.authorizationRevision()).isEqualTo(first.authorizationRevision());
        assertThat(grants.listGrants()).hasSize(5);
        assertThat(receipts()).isEqualTo(1);
    }

    @Test
    void sameIdCannotChangePayloadOrPrincipal() {
        var command = grant("analyst", false);
        repository.apply(actor("admin"), command);
        var other = new PrivilegeMutation(command.requestId(), GRANT, Set.of(Privilege.INSERT), TABLE_SCOPE,
                command.roles(), false, RESTRICT);
        assertThatThrownBy(() -> repository.apply(actor("admin"), other)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.apply(actor("alice"), command))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(grants.listGrantsForRole("analyst")).singleElement()
                .satisfies(grant -> assertThat(grant.privilege()).isEqualTo(Privilege.SELECT));
    }

    @Test
    void unknownRecipientRollsBackWholeStatement() {
        var command = command(GRANT, Set.of(Privilege.SELECT), TABLE_SCOPE,
                Set.of("analyst", "missing"), false, RESTRICT);
        assertThatThrownBy(() -> repository.apply(actor("admin"), command))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(grants.listGrants()).hasSize(1);
        assertThat(receipts()).isZero();
    }

    @Test
    void ordinaryPrivilegeCannotDelegateAndGroupsAreNotRoleNames() {
        repository.apply(actor("admin"), grant("delegator", false));
        assertThatThrownBy(() -> repository.apply(actor("alice"), grant("analyst", false)))
                .isInstanceOf(AuthorizationDeniedException.class);
        var forgedRoleClaim = new UserIdentity("stranger", Set.of("owners"), Map.of(), Instant.now());
        assertThatThrownBy(() -> repository.apply(forgedRoleClaim, grant("analyst", false)))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void currentExplicitGroupMappingCanDelegate() {
        roles.mapGroupToRole(GroupRoleMapping.of("finance", "owners"));
        var groupActor = new UserIdentity("group-user", Set.of("finance"), Map.of(), Instant.now());
        repository.apply(groupActor, grant("analyst", false));
        assertThat(grants.listGrantsForRole("analyst")).hasSize(1);
        roles.unmapGroupFromRole("finance", "owners");
        assertThatThrownBy(() -> repository.apply(groupActor, grant("reader", false)))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void restrictPreservesEntireDelegationChainAndCascadeRemovesOnlyThatChain() {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("admin"), grant("independent", true));
        repository.apply(actor("alice"), grant("analyst", false));
        repository.apply(actor("bob"), grant("analyst", false));
        assertThatThrownBy(() -> repository.apply(actor("admin"), revoke("delegator", false)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Dependent");
        assertThat(grants.listGrantsForRole("analyst")).hasSize(2);
        assertThat(receipts()).isEqualTo(4);
        var result = repository.apply(actor("admin"), revoke("delegator", true));
        assertThat(result.changedGrants()).isEqualTo(2);
        assertThat(grants.listGrantsForRole("analyst")).singleElement()
                .satisfies(grant -> assertThat(grant.grantedBy()).isEqualTo("bob"));
        assertThat(grants.listGrantsForRole("owners")).hasSize(1);
    }

    @Test
    void revokeGrantOptionPreservesUseButRemovesDelegates() {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("alice"), grant("analyst", false));
        var restrict = command(REVOKE_GRANT_OPTION, Set.of(Privilege.SELECT), TABLE_SCOPE,
                Set.of("delegator"), false, RESTRICT);
        assertThatThrownBy(() -> repository.apply(actor("admin"), restrict)).hasMessageContaining("Dependent");
        var cascade = command(REVOKE_GRANT_OPTION, Set.of(Privilege.SELECT), TABLE_SCOPE,
                Set.of("delegator"), false, CASCADE);
        assertThat(repository.apply(actor("admin"), cascade).changedGrants()).isEqualTo(2);
        assertThat(grants.listGrantsForRole("delegator")).singleElement()
                .satisfies(grant -> assertThat(grant.withGrantOption()).isFalse());
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
        assertThatThrownBy(() -> repository.apply(actor("alice"), grant("reader", false)))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void revokeOptionOfOverlappingRecipientsCountsDeletedDescendantsOnce() {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("alice"), grant("analyst", true));
        var revoke = command(REVOKE_GRANT_OPTION, Set.of(Privilege.SELECT), TABLE_SCOPE,
                Set.of("delegator", "analyst"), false, CASCADE);
        assertThat(repository.apply(actor("admin"), revoke).changedGrants()).isEqualTo(2);
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
        assertThat(grants.listGrantsForRole("delegator")).hasSize(1);
    }

    @Test
    void nonOwnerCannotRevokeAnotherGrantorsRows() {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("admin"), grant("analyst", false));
        assertThat(repository.apply(actor("alice"), revoke("analyst", true)).changedGrants()).isZero();
        repository.apply(actor("alice"), grant("analyst", false));
        assertThat(repository.apply(actor("alice"), revoke("analyst", true)).changedGrants()).isEqualTo(1);
        assertThat(grants.listGrantsForRole("analyst")).singleElement()
                .satisfies(grant -> assertThat(grant.grantedBy()).isEqualTo("admin"));
    }

    @Test
    void exactNamespaceRevokeDoesNotDeleteTableOrColumnGrants() {
        var schema = new PrivilegeScope(SCHEMA, "lake", "sales", null);
        repository.apply(actor("admin"), command(GRANT, Set.of(Privilege.SELECT), schema,
                Set.of("analyst"), false, RESTRICT));
        repository.apply(actor("admin"), grant("analyst", false));
        grants.addGrant(PrivilegeGrant.of("analyst", Privilege.SELECT, "COLUMN", "lake", "sales", "orders", "id"));
        var revoke = command(REVOKE, Set.of(Privilege.SELECT), schema, Set.of("analyst"), false, CASCADE);
        assertThat(repository.apply(actor("admin"), revoke).changedGrants()).isEqualTo(1);
        assertThat(grants.listGrantsForRole("analyst")).hasSize(2);
    }

    @Test
    void narrowAuthorityCannotGrantNamespaceOrOtherObjectKinds() {
        repository.apply(actor("admin"), grant("delegator", true));
        var broad = command(GRANT, Set.of(Privilege.SELECT), new PrivilegeScope(SCHEMA, "lake", "sales", null),
                Set.of("analyst"), false, RESTRICT);
        assertThatThrownBy(() -> repository.apply(actor("alice"), broad))
                .isInstanceOf(AuthorizationDeniedException.class);
        var volume = command(GRANT, Set.of(Privilege.READ_VOLUME),
                new PrivilegeScope(VOLUME, "lake", "sales", "orders"),
                Set.of("analyst"), false, RESTRICT);
        assertThatThrownBy(() -> repository.apply(actor("alice"), volume))
                .isInstanceOf(AuthorizationDeniedException.class);
        var profile = command(GRANT, Set.of(Privilege.MANAGE_STORAGE_PROFILE),
                new PrivilegeScope(STORAGE_PROFILE, "lake", null, null), Set.of("analyst"), false, RESTRICT);
        assertThatThrownBy(() -> repository.apply(actor("admin"), profile))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void regrantUpgradesOptionWithoutDowngradeOrDuplicateRows() {
        repository.apply(actor("admin"), grant("delegator", false));
        assertThat(repository.apply(actor("admin"), grant("delegator", true)).changedGrants()).isEqualTo(1);
        assertThat(repository.apply(actor("admin"), grant("delegator", false)).changedGrants()).isZero();
        assertThat(grants.listGrantsForRole("delegator")).singleElement()
                .satisfies(grant -> assertThat(grant.withGrantOption()).isTrue());
    }

    @Test
    void directTrustedGrantRemovalCannotLeaveChildren() {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("alice"), grant("analyst", false));
        grants.revokeGrant(grants.listGrantsForRole("delegator").get(0).id());
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
    }

    @Test
    void coordinatedObjectRenamePreservesDelegationOnTheRenamedObject() throws Exception {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("alice"), grant("analyst", false));
        try (var connection = source.getConnection(); var update = connection.createStatement()) {
            update.executeUpdate("UPDATE security_grants SET table_name='renamed_orders' "
                    + "WHERE object_type='TABLE' AND catalog_name='lake' AND schema_name='sales' AND table_name='orders'");
        }
        assertThat(grants.listGrantsForRole("delegator")).singleElement()
                .satisfies(grant -> assertThat(grant.table()).isEqualTo("renamed_orders"));
        assertThat(grants.listGrantsForRole("analyst")).singleElement()
                .satisfies(grant -> assertThat(grant.table()).isEqualTo("renamed_orders"));
    }

    @Test
    void changingOnlySupportingScopeRevokesNowUncoveredChildren() throws Exception {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("alice"), grant("analyst", false));
        try (var connection = source.getConnection(); var update = connection.createStatement()) {
            update.executeUpdate("UPDATE security_grants SET table_name='other_table' WHERE role_name='delegator'");
        }
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
        assertThat(grants.listGrantsForRole("delegator")).hasSize(1);
    }

    @Test
    void trustedOptionRemovalCannotLeaveDelegatedAuthority() throws Exception {
        repository.apply(actor("admin"), grant("delegator", true));
        repository.apply(actor("alice"), grant("analyst", false));
        try (var connection = source.getConnection(); var update = connection.createStatement()) {
            update.executeUpdate("UPDATE security_grants SET with_grant_option=FALSE WHERE role_name='delegator'");
        }
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
    }

    @Test
    void auditFailureRollsBackAuthorityChanges() throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION reject_privilege_receipt() RETURNS trigger LANGUAGE plpgsql "
                    + "AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$");
            statement.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON security_privilege_mutations "
                    + "FOR EACH ROW EXECUTE FUNCTION reject_privilege_receipt()");
        }
        assertThatThrownBy(() -> repository.apply(actor("admin"), grant("analyst", false)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Failed applying");
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
        assertThat(receipts()).isZero();
    }

    @Test
    void lostCommitAcknowledgementConfirmsSameReceiptWithoutSecondMutation() {
        var failOnce = new AtomicBoolean(true);
        var intercepted = intercept(source, connection -> proxy(Connection.class, connection, (method, args) -> {
            Object result = invoke(connection, method, args);
            if (method.getName().equals("commit") && failOnce.getAndSet(false)) {
                throw new SQLException("injected lost commit acknowledgement", "08006");
            }
            return result;
        }));
        var command = grant("analyst", false);
        assertThatThrownBy(() -> new JdbcPrivilegeMutationRepository(intercepted).apply(actor("admin"), command))
                .isInstanceOf(PrivilegeMutationOutcomeUnknownException.class);
        assertThat(repository.findReceipt(actor("admin"), command.requestId())).isPresent();
        assertThat(repository.findReceipt(actor("alice"), command.requestId())).isEmpty();
        assertThat(repository.apply(actor("admin"), command).replayed()).isTrue();
        assertThat(grants.listGrantsForRole("analyst")).hasSize(1);
        assertThat(receipts()).isEqualTo(1);
    }

    @Test
    void authorityRevokedWhileWaitingIsRecheckedAfterGate() throws Exception {
        var reachedGate = new CountDownLatch(1);
        var intercepted = intercept(source, connection -> proxy(Connection.class, connection, (method, args) -> {
            Object result = invoke(connection, method, args);
            if (method.getName().equals("createStatement")) {
                Statement statement = (Statement) result;
                return proxy(Statement.class, statement, (operation, parameters) -> {
                    if (operation.getName().equals("execute")
                            && parameters[0].toString().contains("pg_advisory_xact_lock")) {
                        reachedGate.countDown();
                    }
                    return invoke(statement, operation, parameters);
                });
            }
            return result;
        }));
        var executor = Executors.newSingleThreadExecutor();
        try (var holder = source.getConnection()) {
            holder.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(holder);
            var future = executor.submit(() -> new JdbcPrivilegeMutationRepository(intercepted)
                    .apply(actor("admin"), grant("analyst", false)));
            assertThat(reachedGate.await(10, TimeUnit.SECONDS)).isTrue();
            try (var update = holder.createStatement()) {
                update.executeUpdate("DELETE FROM pangrova_user_roles WHERE user_id='admin'");
            }
            holder.commit();
            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(AuthorizationDeniedException.class);
            assertThat(grants.listGrantsForRole("analyst")).isEmpty();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private int receipts() {
        try (var connection = source.getConnection(); var query = connection.createStatement();
                var result = query.executeQuery("SELECT count(*) FROM security_privilege_mutations")) {
            result.next();
            return result.getInt(1);
        } catch (SQLException failure) {
            throw new AssertionError(failure);
        }
    }

    private static UserIdentity actor(String name) { return new UserIdentity(name, Set.of(), Map.of(), Instant.now()); }
    private static PrivilegeMutation grant(String role, boolean option) {
        return command(GRANT, Set.of(Privilege.SELECT), TABLE_SCOPE, Set.of(role), option, RESTRICT);
    }
    private static PrivilegeMutation revoke(String role, boolean cascade) {
        return command(REVOKE, Set.of(Privilege.SELECT), TABLE_SCOPE, Set.of(role),
                false, cascade ? CASCADE : RESTRICT);
    }
    private static PrivilegeMutation command(PrivilegeMutation.Operation operation, Set<Privilege> privileges,
            PrivilegeScope scope, Set<String> roles, boolean option, PrivilegeMutation.RevokeBehavior behavior) {
        return new PrivilegeMutation(UUID.randomUUID(), operation, privileges, scope, roles, option, behavior);
    }

    private static DataSource intercept(DataSource source,
            java.util.function.Function<Connection, Connection> wrapper) {
        return proxy(DataSource.class, source, (method, args) -> {
            Object result = invoke(source, method, args);
            return method.getName().equals("getConnection") ? wrapper.apply((Connection) result) : result;
        });
    }
    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }
    private static <T> T proxy(Class<T> type, T target, Invocation invocation) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> invocation.call(method, args)));
    }
    @FunctionalInterface
    private interface Invocation { Object call(java.lang.reflect.Method method, Object[] args) throws Throwable; }
}
