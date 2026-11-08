package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.parser.core.parse.AntlrSqlParser;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.security.api.error.AccessDeniedException;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production SQL parser, dispatcher, bootstrap registration, policy reader and PostgreSQL workflow. */
class PrivilegeSqlExecutionTest extends ContainerTestBase {
    private DataSource source;
    private JdbcGrantStore grants;
    private MetadataExecutionFactory factory;
    private DynamicRbacAccessControl accessControl;

    @BeforeEach
    void initialize() throws Exception {
        source = MigratedPostgres.create();
        grants = new JdbcGrantStore(source);
        var roles = new JdbcRoleStore(source);
        for (String role : Set.of("owners", "delegator", "reader")) roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of("admin", "owners"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "delegator"));
        roles.grantRoleToUser(UserRoleAssignment.of("bob", "reader"));
        grants.addGrant(PrivilegeGrant.of("owners", Privilege.OWNER, "CATALOG", "lake", null, null, null));
        factory = new MetadataExecutionFactory(Map.of());
        SecurityAdministrationBootstrap.register(factory, config(false), source);
        accessControl = policyReader();
    }

    @Test
    void actualSqlRoutesThroughProductionDispatcherAndCrossCoordinatorReadsCurrentGrants() {
        var dispatcher = dispatcher(factory);
        var bob = identity("bob");
        var secondCoordinator = policyReader();
        assertThatThrownBy(() -> secondCoordinator.checkCanSelectFromTable(bob, "lake", "sales", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class);
        execute(dispatcher, "GRANT SELECT ON TABLE orders TO ROLE delegator WITH GRANT OPTION", "admin");
        execute(dispatcher, "GRANT SELECT ON TABLE lake.sales.orders TO reader", "alice");
        secondCoordinator.checkCanSelectFromTable(bob, "lake", "sales", "orders");
        execute(dispatcher, "REVOKE SELECT ON TABLE lake.sales.orders FROM delegator CASCADE", "admin");
        assertThatThrownBy(() -> secondCoordinator.checkCanSelectFromTable(bob, "lake", "sales", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(grants.listGrantsForRole("reader")).isEmpty();
    }

    @Test
    void sqlReturnContainsDurableReceiptAndReplayUsesSameQueryIdentity() {
        var session = session("admin");
        var context = new StatementExecutionContext(null, session.queryId(), session);
        var statement = new AntlrSqlParser().parse("GRANT SELECT, INSERT ON TABLE sales.orders TO reader");
        var result = factory.execute(statement, context);
        assertThat(result.rows()).singleElement().satisfies(row -> {
            assertThat(row.getString("request_id")).isEqualTo(session.queryId());
            assertThat(row.getInt("changed_grants")).isEqualTo(2);
            assertThat(row.getLong("authorization_revision")).isPositive();
        });
        assertThat(factory.execute(statement, context).rows()).isEqualTo(result.rows());
        assertThat(grants.listGrantsForRole("reader")).hasSize(2);
    }

    @Test
    void sqlReceiptLookupConfirmsOnlyOwnOutcomeEvenAfterAuthorityRevocation() {
        var session = session("admin");
        var context = new StatementExecutionContext(null, session.queryId(), session);
        var parser = new AntlrSqlParser();
        factory.execute(parser.parse("GRANT SELECT ON TABLE orders TO reader"), context);
        new JdbcRoleStore(source).revokeRoleFromUser("admin", "owners");
        var lookup = parser.parse("SHOW PRIVILEGE MUTATION '" + session.queryId() + "'");
        assertThat(factory.execute(lookup, context).rows().get(0).getString("status")).isEqualTo("CONFIRMED");
        var bob = session("bob");
        var other = new StatementExecutionContext(null, bob.queryId(), bob);
        assertThat(factory.execute(lookup, other).rows().get(0).getString("status")).isEqualTo("UNCONFIRMED");
        var unknown = parser.parse("SHOW PRIVILEGE MUTATION '" + UUID.randomUUID() + "'");
        assertThat(factory.execute(unknown, context).rows().get(0).getString("status")).isEqualTo("UNCONFIRMED");
    }

    @Test
    void namespaceDefaultsAndQuotedNamesAreResolvedWithoutSplittingIdentifierContents() {
        var roleStore = new JdbcRoleStore(source);
        roleStore.createRole(Role.of("reader.with.dot"));
        execute(dispatcher(factory), "GRANT SELECT ON SCHEMA sales TO ROLE \"reader.with.dot\"", "admin");
        assertThat(grants.listGrantsForRole("reader.with.dot")).singleElement().satisfies(grant -> {
            assertThat(grant.objectType()).isEqualTo("SCHEMA");
            assertThat(grant.catalog()).isEqualTo("lake");
            assertThat(grant.schema()).isEqualTo("sales");
            assertThat(grant.table()).isNull();
        });
    }

    @Test
    void absentAuthenticationAndBypassCannotUsePrivilegeSql() {
        for (var configuration : List.of(config(true), CoordinatorBootstrapConfig.withDefaults("unsecured", 0))) {
            var unavailable = new MetadataExecutionFactory(Map.of());
            SecurityAdministrationBootstrap.register(unavailable, configuration, source);
            assertThatThrownBy(() -> execute(dispatcher(unavailable),
                    "GRANT SELECT ON TABLE orders TO reader", "admin"))
                    .isInstanceOf(AccessDeniedException.class);
            assertThatThrownBy(() -> execute(dispatcher(unavailable), "GRANT TENANT tenant TO bob", "admin"))
                    .isInstanceOf(AccessDeniedException.class);
        }
        var anonymous = new QuerySession(UUID.randomUUID().toString(), Map.of(), null, null, Optional.empty(),
                new OtelExecutionTraceContextFactory().capture());
        assertThatThrownBy(() -> dispatcher(factory).tryDispatch(null,
                "GRANT SELECT ON TABLE orders TO reader", anonymous.queryId(), anonymous))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(grants.listGrantsForRole("reader")).isEmpty();
    }

    @Test
    void sqlCannotEscalateScopeOrTransferOwnership() {
        execute(dispatcher(factory), "GRANT SELECT ON TABLE orders TO delegator WITH GRANT OPTION", "admin");
        assertThatThrownBy(() -> execute(dispatcher(factory), "GRANT SELECT ON SCHEMA sales TO reader", "alice"))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThatThrownBy(() -> execute(dispatcher(factory), "GRANT OWNER ON TABLE orders TO reader", "admin"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> execute(dispatcher(factory), "GRANT READ_VOLUME ON TABLE orders TO reader", "admin"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(grants.listGrantsForRole("reader")).isEmpty();
    }

    @Test
    void tenantSqlUsesTheProductionRegistrationAndCurrentMembershipAuthority() {
        var store = new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source);
        store.createTenant(new org.pangrova.security.api.tenant.Tenant("Tenant.One", "display",
                org.pangrova.security.api.tenant.TenantStatus.ACTIVE, null, List.of("lake"), List.of("sales"),
                null, Map.of(), Instant.EPOCH, "admin"));
        store.addUserToTenant("Tenant.One", "admin", org.pangrova.security.api.tenant.TenantRole.OWNER);
        var dispatcher = dispatcher(factory);
        execute(dispatcher, "GRANT TENANT \"Tenant.One\" TO bob", "admin");
        assertThat(store.getTenantForUser("bob").orElseThrow().roleName()).isEqualTo("MEMBER");
        assertThat(store.getTenant("Tenant.One").orElseThrow().allowedSchemas()).containsExactly("sales");
        assertThatThrownBy(() -> execute(dispatcher, "GRANT TENANT \"Tenant.One\" TO alice", "bob"))
                .isInstanceOf(AuthorizationDeniedException.class);
        execute(dispatcher, "REVOKE TENANT \"Tenant.One\" FROM bob", "admin");
        assertThat(store.getTenantForUser("bob")).isEmpty();
    }

    private StatementDispatcher dispatcher(MetadataExecutionFactory executionFactory) {
        return new StatementDispatcher(new StatementDispatcherContext(new SqlCompiler(), null,
                List.of(executionFactory), List.of(), accessControl));
    }

    private static void execute(StatementDispatcher dispatcher, String sql, String actor) {
        var session = session(actor);
        var parsed = new AntlrSqlParser().parse(sql);
        assertThat(dispatcher.isRoutable(parsed)).isTrue();
        dispatcher.tryDispatch(null, sql, session.queryId(), session);
    }

    private DynamicRbacAccessControl policyReader() {
        return new DynamicRbacAccessControl(new JdbcGrantStore(source), new JdbcRoleStore(source),
                new RowLevelSecurityFilter(new JdbcRowPolicyStore(source)),
                new ColumnMaskingProcessor(new JdbcColumnMaskStore(source)));
    }

    private static QuerySession session(String actor) {
        return new QuerySession(UUID.randomUUID().toString(), Map.of("catalog", "lake", "schema", "sales"),
                identity(actor), null, Optional.empty(), new OtelExecutionTraceContextFactory().capture());
    }

    private static UserIdentity identity(String actor) {
        return new UserIdentity(actor, Set.of(), Map.of(), Instant.now());
    }

    private static CoordinatorBootstrapConfig config(boolean bypass) {
        var security = bypass ? CoordinatorBootstrapConfig.SecurityConfig.noop()
                : new CoordinatorBootstrapConfig.SecurityConfig(
                        CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2, "test-secret",
                        Map.of("issuerUri", "https://issuer.example.test"));
        return new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig("secured", 0, 30_000, CoordinatorRole.COMBINED, "0.0.0.0"),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, false, false, false, false, false, false),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(), null, security, null);
    }
}
