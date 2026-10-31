package org.pangrova.ai.control.store.jdbc.embedding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.store.jdbc.JdbcCurrentDirectRoleReader;
import org.pangrova.security.store.jdbc.PersistentRoleStore;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcCurrentModelAuthorityTest extends ContainerTestBase {
    private static final String ADMIN_ROLE = "model_admin";
    private static final String USAGE_ROLE = "model_usage";
    private static final String PRINCIPAL = "model_user";
    private static final String SIGNATURE = "04" + "00".repeat(31);
    private static final UserIdentity ADMIN = new UserIdentity("model_owner", Set.of(), Map.of(), Instant.EPOCH);
    private DataSource dataSource;
    private PersistentRoleStore roles;
    private JdbcEmbeddingModelRegistry registry;
    private final JdbcCurrentModelAuthority authority =
            new JdbcCurrentModelAuthority(new JdbcCurrentDirectRoleReader());

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of(ADMIN_ROLE));
        roles.createRole(Role.of(USAGE_ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(ADMIN.principal(), ADMIN_ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(PRINCIPAL, USAGE_ROLE));
        registry = new JdbcEmbeddingModelRegistry(dataSource, ADMIN_ROLE, USAGE_ROLE);
        registry.initSchema();
        registry.create(model("model-a", new EmbeddingModelDescriptor.DeploymentFence("model-v7", SIGNATURE)), ADMIN);
    }

    @Test
    void observesSortedImmutableExactContractsAndRetainsAuthorizationGateUntilCallerEndsTransaction() throws Exception {
        registry.create(model("model-z", new EmbeddingModelDescriptor.DeploymentFence("model-v7", SIGNATURE)), ADMIN);
        try (var connection = dataSource.getConnection(); var mutation = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            mutation.setAutoCommit(false);
            var snapshots = authority.observe(connection, PRINCIPAL, Set.of("model-z", "model-a"));
            assertThat(snapshots).extracting(ModelSnapshot::logicalName).containsExactly("model-a", "model-z");
            for (var snapshot : snapshots) {
                assertThat(snapshot.incarnation()).isNotBlank();
                assertThat(snapshot.configurationVersion()).isZero();
                assertThat(snapshot.deploymentVersion()).isEqualTo("model-v7");
                assertThat(snapshot.signature()).isEqualTo(SIGNATURE);
            }
            assertThatThrownBy(() -> snapshots.add(snapshots.get(0)))
                    .isInstanceOf(UnsupportedOperationException.class);
            authority.requireCurrent(connection, PRINCIPAL, snapshots);
            assertThat(tryMutationGate(mutation)).isFalse();
            connection.rollback();
            assertThat(tryMutationGate(mutation)).isTrue();
            mutation.rollback();
        }
    }

    @ParameterizedTest
    @EnumSource(ChangedContract.class)
    void rejectsEachChangedContractDimension(ChangedContract changed) throws Exception {
        var observed = observeCommitted();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate(changed.sql)).isEqualTo(1);
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> authority.requireCurrent(connection, PRINCIPAL, observed))
                    .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("Model contract changed");
            connection.rollback();
        }
    }

    @Test
    void revokedDirectRoleCannotObserveOrReuseAReceipt() throws Exception {
        var observed = observeCommitted();
        assertThat(roles.revokeRoleFromUser(PRINCIPAL, USAGE_ROLE)).isTrue();
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> authority.observe(connection, PRINCIPAL, Set.of("model-a")))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThatThrownBy(() -> authority.requireCurrent(connection, PRINCIPAL, observed))
                    .isInstanceOf(AuthorizationDeniedException.class);
            connection.rollback();
        }
    }

    @Test
    void usesCallerConnectionForRolesAndUncommittedModelStateWithoutEndingTheTransaction() throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var calls = new AtomicInteger();
            var reader = new JdbcCurrentDirectRoleReader();
            var observedAuthority = new JdbcCurrentModelAuthority((supplied, principal) -> {
                assertThat(supplied).isSameAs(connection);
                assertThat(principal).isEqualTo(PRINCIPAL);
                calls.incrementAndGet();
                return reader.requireCurrentRoles(supplied, principal);
            });
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE pangrova_embedding_models SET version=7 WHERE model_name='model-a'");
            }
            var observed = observedAuthority.observe(connection, PRINCIPAL, Set.of("model-a"));
            assertThat(observed.get(0).configurationVersion()).isEqualTo(7);
            observedAuthority.requireCurrent(connection, PRINCIPAL, observed);
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE pangrova_embedding_models SET version=8 WHERE model_name='model-a'");
            }
            assertThatThrownBy(() -> observedAuthority.requireCurrent(connection, PRINCIPAL, observed))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThat(calls).hasValue(3);
            assertThat(connection.isClosed()).isFalse();
            assertThat(connection.getAutoCommit()).isFalse();
            assertThat(modelVersion(connection)).isEqualTo(8);
            try (var independent = dataSource.getConnection()) {
                assertThat(modelVersion(independent)).isZero();
            }
            connection.rollback();
            assertThat(modelVersion(connection)).isZero();
        }
    }

    @Test
    void emptyValidationIsANoopWhileEmptyObservationStillRequiresTransactionAndCurrentRoles() throws Exception {
        try (var connection = dataSource.getConnection()) {
            assertThatCode(() -> authority.requireCurrent(connection, "unassigned", List.of()))
                    .doesNotThrowAnyException();
            assertThatThrownBy(() -> authority.observe(connection, PRINCIPAL, Set.of()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("explicit transaction");
            connection.setAutoCommit(false);
            assertThat(authority.observe(connection, PRINCIPAL, Set.of())).isEmpty();
            assertThatThrownBy(() -> authority.observe(connection, "unassigned", Set.of()))
                    .isInstanceOf(AuthorizationDeniedException.class);
            connection.rollback();
        }
    }

    @Test
    void absentOrUnboundModelsCannotBecomeAuthoritySnapshots() throws Exception {
        registry.create(model("unbound", EmbeddingModelDescriptor.DeploymentFence.unbound()), ADMIN);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> authority.observe(connection, PRINCIPAL, Set.of("absent")))
                    .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("absent or revoked");
            assertThatThrownBy(() -> authority.observe(connection, PRINCIPAL, Set.of("unbound")))
                    .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("exact deployment");
            connection.rollback();
        }
    }

    private List<ModelSnapshot> observeCommitted() throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var models = authority.observe(connection, PRINCIPAL, Set.of("model-a"));
            connection.commit();
            return models;
        }
    }

    private static int modelVersion(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                "SELECT version FROM pangrova_embedding_models WHERE model_name='model-a'")) {
            assertThat(rows.next()).isTrue();
            return rows.getInt(1);
        }
    }

    private static boolean tryMutationGate(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                "SELECT pg_try_advisory_xact_lock(207578817, 1)")) {
            assertThat(rows.next()).isTrue();
            return rows.getBoolean(1);
        }
    }

    private static EmbeddingModelDescriptor model(String name, EmbeddingModelDescriptor.DeploymentFence fence) {
        return new EmbeddingModelDescriptor(name,
                new EmbeddingModelDescriptor.ProviderRoute("openai-compatible", "https://model.example/v1", "secret"),
                new EmbeddingModelDescriptor.EmbeddingContract(128, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(32, 100), 0, fence);
    }

    private enum ChangedContract {
        INCARNATION("UPDATE pangrova_embedding_models SET incarnation_id='replacement-model' "
                + "WHERE model_name='model-a'"),
        CONFIGURATION("UPDATE pangrova_embedding_models SET version=1 WHERE model_name='model-a'"),
        DEPLOYMENT("UPDATE pangrova_embedding_models SET deployment_version='model-v8' WHERE model_name='model-a'"),
        SIGNATURE("UPDATE pangrova_embedding_models SET deployment_signature_sha256='05"
                + "00".repeat(31) + "' WHERE model_name='model-a'");

        private final String sql;

        ChangedContract(String sql) {
            this.sql = sql;
        }
    }
}
