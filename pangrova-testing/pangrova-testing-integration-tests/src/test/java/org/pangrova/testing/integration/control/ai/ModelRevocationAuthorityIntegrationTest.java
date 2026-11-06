package org.pangrova.testing.integration.control.ai;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.ai.control.embedding.EmbeddingRegistryAccessDeniedException;
import org.pangrova.ai.control.store.jdbc.embedding.JdbcEmbeddingModelRegistry;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that the real registry and deferred publication authority observe the same revocation. */
class ModelRevocationAuthorityIntegrationTest extends ContainerTestBase {
    private static final String ADMIN_GROUP = "pangrova_admin";
    private static final String USAGE_GROUP = "pangrova_usage";
    private static final UserIdentity ADMIN =
            new UserIdentity("admin", Set.of("idp-admin-group"), Map.of(), Instant.EPOCH);
    private static final UserIdentity USER =
            new UserIdentity("user", Set.of("idp-user-group"), Map.of(), Instant.EPOCH);
    private JdbcEmbeddingModelRegistry registry;
    private DataSource dataSource;
    private JdbcRoleStore roles;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of(ADMIN_GROUP));
        roles.createRole(Role.of(USAGE_GROUP));
        roles.grantRoleToUser(UserRoleAssignment.of(ADMIN.principal(), ADMIN_GROUP));
        roles.grantRoleToUser(UserRoleAssignment.of(USER.principal(), USAGE_GROUP));
        registry = new JdbcEmbeddingModelRegistry(dataSource, ADMIN_GROUP, USAGE_GROUP);
        registry.initSchema();
    }

    @Test
    void directRoleDeletionAndRecreationCannotRestoreModelUsageOrDeferredAuthority() throws Exception {
        registry.create(boundModel("m", 128), ADMIN);
        var observed = observeModel(USER);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate(
                    "DELETE FROM pangrova_roles WHERE role_name='" + USAGE_GROUP + "'"));
        }
        assertTrue(roles.createRole(Role.of(USAGE_GROUP)));
        roles.grantRoleToUser(UserRoleAssignment.of(USER.principal(), USAGE_GROUP));
        assertThrows(EmbeddingRegistryAccessDeniedException.class, () -> registry.find("m", USER));
        assertTrue(registry.find("m", ADMIN).isPresent());
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThrows(org.pangrova.security.api.error.AuthorizationDeniedException.class,
                    () -> ModelAuthorityFixtures.models().requireCurrent(
                            connection, USER.principal(), observed));
        }
        assertModelRoles(ADMIN_GROUP, null);
    }

    private List<ModelSnapshot> observeModel(UserIdentity user)
            throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            return ModelAuthorityFixtures.models().observe(
                    connection, user.principal(), Set.of("m"));
        }
    }

    private void assertModelRoles(String adminRole, String usageRole) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery(
                     "SELECT admin_role,usage_role FROM pangrova_embedding_models WHERE model_name='m'")) {
            assertTrue(rows.next());
            assertEquals(adminRole, rows.getString(1));
            assertEquals(usageRole, rows.getString(2));
        }
    }

    private static EmbeddingModelDescriptor model(String name, int dimension) {
        return model(name, dimension, 0);
    }

    private static EmbeddingModelDescriptor model(String name, int dimension, int version) {
        return new EmbeddingModelDescriptor(
                name,
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://api.example.com/v1/embeddings",
                        "secret-ref-key"),
                new EmbeddingModelDescriptor.EmbeddingContract(dimension, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(32, 100),
                version, EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    private static EmbeddingModelDescriptor boundModel(String name, int dimension) {
        EmbeddingModelDescriptor descriptor = model(name, dimension);
        return new EmbeddingModelDescriptor(
                descriptor.name(), descriptor.route(), descriptor.contract(), descriptor.limits(),
                descriptor.version(), new EmbeddingModelDescriptor.DeploymentFence(
                        "model-v7", "04" + "00".repeat(31)));
    }

}
