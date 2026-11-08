package org.pangrova.persistence.ai.postgresql.embedding;

import org.pangrova.ai.control.embedding.EmbeddingRegistryAccessDeniedException;
import org.pangrova.ai.control.embedding.ModelNotFoundException;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;
import org.pangrova.security.api.identity.UserIdentity;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/** Reads explicit model role policy and current persistent direct-role assignments. */
final class EmbeddingModelRoleAuthority {
    private static final JdbcCurrentDirectRoleReader CURRENT_ROLES = new JdbcCurrentDirectRoleReader();

    private EmbeddingModelRoleAuthority() {}

    static void requireRole(Connection connection, UserIdentity user, String role,
                            String operation, String modelName) throws SQLException {
        if (!roles(connection, user).contains(role)) {
            throw new EmbeddingRegistryAccessDeniedException(user.principal(), operation, modelName);
        }
    }

    static void requireModelRole(Connection connection, UserIdentity user, String name,
                                 boolean administrative, String operation) throws SQLException {
        if (!hasModelRole(connection, user, name, administrative)) {
            throw new EmbeddingRegistryAccessDeniedException(user.principal(), operation, name);
        }
    }

    static boolean hasModelRole(Connection connection, UserIdentity user, String name,
                                boolean administrative) throws SQLException {
        var roles = roles(connection, user);
        try (var statement = connection.prepareStatement(
                "SELECT admin_role,usage_role FROM pangrova_embedding_models WHERE model_name=?")) {
            statement.setString(1, name);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new ModelNotFoundException(name);
                }
                String admin = result.getString("admin_role");
                String usage = result.getString("usage_role");
                return (admin != null && roles.contains(admin))
                        || (!administrative && usage != null && roles.contains(usage));
            }
        }
    }

    private static Set<String> roles(Connection connection, UserIdentity user) throws SQLException {
        try {
            return CURRENT_ROLES.requireCurrentRoles(connection, user.principal());
        } catch (AuthorizationDeniedException noDirectAssignments) {
            return Set.of();
        }
    }
}
