package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.security.store.PersistentDeferredAuthorization;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Current PostgreSQL technical model contracts and invocation-role authority for durable publication. */
public final class GeneratedModelCurrentAuthority {
    private GeneratedModelCurrentAuthority() {}

    /** Captures exact model incarnations after checking current direct-role invocation authority. */
    public static List<ModelSnapshot> observe(Connection connection, String principal, Set<String> modelNames)
            throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        Set<String> roles = PersistentDeferredAuthorization.requireCurrentRoles(connection, principal);
        List<ModelSnapshot> models = new ArrayList<>();
        for (String name : new java.util.TreeSet<>(modelNames)) {
            models.add(readModel(connection, roles, name));
        }
        return List.copyOf(models);
    }

    /** Compares current model incarnation, revision and deployment on the same publication connection. */
    public static void requireCurrent(Connection connection, String principal, List<ModelSnapshot> expected)
            throws SQLException {
        if (expected.isEmpty()) {
            return;
        }
        AuthorizationTransactionGate.lockPublication(connection);
        Set<String> roles = PersistentDeferredAuthorization.requireCurrentRoles(connection, principal);
        for (ModelSnapshot model : expected) {
            if (!readModel(connection, roles, model.logicalName()).equals(model)) {
                throw new AuthorizationDeniedException(
                        "Model contract changed; securely recompile the generated intent");
            }
        }
    }

    private static ModelSnapshot readModel(Connection connection, Set<String> roles, String name) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT incarnation_id,version,deployment_version,"
                + "deployment_signature_sha256,admin_role,usage_role "
                + "FROM pangrova_embedding_models WHERE model_name=?")) {
            statement.setString(1, name);
            try (var result = statement.executeQuery()) {
                if (!result.next() || !(containsRole(roles, result.getString("admin_role"))
                        || containsRole(roles, result.getString("usage_role")))) {
                    throw new AuthorizationDeniedException(
                            "Current model invocation authority is absent or revoked: " + name);
                }
                try {
                    return new ModelSnapshot(name, result.getString("incarnation_id"), result.getInt("version"),
                            result.getString("deployment_version"), result.getString("deployment_signature_sha256"));
                } catch (IllegalArgumentException invalidContract) {
                    throw new AuthorizationDeniedException(
                            "Current model is not bound to an exact deployment: " + name);
                }
            }
        }
    }

    private static boolean containsRole(Set<String> roles, String role) {
        return role != null && roles.contains(role);
    }
}
