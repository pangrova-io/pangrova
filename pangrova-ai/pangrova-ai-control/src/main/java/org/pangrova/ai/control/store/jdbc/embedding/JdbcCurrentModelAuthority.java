package org.pangrova.ai.control.store.jdbc.embedding;

import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;
import org.pangrova.common.api.annotation.Internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Current PostgreSQL technical model contracts and invocation-role authority for durable publication. */
@Internal
public final class JdbcCurrentModelAuthority implements CurrentModelAuthority {
    private final CurrentDirectRoleReader currentRoles;

    /** Uses the supplied current-role reader on the same caller-owned authorization transaction. */
    public JdbcCurrentModelAuthority(CurrentDirectRoleReader currentRoles) {
        this.currentRoles = Objects.requireNonNull(currentRoles, "currentRoles");
    }

    /** Captures exact model incarnations after checking current direct-role invocation authority. */
    @Override
    public List<ModelSnapshot> observe(Connection connection, String principal, Set<String> modelNames)
            throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        Set<String> roles = currentRoles.requireCurrentRoles(connection, principal);
        List<ModelSnapshot> models = new ArrayList<>();
        for (String name : new java.util.TreeSet<>(modelNames)) {
            models.add(readModel(connection, roles, name));
        }
        return List.copyOf(models);
    }

    /** Compares current model incarnation, revision and deployment on the same publication connection. */
    @Override
    public void requireCurrent(Connection connection, String principal, List<ModelSnapshot> expected)
            throws SQLException {
        if (expected.isEmpty()) {
            return;
        }
        AuthorizationTransactionGate.lockPublication(connection);
        Set<String> roles = currentRoles.requireCurrentRoles(connection, principal);
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
