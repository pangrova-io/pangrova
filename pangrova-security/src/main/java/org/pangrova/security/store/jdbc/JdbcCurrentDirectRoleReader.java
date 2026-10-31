package org.pangrova.security.store.jdbc;

import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.spi.jdbc.CurrentDirectRoleReader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/** Current direct assignments joined to actual role authority on the supplied database connection. */
public final class JdbcCurrentDirectRoleReader implements CurrentDirectRoleReader {
    /** Reads current direct roles, joined to existing role authority rather than transient identity groups. */
    @Override
    public Set<String> requireCurrentRoles(Connection connection, String principal) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT assignment.role_name FROM pangrova_user_roles assignment "
                        + "JOIN pangrova_roles role ON role.role_name=assignment.role_name "
                        + "WHERE assignment.user_id=?")) {
            statement.setString(1, principal);
            try (var result = statement.executeQuery()) {
                Set<String> roles = new HashSet<>();
                while (result.next()) {
                    roles.add(result.getString(1));
                }
                if (roles.isEmpty()) {
                    throw new AuthorizationDeniedException(
                            "Deferred work requires current direct user-role assignments");
                }
                return Set.copyOf(roles);
            }
        }
    }

}
