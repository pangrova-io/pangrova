package org.pangrova.persistence.jdbc.security.policy;

import org.pangrova.persistence.api.security.policy.CurrentGrantReader;
import org.pangrova.spi.security.policy.PrivilegeGrant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Reads current role grants without owning or finishing the caller transaction. */
public final class JdbcCurrentGrantReader implements CurrentGrantReader {
    @Override
    public List<PrivilegeGrant> readRoleGrants(Connection connection, Set<String> roles) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM security_grants ORDER BY grant_id");
             var result = statement.executeQuery()) {
            return JdbcGrantStore.collectGrants(result).stream()
                    .filter(grant -> roles.contains(grant.roleName())).toList();
        }
    }
}
