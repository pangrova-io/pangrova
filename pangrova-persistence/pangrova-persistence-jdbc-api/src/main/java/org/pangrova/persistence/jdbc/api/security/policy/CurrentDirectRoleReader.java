package org.pangrova.persistence.jdbc.api.security.policy;

import org.pangrova.common.api.annotation.Internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/** Security-owned current direct-role facts on the caller's database connection. */
@Internal
@FunctionalInterface
public interface CurrentDirectRoleReader {
    /**
     * Returns existing direct roles, rejecting subjects with no current assignment.
     * Identity-provider groups and persisted execution snapshots are not role authority.
     * The reader does not acquire a connection or commit, roll back or close it.
     * Side-effecting callers must first hold the current-authorization transaction gate
     * and retain that transaction through their protected mutation.
     */
    Set<String> requireCurrentRoles(Connection connection, String principal) throws SQLException;
}
