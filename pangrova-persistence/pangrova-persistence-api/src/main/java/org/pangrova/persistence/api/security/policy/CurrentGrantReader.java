package org.pangrova.persistence.api.security.policy;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import java.util.Set;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Current persisted facts on the supplied connection; never commits, rolls back or closes it. */
@Internal
public interface CurrentGrantReader {
    List<PrivilegeGrant> readRoleGrants(Connection connection, Set<String> roles) throws SQLException;
}
