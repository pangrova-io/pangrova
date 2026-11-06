package org.pangrova.persistence.security.postgresql.support;

import org.pangrova.persistence.security.postgresql.codec.PolicyRoleCodec;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

/**
 * Base for the security/governance policy stores persisted on the shared SQL state (SI-4).
 *
 * <p>Uses the supplied data source and owns only transactions it opens itself.
 * The canonical Schema Migrations bundle must initialize the datasource before construction.
 */
public abstract class JdbcPolicyStoreSupport {

    /** Shared SQL data source. Never null. */
    protected final DataSource dataSource;

    protected JdbcPolicyStoreSupport(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    /** Rolls back an owned transaction while retaining the original failure. */
    protected static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    protected static String encodeRoles(List<String> roles) {
        return PolicyRoleCodec.encodeRoles(roles);
    }

    protected static List<String> decodeRoles(String encoded) {
        return PolicyRoleCodec.decodeRoles(encoded);
    }
}
