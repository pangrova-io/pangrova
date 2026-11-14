package org.pangrova.persistence.security.postgresql.support;

import org.pangrova.persistence.security.postgresql.codec.PolicyRoleCodec;

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

    protected static String encodeRoles(List<String> roles) {
        return PolicyRoleCodec.encodeRoles(roles);
    }

    protected static List<String> decodeRoles(String encoded) {
        return PolicyRoleCodec.decodeRoles(encoded);
    }
}
