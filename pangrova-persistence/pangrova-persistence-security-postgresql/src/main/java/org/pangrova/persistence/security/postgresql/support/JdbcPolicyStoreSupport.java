package org.pangrova.persistence.security.postgresql.support;

import org.pangrova.persistence.security.postgresql.codec.PolicyRoleCodec;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

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

    protected static RowPolicy readRowPolicy(ResultSet row) throws SQLException {
        return new RowPolicy(row.getLong("policy_id"), row.getString("policy_name"),
                row.getString("catalog_name"), row.getString("schema_name"), row.getString("table_name"),
                row.getString("filter_expression"), decodeRoles(row.getString("apply_to_roles")),
                row.getBoolean("enabled"), row.getInt("priority"), row.getObject("table_id", UUID.class));
    }

    protected static ColumnMask readColumnMask(ResultSet row) throws SQLException {
        return new ColumnMask(row.getLong("mask_id"), row.getString("mask_name"),
                row.getString("catalog_name"), row.getString("schema_name"), row.getString("table_name"),
                row.getString("column_name"), row.getString("mask_expression"),
                decodeRoles(row.getString("apply_to_roles")), decodeRoles(row.getString("exempt_roles")),
                row.getBoolean("enabled"), row.getObject("table_id", UUID.class));
    }
}
