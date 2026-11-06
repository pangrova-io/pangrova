package org.pangrova.persistence.security.postgresql.policy;

import org.pangrova.persistence.jdbc.api.security.policy.CurrentPolicyReader;
import org.pangrova.persistence.security.postgresql.codec.PolicyRoleCodec;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Reads current persisted policies on the caller connection, without rendering or authorization. */
public final class JdbcCurrentPolicyReader implements CurrentPolicyReader {
    @Override
    public List<RowPolicy> readRows(Connection connection, TablePermission permission)
            throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT * FROM row_policy WHERE catalog_name=? AND schema_name=? AND table_name=? AND enabled")) {
            bindTable(statement, permission);
            try (var result = statement.executeQuery()) {
                var policies = new ArrayList<RowPolicy>();
                while (result.next()) {
                    policies.add(new RowPolicy(result.getLong("policy_id"), result.getString("policy_name"),
                            result.getString("catalog_name"), result.getString("schema_name"),
                            result.getString("table_name"), result.getString("filter_expression"),
                            PolicyRoleCodec.decodeRoles(result.getString("apply_to_roles")),
                            true, result.getInt("priority"), result.getObject("table_id", java.util.UUID.class)));
                }
                return policies;
            }
        }
    }

    @Override
    public List<ColumnMask> readMasks(
            Connection connection, TablePermission permission, String column) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT * FROM column_mask WHERE catalog_name=? AND schema_name=? AND table_name=? "
                        + "AND column_name=? AND enabled")) {
            bindTable(statement, permission);
            statement.setString(4, column);
            try (var result = statement.executeQuery()) {
                var masks = new ArrayList<ColumnMask>();
                while (result.next()) {
                    masks.add(new ColumnMask(result.getLong("mask_id"), result.getString("mask_name"),
                            result.getString("catalog_name"), result.getString("schema_name"),
                            result.getString("table_name"), column, result.getString("mask_expression"),
                            PolicyRoleCodec.decodeRoles(result.getString("apply_to_roles")),
                            PolicyRoleCodec.decodeRoles(result.getString("exempt_roles")), true,
                            result.getObject("table_id", java.util.UUID.class)));
                }
                return masks;
            }
        }
    }

    private static void bindTable(java.sql.PreparedStatement statement, TablePermission permission)
            throws SQLException {
        statement.setString(1, permission.table().catalog());
        statement.setString(2, permission.table().schema());
        statement.setString(3, permission.table().table());
    }

}
