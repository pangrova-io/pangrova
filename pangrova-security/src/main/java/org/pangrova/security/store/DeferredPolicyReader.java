package org.pangrova.security.store;

import org.pangrova.security.authz.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.authz.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.authz.DeferredSubjectContext;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Reads and renders current policy content without leaving the publication transaction. */
final class DeferredPolicyReader {
    private DeferredPolicyReader() {}

    static PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
                                  TablePermission permission) throws SQLException {
        // The principal is the persisted authenticated subject, not a maintenance identity.
        // Groups and ephemeral authentication attributes are deliberately not reconstructed.
        var subject = DeferredSubjectContext.fromPrincipal(principal, null);
        var policies = readRows(connection, permission);
        policies.stream().filter(policy -> policy.appliesTo(roles))
                .forEach(policy -> DeferredSubjectContext.requireStable(subject, policy.filterExpression()));
        var rowFilter = RowLevelSecurityFilter.renderPolicies(subject, roles, policies);
        Map<String, Optional<String>> masks = new TreeMap<>();
        for (String column : permission.columns()) {
            var columnMasks = readMasks(connection, permission, column);
            columnMasks.stream().filter(mask -> mask.appliesTo(roles) && !mask.isExempt(roles))
                    .forEach(mask -> DeferredSubjectContext.requireStable(subject, mask.maskExpression()));
            masks.put(column, ColumnMaskingProcessor.renderMasks(subject, roles, column, columnMasks));
        }
        return new PolicySnapshot(rowFilter, masks);
    }

    private static ArrayList<RowPolicy> readRows(Connection connection, TablePermission permission)
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
                            PersistentStoreSupport.decodeRoles(result.getString("apply_to_roles")),
                            true, result.getInt("priority")));
                }
                return policies;
            }
        }
    }

    private static ArrayList<ColumnMask> readMasks(
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
                            PersistentStoreSupport.decodeRoles(result.getString("apply_to_roles")),
                            PersistentStoreSupport.decodeRoles(result.getString("exempt_roles")), true));
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
