package org.pangrova.persistence.workflow.api.security;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.security.api.rowfilter.RowPolicy;

/**
 * Security-owned deferred authorization evaluation over a caller-owned transaction connection.
 * The owner supplies table, row-policy and column-mask observation; policy rendering stays in
 * Security Core.
 */
public interface TableControlPolicyEvaluator {

    void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope,
            PolicyObserver observer) throws SQLException;

    PolicySnapshot observePolicies(Connection connection, String principal, Set<String> roles,
            TablePermission permission, RowProjection rows, MaskProjection masks) throws SQLException;

    @FunctionalInterface
    interface PolicyObserver {
        PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
                TablePermission permission) throws SQLException;
    }

    @FunctionalInterface
    interface RowProjection {
        List<RowPolicy> apply(List<RowPolicy> policies) throws SQLException;
    }

    @FunctionalInterface
    interface MaskProjection {
        List<ColumnMask> apply(String column, List<ColumnMask> masks) throws SQLException;
    }
}
