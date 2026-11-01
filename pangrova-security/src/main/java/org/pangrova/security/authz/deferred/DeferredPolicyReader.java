package org.pangrova.security.authz.deferred;

import org.pangrova.spi.security.authz.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.authz.DeferredSubjectContext;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.persistence.api.security.policy.CurrentPolicyReader;
import org.pangrova.persistence.jdbc.security.policy.JdbcCurrentPolicyReader;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.rowfilter.RowPolicy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Reads and renders current policy content without leaving the publication transaction. */
public final class DeferredPolicyReader {
    private static final CurrentPolicyReader CURRENT = new JdbcCurrentPolicyReader();

    private DeferredPolicyReader() {}

    public static PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
                                  TablePermission permission) throws SQLException {
        return observe(connection, principal, roles, permission, rows -> rows, (column, masks) -> masks);
    }

    /** Owner adapter supplies identity-bound masks while rendering stays in the security owner. */
    public static PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
            TablePermission permission, RowProjection rowProjection, MaskProjection projection) throws SQLException {
        // The principal is the persisted authenticated subject, not a maintenance identity.
        // Groups and ephemeral authentication attributes are deliberately not reconstructed.
        var subject = DeferredSubjectContext.fromPrincipal(principal, null);
        var policies = rowProjection.apply(CURRENT.readRows(connection, permission));
        policies.stream().filter(policy -> policy.appliesTo(roles))
                .forEach(policy -> DeferredSubjectContext.requireStable(subject, policy.filterExpression()));
        var rowFilter = RowLevelSecurityFilter.renderPolicies(subject, roles, policies);
        Map<String, Optional<String>> masks = new TreeMap<>();
        for (String column : permission.columns()) {
            var columnMasks = projection.apply(column, CURRENT.readMasks(connection, permission, column));
            columnMasks.stream().filter(mask -> mask.appliesTo(roles) && !mask.isExempt(roles))
                    .forEach(mask -> DeferredSubjectContext.requireStable(subject, mask.maskExpression()));
            masks.put(column, ColumnMaskingProcessor.renderMasks(subject, roles, column, columnMasks));
        }
        return new PolicySnapshot(rowFilter, masks);
    }

    @FunctionalInterface
    public interface RowProjection {
        java.util.List<RowPolicy> apply(java.util.List<RowPolicy> policies) throws SQLException;
    }

    @FunctionalInterface
    public interface MaskProjection {
        java.util.List<ColumnMask> apply(String column, java.util.List<ColumnMask> masks) throws SQLException;
    }

}
