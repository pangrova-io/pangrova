package org.pangrova.security.authorization.deferred;

import org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.authorization.DeferredSubjectContext;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentPolicyReader;
import java.util.Objects;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Reads and renders current policy content without leaving the publication transaction. */
public final class DeferredPolicyReader {
    private final CurrentPolicyReader current;

    /** Borrows a thread-safe reader; policy evaluation never owns its database transaction. */
    public DeferredPolicyReader(CurrentPolicyReader current) {
        this.current = Objects.requireNonNull(current, "current");
    }

    public PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
                                  TablePermission permission) throws SQLException {
        return observe(connection, principal, roles, permission,
                () -> current.readRows(connection, permission), masks -> masks);
    }

    /** Owner adapter supplies identity-bound masks while rendering stays in the security owner. */
    public PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
            TablePermission permission, RowReader rows, MaskProjection projection) throws SQLException {
        // The principal is the persisted authenticated subject, not a maintenance identity.
        // Groups and ephemeral authentication attributes are deliberately not reconstructed.
        var subject = DeferredSubjectContext.fromPrincipal(principal, null);
        var policies = rows.read();
        policies.stream().filter(policy -> policy.appliesTo(roles))
                .forEach(policy -> DeferredSubjectContext.requireStable(subject, policy.filterExpression()));
        var rowFilter = RowLevelSecurityFilter.renderPolicies(subject, roles, policies);
        if (permission.columns().isEmpty()) {
            return new PolicySnapshot(rowFilter, Map.of());
        }
        var byColumn = projection.apply(current.readMasks(connection, permission)).stream()
                .collect(Collectors.groupingBy(ColumnMask::columnName));
        Map<String, Optional<String>> masks = new TreeMap<>();
        for (String column : permission.columns()) {
            var columnMasks = byColumn.getOrDefault(column, List.of());
            columnMasks.stream().filter(mask -> mask.appliesTo(roles) && !mask.isExempt(roles))
                    .forEach(mask -> DeferredSubjectContext.requireStable(subject, mask.maskExpression()));
            masks.put(column, ColumnMaskingProcessor.renderMasks(subject, roles, column, columnMasks));
        }
        return new PolicySnapshot(rowFilter, masks);
    }

    @FunctionalInterface
    public interface RowReader {
        List<RowPolicy> read() throws SQLException;
    }

    @FunctionalInterface
    public interface MaskProjection {
        List<ColumnMask> apply(List<ColumnMask> masks) throws SQLException;
    }

}
