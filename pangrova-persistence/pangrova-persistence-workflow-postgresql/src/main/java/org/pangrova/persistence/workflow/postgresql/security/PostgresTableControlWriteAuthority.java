package org.pangrova.persistence.workflow.postgresql.security;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.persistence.table.postgresql.governancebinding.JdbcTableControlGovernanceBindingStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentGrantReader;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentPolicyReader;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.security.authorization.deferred.DeferredAuthorization;
import org.pangrova.security.authorization.deferred.DeferredPolicyReader;
import org.pangrova.persistence.workflow.api.security.TableControlPolicyProjections;
import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.table.api.policy.GovernanceBinding;

/**
 * Cross-domain same-connection write authority: binds Metadata-owned policy projections with
 * Table/Security persistence reads and Security-owned deferred evaluation.
 */
public final class PostgresTableControlWriteAuthority implements TableControlWriteAuthority {

    private final DeferredAuthorization authorization = new DeferredAuthorization(
            AuthorizationTransactionGate::lockPublication, new JdbcCurrentDirectRoleReader(), new JdbcCurrentGrantReader());
    private final DeferredPolicyReader policies = new DeferredPolicyReader(new JdbcCurrentPolicyReader());
    private final TableControlPolicyProjections projections;

    public PostgresTableControlWriteAuthority(TableControlPolicyProjections projections) {
        this.projections = Objects.requireNonNull(projections, "projections");
    }

    @Override
    public void requireCurrent(Connection connection, String principal,
            DeferredAuthorizationScope scope) throws SQLException {
        authorization.requireCurrent(connection, principal, scope, this::observe);
    }

    private PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
            TablePermission permission) throws SQLException {
        var table = permission.table();
        UUID tableId = identity(connection, permission);
        var bindings = tableId == null ? List.<GovernanceBinding>of()
                : JdbcTableControlGovernanceBindingStore.list(connection, tableId);
        var boundRows = tableId == null ? List.<org.pangrova.security.api.rowfilter.RowPolicy>of()
                : JdbcRowPolicyStore.list(connection, tableId);
        return policies.observe(connection, principal, roles, permission,
                rows -> projections.projectRows(tableId, table.catalog(), table.schema(), table.table(),
                        Stream.concat(rows.stream(), boundRows.stream()).toList()),
                (column, masks) -> projections.projectColumns(tableId, table.catalog(), table.schema(),
                                table.table(), masks, bindings).stream()
                        .filter(mask -> mask.columnName().equals(column)).toList());
    }

    private static UUID identity(Connection connection, TablePermission permission)
            throws SQLException {
        var table = permission.table();
        try (var query = connection.prepareStatement("SELECT control.table_id, control.state, "
                + "control.catalog_incarnation_id, control.catalog_revision, catalog.incarnation_id, "
                + "catalog.revision, catalog.state AS catalog_state FROM table_control_registry control "
                + "LEFT JOIN catalog_definitions catalog ON catalog.name=control.catalog_name "
                + "WHERE control.table_id IN (SELECT table_id FROM table_control_name_reservation "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=?) "
                + "AND control.state<>'DROPPED'")) {
            query.setString(1, table.catalog());
            query.setString(2, table.schema());
            query.setString(3, table.table());
            try (var row = query.executeQuery()) {
                if (!row.next()) return null;
                if (!"ACTIVE".equals(row.getString("state"))
                        || !"ACTIVE".equals(row.getString("catalog_state"))
                        || row.getString("incarnation_id") == null
                        || !row.getString("incarnation_id").equals(row.getString("catalog_incarnation_id"))
                        || row.getLong("revision") != row.getLong("catalog_revision")) {
                    throw new AuthorizationDeniedException("Deferred table identity or catalog deployment changed");
                }
                return row.getObject("table_id", UUID.class);
            }
        }
    }
}
