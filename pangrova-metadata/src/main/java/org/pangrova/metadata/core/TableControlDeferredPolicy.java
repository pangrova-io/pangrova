package org.pangrova.metadata.core;

import org.pangrova.metadata.core.control.JdbcTableControlGovernanceBindingStore;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.store.DeferredPolicyReader;
import org.pangrova.security.store.PersistentDeferredAuthorization;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Uses the metadata owner's current stable identity and canonical tags during durable authorization. */
public final class TableControlDeferredPolicy {
    private TableControlDeferredPolicy() {}

    public static void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope)
            throws SQLException {
        PersistentDeferredAuthorization.requireCurrent(
                connection, principal, scope, TableControlDeferredPolicy::observe);
    }

    private static DeferredAuthorizationScope.PolicySnapshot observe(Connection connection, String principal,
            Set<String> roles, DeferredAuthorizationScope.TablePermission permission) throws SQLException {
        var table = permission.table();
        UUID tableId = identity(connection, permission);
        var bindings = tableId == null ? List.<org.pangrova.metadata.core.control.GovernanceBinding>of()
                : JdbcTableControlGovernanceBindingStore.list(connection, tableId);
        return DeferredPolicyReader.observe(connection, principal, roles, permission,
                rows -> TableControlRowPolicyStore.project(tableId, table.catalog(), table.schema(), table.table(),
                        rows, tableId == null ? List.of()
                                : org.pangrova.security.store.PersistentRowPolicyStore.list(connection, tableId)),
                (column, masks) ->
                TableControlColumnMaskStore.project(tableId, table.catalog(), table.schema(), table.table(),
                        masks, bindings).stream().filter(mask -> mask.columnName().equals(column)).toList());
    }

    private static UUID identity(Connection connection, DeferredAuthorizationScope.TablePermission permission)
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
