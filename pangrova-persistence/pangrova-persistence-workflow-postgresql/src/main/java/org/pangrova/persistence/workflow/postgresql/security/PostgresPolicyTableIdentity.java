package org.pangrova.persistence.workflow.postgresql.security;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.security.api.exception.AuthorizationDeniedException;

/** Observes the exact table registration and its catalog deployment in one statement snapshot. */
final class PostgresPolicyTableIdentity {
    private PostgresPolicyTableIdentity() {}

    record VerifiedTable(UUID tableId, String incarnation, long revision, boolean activeCatalog) {}

    static Optional<VerifiedTable> find(Connection connection, String catalog, String schema, String table,
            String denial) throws SQLException {
        try (var query = connection.prepareStatement("SELECT control.table_id, control.state, "
                + "control.catalog_incarnation_id, control.catalog_revision, catalog.incarnation_id, "
                + "catalog.revision, catalog.state AS catalog_state FROM table_control_registry control "
                + "LEFT JOIN catalog_definitions catalog ON catalog.name=control.catalog_name "
                + "WHERE control.table_id IN (SELECT table_id FROM table_control_name_reservation "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=?) "
                + "AND control.state<>'DROPPED'")) {
            query.setString(1, catalog);
            query.setString(2, schema);
            query.setString(3, table);
            try (var row = query.executeQuery()) {
                if (!row.next()) return Optional.empty();
                if (!"ACTIVE".equals(row.getString("state"))
                        || row.getString("incarnation_id") == null
                        || !row.getString("incarnation_id").equals(row.getString("catalog_incarnation_id"))
                        || row.getLong("revision") != row.getLong("catalog_revision")) {
                    throw new AuthorizationDeniedException(denial);
                }
                return Optional.of(new VerifiedTable(row.getObject("table_id", UUID.class),
                        row.getString("incarnation_id"), row.getLong("revision"),
                        "ACTIVE".equals(row.getString("catalog_state"))));
            }
        }
    }
}
