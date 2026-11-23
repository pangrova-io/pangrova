package org.pangrova.persistence.workflow.postgresql.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.security.api.exception.AuthorizationDeniedException;

/** Observes the exact table registration and its catalog deployment in one statement snapshot. */
final class PostgresPolicyTableIdentity {
    private static final String IDENTITY_COLUMNS = "SELECT control.table_id, control.state, "
            + "control.catalog_incarnation_id, control.catalog_revision, catalog.incarnation_id, "
            + "catalog.revision, catalog.state AS catalog_state";
    private static final String IDENTITY_TABLES = " FROM table_control_registry control "
            + "LEFT JOIN catalog_definitions catalog ON catalog.name=control.catalog_name ";
    private static final String CURRENT_NAME = "WHERE control.table_id IN (SELECT table_id "
            + "FROM table_control_name_reservation WHERE catalog_name=? AND schema_name=? AND table_name=?) "
            + "AND control.state<>'DROPPED'";
    private static final String FIND = IDENTITY_COLUMNS + IDENTITY_TABLES + CURRENT_NAME;

    private PostgresPolicyTableIdentity() {}

    record VerifiedTable(UUID tableId, String incarnation, long revision, boolean activeCatalog) {}

    static Optional<VerifiedTable> find(Connection connection, String catalog, String schema, String table,
            String denial) throws SQLException {
        try (var query = connection.prepareStatement(FIND)) {
            bindName(query, catalog, schema, table);
            try (var row = query.executeQuery()) {
                return row.next() ? Optional.of(verify(row, denial)) : Optional.empty();
            }
        }
    }

    private static void bindName(PreparedStatement query, String catalog, String schema, String table)
            throws SQLException {
        query.setString(1, catalog);
        query.setString(2, schema);
        query.setString(3, table);
    }

    static VerifiedTable verify(ResultSet row, String denial) throws SQLException {
        if (!"ACTIVE".equals(row.getString("state"))
                || row.getString("incarnation_id") == null
                || !row.getString("incarnation_id").equals(row.getString("catalog_incarnation_id"))
                || row.getLong("revision") != row.getLong("catalog_revision")) {
            throw new AuthorizationDeniedException(denial);
        }
        return new VerifiedTable(row.getObject("table_id", UUID.class),
                row.getString("incarnation_id"), row.getLong("revision"),
                "ACTIVE".equals(row.getString("catalog_state")));
    }
}
