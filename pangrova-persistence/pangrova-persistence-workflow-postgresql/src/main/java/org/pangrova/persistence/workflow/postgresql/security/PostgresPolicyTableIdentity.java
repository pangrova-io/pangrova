package org.pangrova.persistence.workflow.postgresql.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.table.api.policy.GovernanceBinding;

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
    private static final String FIND_READ = IDENTITY_COLUMNS + ", binding.binding_id, binding.binding_type, "
            + "binding.binding_ref, binding.config, binding.created_by, binding.created_at, binding.updated_at"
            + IDENTITY_TABLES + "LEFT JOIN table_control_governance_binding binding "
            + "ON binding.table_id=control.table_id " + CURRENT_NAME
            + " ORDER BY binding.binding_type, binding.binding_ref";

    private PostgresPolicyTableIdentity() {}

    record VerifiedTable(UUID tableId, String incarnation, long revision, boolean activeCatalog) {}

    record VerifiedRead(VerifiedTable table, List<GovernanceBinding> bindings) {}

    static Optional<VerifiedTable> find(Connection connection, String catalog, String schema, String table,
            String denial) throws SQLException {
        try (var query = connection.prepareStatement(FIND)) {
            bindName(query, catalog, schema, table);
            try (var row = query.executeQuery()) {
                return row.next() ? Optional.of(verify(row, denial)) : Optional.empty();
            }
        }
    }

    static Optional<VerifiedRead> findRead(Connection connection, String catalog, String schema, String table,
            String denial) throws SQLException {
        try (var query = connection.prepareStatement(FIND_READ)) {
            bindName(query, catalog, schema, table);
            try (var row = query.executeQuery()) {
                if (!row.next()) return Optional.empty();
                var verified = verify(row, denial);
                List<GovernanceBinding> bindings = new ArrayList<>();
                do {
                    if (row.getObject("binding_id") != null) {
                        bindings.add(PostgresTablePolicyRepository.readBinding(row));
                    }
                } while (row.next());
                return Optional.of(new VerifiedRead(verified, List.copyOf(bindings)));
            }
        }
    }

    private static void bindName(PreparedStatement query, String catalog, String schema, String table)
            throws SQLException {
        query.setString(1, catalog);
        query.setString(2, schema);
        query.setString(3, table);
    }

    private static VerifiedTable verify(ResultSet row, String denial) throws SQLException {
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
