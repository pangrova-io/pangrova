package org.pangrova.persistence.security.postgresql.mask;

import org.pangrova.persistence.security.postgresql.support.JdbcPolicyStoreSupport;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore;

/**
 * Column masking policy store persisted on the shared SQL state (SI-4). Replaces
 * {@code EmptyColumnMaskStore} in production once column masking is enabled, so authored masks load
 * and enforce. Selected by {@code SecurityBootstrap} per config; until then masking remains inert.
 */
public final class JdbcColumnMaskStore extends JdbcPolicyStoreSupport
        implements EnrichedColumnMaskStore {
    private static final String MASK_COLUMNS = "SELECT mask_id, mask_name, catalog_name, schema_name, table_name, "
            + "column_name, mask_expression, apply_to_roles, exempt_roles, enabled, table_id FROM column_mask ";
    private static final String TABLE_MASKS = MASK_COLUMNS
            + "WHERE catalog_name=? AND schema_name=? AND table_name=? AND enabled=TRUE";
    private static final String MASK_ORDER = " ORDER BY mask_id";

    public JdbcColumnMaskStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
        java.util.Objects.requireNonNull(column, "column");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(TABLE_MASKS + " AND column_name=?" + MASK_ORDER)) {
            bindTable(ps, catalog, schema, table);
            ps.setString(4, column);
            return readMasks(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading column masks for "
                    + catalog + "." + schema + "." + table + "." + column, e);
        }
    }

    @Override
    public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(TABLE_MASKS + MASK_ORDER)) {
            bindTable(ps, catalog, schema, table);
            return readMasks(ps);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed loading table masks for " + catalog + "." + schema + "." + table, e);
        }
    }

    /** Reads all requested columns on the caller's existing authorization transaction. */
    public static List<ColumnMask> listEnabledForColumns(Connection connection, String catalog, String schema,
            String table, Set<String> columns) throws SQLException {
        if (columns.isEmpty()) {
            return List.of();
        }
        try (PreparedStatement statement = connection.prepareStatement(
                TABLE_MASKS + " AND column_name=ANY(?)" + MASK_ORDER)) {
            bindTable(statement, catalog, schema, table);
            Array names = connection.createArrayOf("text", columns.toArray(String[]::new));
            try {
                statement.setArray(4, names);
                return readMasks(statement);
            } finally {
                names.free();
            }
        }
    }

    private static void bindTable(PreparedStatement statement, String catalog, String schema, String table)
            throws SQLException {
        statement.setString(1, catalog);
        statement.setString(2, schema);
        statement.setString(3, table);
    }

    private static List<ColumnMask> readMasks(PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            List<ColumnMask> masks = new ArrayList<>();
            while (rs.next()) {
                masks.add(new ColumnMask(rs.getLong("mask_id"), rs.getString("mask_name"),
                        rs.getString("catalog_name"), rs.getString("schema_name"), rs.getString("table_name"),
                        rs.getString("column_name"), rs.getString("mask_expression"),
                        decodeRoles(rs.getString("apply_to_roles")), decodeRoles(rs.getString("exempt_roles")),
                        rs.getBoolean("enabled"), rs.getObject("table_id", java.util.UUID.class)));
            }
            return masks;
        }
    }

    @Override
    public void saveColumnMask(ColumnMask mask) {
        String sql = "INSERT INTO column_mask (mask_name, catalog_name, schema_name, table_name, "
                + "column_name, mask_expression, apply_to_roles, exempt_roles, enabled, table_id) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (catalog_name, schema_name, table_name, column_name, mask_name, table_id) "
                + "DO UPDATE SET mask_expression = EXCLUDED.mask_expression, "
                + "apply_to_roles = EXCLUDED.apply_to_roles, "
                + "exempt_roles = EXCLUDED.exempt_roles, enabled = EXCLUDED.enabled";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mask.maskName());
            ps.setString(2, mask.catalogName());
            ps.setString(3, mask.schemaName());
            ps.setString(4, mask.tableName());
            ps.setString(5, mask.columnName());
            ps.setString(6, mask.maskExpression());
            ps.setString(7, encodeRoles(mask.applyToRoles()));
            ps.setString(8, encodeRoles(mask.exemptRoles()));
            ps.setBoolean(9, mask.enabled());
            ps.setObject(10, mask.tableId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed saving column mask " + mask.maskName(), e);
        }
    }

    @Override
    public void deleteMask(long maskId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM column_mask WHERE mask_id = ?")) {
            ps.setLong(1, maskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed deleting column mask " + maskId, e);
        }
    }
}
