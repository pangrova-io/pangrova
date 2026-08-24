package org.pangrova.security.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.EnrichedColumnMaskStore;

/**
 * Column masking policy store persisted on the shared SQL state (SI-4). Replaces
 * {@code EmptyColumnMaskStore} in production once column masking is enabled, so authored masks load
 * and enforce. Selected by {@code SecurityBootstrap} per config; until then masking remains inert.
 */
public final class PersistentColumnMaskStore extends PersistentStoreSupport
        implements EnrichedColumnMaskStore {

    public PersistentColumnMaskStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
        String sql = "SELECT mask_id, mask_name, catalog_name, schema_name, table_name, column_name, "
                + "mask_expression, apply_to_roles, exempt_roles, enabled FROM column_mask "
                + "WHERE catalog_name = ? AND schema_name = ? AND table_name = ? AND column_name = ? "
                + "AND enabled = TRUE ORDER BY mask_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            ps.setString(4, column);
            try (ResultSet rs = ps.executeQuery()) {
                List<ColumnMask> masks = new ArrayList<>();
                while (rs.next()) {
                    masks.add(new ColumnMask(
                            rs.getLong("mask_id"),
                            rs.getString("mask_name"),
                            rs.getString("catalog_name"),
                            rs.getString("schema_name"),
                            rs.getString("table_name"),
                            rs.getString("column_name"),
                            rs.getString("mask_expression"),
                            decodeRoles(rs.getString("apply_to_roles")),
                            decodeRoles(rs.getString("exempt_roles")),
                            rs.getBoolean("enabled")));
                }
                return masks;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading column masks for "
                    + catalog + "." + schema + "." + table + "." + column, e);
        }
    }

    @Override
    public void saveColumnMask(ColumnMask mask) {
        String sql = "INSERT INTO column_mask (mask_name, catalog_name, schema_name, table_name, "
                + "column_name, mask_expression, apply_to_roles, exempt_roles, enabled) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
