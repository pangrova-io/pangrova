package org.pangrova.governance.classification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC-backed column tag store. Persists governance column tags to the shared
 * SQL state so that PII tags survive coordinator restarts and auto-masking
 * rules are correctly regenerated on startup.
 *
 * <p>Schema is initialized idempotently via {@link #initSchema()} — safe to
 * call repeatedly at bootstrap.</p>
 */
public final class JdbcColumnTagStore implements ColumnTagStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcColumnTagStore.class);

    private static final String CREATE_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS column_tag ("
                    + "catalog_name VARCHAR(255) NOT NULL, "
                    + "schema_name  VARCHAR(255) NOT NULL, "
                    + "table_name   VARCHAR(255) NOT NULL, "
                    + "column_name  VARCHAR(255) NOT NULL, "
                    + "tag_key      VARCHAR(255) NOT NULL, "
                    + "tag_value    VARCHAR(1024) NOT NULL, "
                    + "PRIMARY KEY (catalog_name, schema_name, table_name, column_name, tag_key)"
                    + ")";

    private static final String CREATE_INDEX_DDL =
            "CREATE INDEX idx_column_tag_column ON column_tag "
                    + "(catalog_name, schema_name, table_name, column_name)";

    private final DataSource dataSource;

    /**
     * Creates a store backed by the given shared SQL data source.
     */
    public JdbcColumnTagStore(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    /**
     * Creates the {@code column_tag} table if it does not already exist.
     * Safe to call repeatedly (idempotent).
     */
    public void initSchema() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_DDL);
            try {
                stmt.execute(CREATE_INDEX_DDL);
            } catch (SQLException e) {
                // Index may already exist — tolerate
                if (!isAlreadyExists(e)) {
                    throw e;
                }
            }
            LOG.info("event=column_tag_schema_initialized");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize column_tag schema", e);
        }
    }

    @Override
    public void saveTag(String catalog, String schema, String table,
                        String columnName, String tagKey, String tagValue) {
        // Use REPLACE/MERGE semantics: delete then insert (portable across H2/MySQL/PostgreSQL)
        String deleteSql = "DELETE FROM column_tag WHERE catalog_name = ? AND schema_name = ? "
                + "AND table_name = ? AND column_name = ? AND tag_key = ?";
        String insertSql = "INSERT INTO column_tag (catalog_name, schema_name, table_name, "
                + "column_name, tag_key, tag_value) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement del = conn.prepareStatement(deleteSql)) {
                del.setString(1, catalog);
                del.setString(2, schema);
                del.setString(3, table);
                del.setString(4, columnName);
                del.setString(5, tagKey);
                del.executeUpdate();
            }
            try (PreparedStatement ins = conn.prepareStatement(insertSql)) {
                ins.setString(1, catalog);
                ins.setString(2, schema);
                ins.setString(3, table);
                ins.setString(4, columnName);
                ins.setString(5, tagKey);
                ins.setString(6, tagValue);
                ins.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed saving column tag " + tagKey
                    + " for " + catalog + "." + schema + "." + table + "." + columnName, e);
        }
    }

    @Override
    public void deleteTag(String catalog, String schema, String table,
                          String columnName, String tagKey) {
        String sql = "DELETE FROM column_tag WHERE catalog_name = ? AND schema_name = ? "
                + "AND table_name = ? AND column_name = ? AND tag_key = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            ps.setString(4, columnName);
            ps.setString(5, tagKey);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed deleting column tag " + tagKey
                    + " for " + catalog + "." + schema + "." + table + "." + columnName, e);
        }
    }

    @Override
    public Map<String, String> getTags(String catalog, String schema, String table, String columnName) {
        String sql = "SELECT tag_key, tag_value FROM column_tag "
                + "WHERE catalog_name = ? AND schema_name = ? AND table_name = ? AND column_name = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            ps.setString(4, columnName);
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> tags = new LinkedHashMap<>();
                while (rs.next()) {
                    tags.put(rs.getString("tag_key"), rs.getString("tag_value"));
                }
                return tags;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading column tags for "
                    + catalog + "." + schema + "." + table + "." + columnName, e);
        }
    }

    @Override
    public List<ColumnTagEntry> getAllTags() {
        String sql = "SELECT catalog_name, schema_name, table_name, column_name, tag_key, tag_value "
                + "FROM column_tag ORDER BY catalog_name, schema_name, table_name, column_name, tag_key";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<ColumnTagEntry> entries = new ArrayList<>();
            while (rs.next()) {
                entries.add(new ColumnTagEntry(
                        rs.getString("catalog_name"),
                        rs.getString("schema_name"),
                        rs.getString("table_name"),
                        rs.getString("column_name"),
                        rs.getString("tag_key"),
                        rs.getString("tag_value")));
            }
            return entries;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading all column tags", e);
        }
    }

    private static boolean isAlreadyExists(SQLException e) {
        String msg = e.getMessage();
        return msg != null && msg.toLowerCase().contains("exist");
    }
}
