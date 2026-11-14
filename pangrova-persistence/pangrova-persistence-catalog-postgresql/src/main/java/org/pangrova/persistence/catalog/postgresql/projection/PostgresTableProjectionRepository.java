package org.pangrova.persistence.catalog.postgresql.projection;

import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;

/** PostgreSQL implementation of {@link TableProjectionRepository}; borrows the DataSource. */
public final class PostgresTableProjectionRepository implements TableProjectionRepository {
    private static final int SEARCH_LIMIT = 100;

    private final DataSource dataSource;

    public PostgresTableProjectionRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
        var ref = new QualifiedTableRef(catalog, schema, table);
        try (Connection conn = dataSource.getConnection()) {
            return CatalogProjectionSql.load(conn, ref);
        } catch (SQLException failure) {
            throw new MetastoreException("Cannot load table projection " + ref, failure);
        }
    }

    @Override
    public List<QualifiedTableRef> searchTables(String pattern) {
        String sql = "SELECT catalog_name, schema_name, table_name FROM table_metadata "
                + "WHERE table_name LIKE ? ESCAPE '\\' LIMIT ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, "%" + escapeLikePattern(pattern) + "%");
            statement.setInt(2, SEARCH_LIMIT);
            try (ResultSet rows = statement.executeQuery()) {
                return mapTableRefs(rows);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Cannot search table projections", failure);
        }
    }

    private static String escapeLikePattern(String input) {
        return input.replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_");
    }

    private List<QualifiedTableRef> mapTableRefs(ResultSet rs) throws SQLException {
        List<QualifiedTableRef> refs = new ArrayList<>();
        while (rs.next()) {
            refs.add(new QualifiedTableRef(rs.getString(1), rs.getString(2), rs.getString(3)));
        }
        return refs;
    }

}
