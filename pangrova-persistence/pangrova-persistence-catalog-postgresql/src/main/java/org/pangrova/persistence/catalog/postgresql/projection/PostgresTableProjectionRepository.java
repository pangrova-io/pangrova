package org.pangrova.persistence.catalog.postgresql.projection;

import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.pangrova.persistence.catalog.api.projection.TableProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.transaction.PostgresCatalogPersistenceSupport;

/** PostgreSQL implementation of {@link TableProjectionRepository}; borrows the DataSource. */
public final class PostgresTableProjectionRepository extends PostgresCatalogPersistenceSupport
        implements TableProjectionRepository {
    private static final int SEARCH_LIMIT = 100;

    public PostgresTableProjectionRepository(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
        String sql = "SELECT columns, partition_spec, sort_order, properties, "
                + "table_created_at, table_updated_at FROM table_metadata "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        }, rs -> rs.next()
                ? Optional.of(ProjectionRows.readTable(rs, new QualifiedTableRef(catalog, schema, table)))
                : Optional.empty());
    }

    @Override
    public List<QualifiedTableRef> searchTables(String pattern) {
        String sql = "SELECT catalog_name, schema_name, table_name FROM table_metadata "
                + "WHERE table_name LIKE ? ESCAPE '\\' LIMIT ?";
        return executeQuery(sql, ps -> {
            ps.setString(1, "%" + escapeLikePattern(pattern) + "%");
            ps.setInt(2, SEARCH_LIMIT);
        }, this::mapTableRefs);
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
