package org.pangrova.persistence.catalog.postgresql.projection;

import org.pangrova.persistence.catalog.api.projection.SchemaProjectionRepository;
import org.pangrova.persistence.catalog.postgresql.transaction.PostgresCatalogPersistenceBase;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

/** Read-only projection view; all mutation authority belongs to the projection workflow. */
public final class PostgresSchemaProjectionRepository extends PostgresCatalogPersistenceBase
        implements SchemaProjectionRepository {
    public PostgresSchemaProjectionRepository(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public List<String> loadSchemas(String catalogName) {
        return executeQuery("SELECT schema_name FROM schema_metadata WHERE catalog_name=? ORDER BY schema_name",
                statement -> statement.setString(1, catalogName), rows -> {
                    List<String> names = new ArrayList<>();
                    while (rows.next()) names.add(rows.getString(1));
                    return List.copyOf(names);
                });
    }
}
