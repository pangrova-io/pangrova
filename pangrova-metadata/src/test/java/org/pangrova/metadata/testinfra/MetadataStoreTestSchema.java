package org.pangrova.metadata.testinfra;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test-only schema creation for metadata stores that no longer own DDL.
 *
 * <p>Production schema is owned by the per-owner Flyway baseline. Tests that use
 * H2 or an isolated PostgreSQL schema call this helper explicitly instead of
 * relying on store constructors or production initSchema methods.</p>
 */
public final class MetadataStoreTestSchema {

    private MetadataStoreTestSchema() {
    }

    private static void execute(DataSource dataSource, String... statements) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
