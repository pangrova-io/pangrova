package org.pangrova.testing.testkit.postgres;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.SQLException;

/** Applies the canonical resource bundle without linking any Metadata runtime implementation. */
public final class MigratedPostgres {
    private MigratedPostgres() {}

    /** Creates an isolated PostgreSQL schema and applies the complete production version sequence. */
    public static DataSource create() throws SQLException {
        DataSource source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        return source;
    }
}
