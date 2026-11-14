package org.pangrova.testing.testkit.postgres;

import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.UUID;

/** Applies the canonical resource bundle without linking any Metadata runtime implementation. */
public final class MigratedPostgres {
    private MigratedPostgres() {}

    /** Creates an isolated PostgreSQL schema and applies the complete production version sequence. */
    public static DataSource create() throws SQLException {
        DataSource source = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        return source;
    }

    /** Initializes an isolated installation in a container already owned by the test fixture. */
    public static PGSimpleDataSource create(String jdbcUrl, String user, String password) {
        String schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(jdbcUrl);
        source.setUser(user);
        source.setPassword(password);
        source.setCurrentSchema(schema);
        Flyway.configure().dataSource(source).schemas(schema).locations("classpath:db/migration")
                .load().migrate();
        return source;
    }
}
