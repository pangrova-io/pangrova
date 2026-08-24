package org.pangrova.metadata.store;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Applies versioned SQL migrations to the metadata database schema.
 *
 * <p>Migrations are loaded from {@code db/migration/} on the classpath.
 * Each migration file follows the naming pattern {@code V{version}__{description}.sql}.</p>
 *
 * <p>Call {@link #migrate(DataSource)} once during bootstrap, before any
 * JDBC store is used. Applied migrations are tracked in a schema history
 * table; only pending migrations are executed on subsequent calls.</p>
 */
public final class SchemaMigrationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaMigrationRunner.class);

    private SchemaMigrationRunner() {
    }

    /**
     * Applies all pending migrations to the given DataSource.
     *
     * @param dataSource the target database connection pool
     * @return the number of migrations applied
     */
    public static int migrate(DataSource dataSource) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();

        MigrateResult result = flyway.migrate();
        int applied = result.migrationsExecuted;
        LOG.info("event=schema_migration_complete migrationsApplied={} schemaVersion={}",
                applied, result.targetSchemaVersion);
        return applied;
    }
}
