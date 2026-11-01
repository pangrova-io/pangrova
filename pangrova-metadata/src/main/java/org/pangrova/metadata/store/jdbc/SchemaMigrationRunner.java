package org.pangrova.metadata.store.jdbc;

import java.util.List;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.pangrova.security.store.SecuritySchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies versioned SQL migrations to a per-owner schema baseline.
 *
 * <p>Each owner module ships its own baseline under {@code db/<owner>/migration/} and records
 * its history in a dedicated {@code flyway_schema_history_<owner>} table, so owner baselines
 * evolve independently. The metadata owner defaults to {@code db/metadata/migration} and
 * {@code flyway_schema_history_metadata}.</p>
 *
 * <p>{@link #migrateAll(DataSource)} applies the complete production sequence: the bundled
 * security schema (idempotent, not Flyway-managed) first, then every owner baseline present on
 * the classpath in dependency order. Owners whose migration location is absent from the
 * classpath are skipped, so a module-scoped test runtime applies exactly the domains it
 * carries.</p>
 *
 * <p>Call {@link #migrate(DataSource)} or {@link #migrateAll(DataSource)} once during bootstrap,
 * before any JDBC store is used. Applied migrations are tracked in the history tables; only
 * pending migrations are executed on subsequent calls.</p>
 */
public final class SchemaMigrationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaMigrationRunner.class);

    /** Classpath location of the metadata-owner baseline. */
    public static final String METADATA_LOCATION = "db/metadata/migration";

    /** History table used by the metadata-owner baseline. */
    public static final String METADATA_HISTORY_TABLE = "flyway_schema_history_metadata";

    /**
     * Owner baselines applied after the metadata owner, in dependency order:
     * semantic and ai are self-contained; query depends on the bundled security schema
     * ({@code storage_profile_revisions} FK); materialization depends on query
     * ({@code write_commit_journal} trigger); observability is applied last.
     */
    private static final List<OwnerMigration> OWNER_SEQUENCE = List.of(
            new OwnerMigration("db/semantic/migration", "flyway_schema_history_semantic"),
            new OwnerMigration("db/ai/evaluation/migration", "flyway_schema_history_ai"),
            new OwnerMigration("db/coordinator/coordination/migration", "flyway_schema_history_coordinator"),
            new OwnerMigration("db/governance/migration", "flyway_schema_history_governance"),
            new OwnerMigration("db/indexing/migration", "flyway_schema_history_indexing"),
            new OwnerMigration("db/streaming/migration", "flyway_schema_history_streaming"),
            new OwnerMigration("db/query/migration", "flyway_schema_history_query"),
            new OwnerMigration("db/materialization/migration", "flyway_schema_history_materialization"),
            new OwnerMigration("db/observability/audit/migration", "flyway_schema_history_observability"));

    private SchemaMigrationRunner() {
    }

    /**
     * Applies the metadata-owner baseline.
     *
     * @param dataSource the target database connection pool
     * @return the number of migrations applied
     */
    public static int migrate(DataSource dataSource) {
        return migrate(dataSource, METADATA_LOCATION, METADATA_HISTORY_TABLE);
    }

    /**
     * Applies the pending migrations of one owner baseline.
     *
     * @param dataSource the target database connection pool
     * @param location classpath location of the owner's migrations (e.g. {@code db/query/migration})
     * @param historyTable history table dedicated to this owner (e.g. {@code flyway_schema_history_query})
     * @return the number of migrations applied
     */
    public static int migrate(DataSource dataSource, String location, String historyTable) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:" + location)
                .table(historyTable)
                // Owner baselines share one schema, so earlier owners' tables make the schema
                // non-empty; baseline at 0 instead of failing on the missing history table.
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();

        MigrateResult result = flyway.migrate();
        int applied = result.migrationsExecuted;
        LOG.info("event=schema_migration_complete location={} historyTable={} migrationsApplied={} schemaVersion={}",
                location, historyTable, applied, result.targetSchemaVersion);
        return applied;
    }

    /**
     * Applies the complete production schema sequence: the bundled security schema first (so
     * cross-owner foreign keys resolve), then the metadata owner and every owner baseline whose
     * migration location exists on the classpath, in dependency order. Idempotent: the security
     * schema tolerates existing objects and per-owner history tables make re-runs no-ops.
     *
     * @param dataSource the target database connection pool
     */
    public static void migrateAll(DataSource dataSource) {
        SecuritySchema.initialize(dataSource);
        migrate(dataSource);
        for (OwnerMigration owner : OWNER_SEQUENCE) {
            if (classLoader().getResource(owner.location()) == null) {
                LOG.warn("event=schema_migration_owner_skipped owner={} reason=location_not_on_classpath",
                        owner.historyTable());
                continue;
            }
            migrate(dataSource, owner.location(), owner.historyTable());
        }
    }

    private static ClassLoader classLoader() {
        ClassLoader cl = SchemaMigrationRunner.class.getClassLoader();
        return cl == null ? ClassLoader.getSystemClassLoader() : cl;
    }

    /** One owner baseline: its classpath location and the history table recording its application. */
    private record OwnerMigration(String location, String historyTable) {
    }
}
