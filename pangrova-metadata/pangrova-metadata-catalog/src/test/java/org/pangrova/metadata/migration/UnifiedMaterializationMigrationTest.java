package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.testing.testkit.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** V68 imports every legacy definition once and removes the retired Dynamic Table authority. */
class UnifiedMaterializationMigrationTest extends ContainerTestBase {

    @Test
    void backfillsLogicalDefinitionsSuspendsLegacyHeadsAndDropsOldAuthority() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("materialization_v68_" + java.util.UUID.randomUUID().toString().replace("-", ""));
        String suffix = java.util.UUID.randomUUID().toString().replace("-", "");
        String rollup = "lake.analytics.rollup_" + suffix;
        String pending = "lake.analytics.pending_" + suffix;
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").target("60").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO dynamic_table_publications (tenant_id,logical_name,"
                    + "incarnation_id,definition_sql,binding_catalog,binding_schema,published_table,revision,"
                    + "lifecycle_state,created_at,updated_at) VALUES "
                    + "('tenant','" + rollup + "',gen_random_uuid(),"
                    + "'SELECT count(*) FROM raw.events','lake','raw','lake.analytics.__pg_dt_legacy',7,"
                    + "'ACTIVE',clock_timestamp(),clock_timestamp()),"
                    + "('tenant','" + pending + "',gen_random_uuid(),"
                    + "'SELECT 1','lake','raw',NULL,0,'CREATING',clock_timestamp(),clock_timestamp())");
            statement.executeUpdate("INSERT INTO dynamic_table_state (dt_name,tenant_id,lifecycle_state,"
                    + "effective_refresh_mode,target_lag_ms,created_at,updated_at) VALUES "
                    + "('" + rollup + "','tenant','ACTIVE','SNAPSHOT',0,clock_timestamp(),clock_timestamp()),"
                    + "('" + pending + "','tenant','CREATING','SNAPSHOT',0,clock_timestamp(),clock_timestamp())");
        }

        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();

        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT object_name,object_kind,state,definition_revision,"
                    + "revision,refresh_mode,security_mode,definer_principal FROM materialization.objects "
                    + "WHERE object_name IN ('" + pending + "','" + rollup + "') ORDER BY object_name")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("object_name")).isEqualTo(pending);
                assertThat(rows.getString("state")).isEqualTo("CREATING");
                assertThat(rows.getString("object_kind")).isEqualTo("DYNAMIC_TABLE");
                assertThat(rows.getString("refresh_mode")).isEqualTo("FULL");
                assertThat(rows.getString("security_mode")).isEqualTo("DEFINER");
                assertThat(rows.getString("definer_principal")).isEqualTo("system:migration");
                assertThat(rows.getLong("definition_revision")).isEqualTo(1);
                assertThat(rows.getLong("revision")).isZero();
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("object_name")).isEqualTo(rollup);
                assertThat(rows.getString("state")).isEqualTo("SUSPENDED");
            }
            try (var rows = statement.executeQuery("SELECT definition_sql,resolved,output_schema "
                    + "FROM materialization.definition_revisions WHERE object_name='" + rollup + "'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("definition_sql")).isEqualTo("SELECT count(*) FROM raw.events");
                assertThat(rows.getBoolean("resolved")).isFalse();
                assertThat(rows.getObject("output_schema")).isNull();
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM materialization.schedules "
                    + "WHERE object_name IN ('" + pending + "','" + rollup + "') "
                    + "AND state='DISABLED' AND target_lag_ms IS NULL")) {
                rows.next();
                assertThat(rows.getInt(1)).isEqualTo(2);
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM materialization.events "
                    + "WHERE object_name IN ('" + pending + "','" + rollup + "') "
                    + "AND event_type='MIGRATED'")) {
                rows.next();
                assertThat(rows.getInt(1)).isEqualTo(2);
            }
            assertThat(tableExists(statement, "dynamic_table_publications")).isFalse();
            assertThat(tableExists(statement, "dynamic_table_refresh_attempts")).isFalse();
            assertThat(tableExists(statement, "dynamic_table_refresh_native_commits")).isFalse();
            assertThat(tableExists(statement, "dynamic_table_state")).isFalse();
            assertThat(tableExists(statement, "dynamic_table_refresh_history")).isFalse();
        }
    }

    @Test
    void definitionsAreImmutableAndObjectRevisionIsCasGuarded() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("materialization_v68_guards_" + java.util.UUID.randomUUID().toString().replace("-", ""));
        String tenant = "t_" + java.util.UUID.randomUUID().toString().replace("-", "");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO materialization.objects (tenant_id,object_name,object_kind,"
                    + "incarnation_id,definition_revision,revision,state,definer_principal,security_mode,"
                    + "refresh_mode,created_at,updated_at) VALUES ('" + tenant + "','c.s.t','DYNAMIC_TABLE',"
                    + "gen_random_uuid(),1,0,'CREATING','u','DEFINER','FULL',"
                    + "clock_timestamp(),clock_timestamp())");
            statement.executeUpdate("INSERT INTO materialization.definition_revisions (tenant_id,object_name,"
                    + "revision,definition_sql,canonical_sql,plan_fingerprint,resolved,output_schema,"
                    + "output_schema_hash,sources,policy_snapshot,security_mode,definer_principal,created_at) "
                    + "VALUES ('" + tenant + "','c.s.t',1,'SELECT 1','SELECT 1',repeat('a',64),FALSE,NULL,NULL,"
                    + "'[]'::jsonb,'{}'::jsonb,'DEFINER','u',clock_timestamp())");
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE materialization.definition_revisions SET definition_sql='SELECT 2' "
                            + "WHERE tenant_id='" + tenant + "'"))
                    .hasMessageContaining("definitions are immutable");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE materialization.objects SET revision=2 WHERE tenant_id='" + tenant + "'"))
                    .hasMessageContaining("revision must advance exactly one");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE materialization.objects SET revision=1,state='ACTIVE' WHERE tenant_id='" + tenant + "'"))
                    .hasMessageContaining("active materialization requires a published generation");
        }
    }

    private static boolean tableExists(java.sql.Statement statement, String table) throws Exception {
        try (var rows = statement.executeQuery(
                "SELECT to_regclass('public." + table + "') IS NOT NULL")) {
            rows.next();
            return rows.getBoolean(1);
        }
    }
}
