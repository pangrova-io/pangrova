package org.pangrova.metadata.dynamictable;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Old heads remain readable; migration cannot manufacture execution or Native commit evidence. */
class JdbcDynamicTableRefreshMigrationTest extends ContainerTestBase {
    @Test
    void legacyPublishedHeadSurvivesUpgradeButCannotAdvanceWithoutOwnedNativeEvidence() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("dynamic_refresh_upgrade");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").target("60").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO dynamic_table_publications (tenant_id,logical_name,incarnation_id,"
                    + "definition_sql,binding_catalog,binding_schema,published_table,revision,lifecycle_state,created_at,updated_at) "
                    + "VALUES ('tenant','lake.analytics.rollup','f8f7ba19-f423-4bde-9108-e17dd039285a',"
                    + "'SELECT 1','lake','raw','lake.analytics.__pg_dt_legacy',7,'ACTIVE',clock_timestamp(),clock_timestamp())");
        }
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        var store = new JdbcDynamicTablePublicationStore(dataSource);
        var legacy = store.load("tenant", "lake.analytics.rollup").orElseThrow();
        assertThat(legacy.revision()).isEqualTo(7);
        assertThat(legacy.publishedTable()).isEqualTo("lake.analytics.__pg_dt_legacy");
        assertThat(store.findRefresh("tenant", "legacy")).isEmpty();
        var leader = new JdbcDistributedLease(dataSource).tryAcquire("coordinator_leader", "coordinator",
                Duration.ofMinutes(5)).orElseThrow();
        var owner = new JdbcExecutionAnchorStore(dataSource).tryAcquire("refresh", "coordinator", "process",
                Duration.ofMinutes(5)).orElseThrow();
        var issuer = new DynamicTableRefreshIssuer(owner, leader.epoch(), "test-user");
        var next = store.reserveRefresh(legacy, issuer);
        assertThat(next.attempt().baseRevision()).isEqualTo(7);
        assertThat(next.attempt().candidateTable()).isNotEqualTo(legacy.publishedTable());
        assertThatThrownBy(() -> store.publish(next.attempt(), issuer)).isInstanceOf(IllegalStateException.class)
                .satisfies(failure -> assertThat(failure.getCause())
                        .hasMessageContaining("no exact committed Native publication"));
        assertThat(store.load("tenant", "lake.analytics.rollup")).contains(legacy);
    }
}
