package org.pangrova.metadata.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

/** V70 keeps the durable dependency graph consistent with object drops. */
class MaterializationDependencyGuardMigrationTest extends ContainerTestBase {

    @Test
    void liveDependentsBlockDropsAndDroppedUpstreamsRejectNewEdges() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource(
                "materialization_v70_" + java.util.UUID.randomUUID().toString().replace("-", ""));
        String tenant = "t_" + java.util.UUID.randomUUID().toString().replace("-", "");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate(objectInsert(tenant, "c.s.upstream"));
            statement.executeUpdate(objectInsert(tenant, "c.s.downstream"));
            statement.executeUpdate(definitionInsert(tenant, "c.s.upstream"));
            statement.executeUpdate(definitionInsert(tenant, "c.s.downstream"));
            statement.executeUpdate("INSERT INTO materialization.dependencies (tenant_id,"
                    + "downstream_object,upstream_object,downstream_definition_revision,"
                    + "dependency_kind) VALUES ('" + tenant + "','c.s.downstream','c.s.upstream',1,"
                    + "'DIRECT')");
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE materialization.objects SET revision=1,state='DROPPING' "
                            + "WHERE tenant_id='" + tenant + "' AND object_name='c.s.upstream'"))
                    .hasMessageContaining("still has live dependents");

            statement.executeUpdate("UPDATE materialization.objects SET revision=1,state='DROPPING' "
                    + "WHERE tenant_id='" + tenant + "' AND object_name='c.s.downstream'");
            statement.executeUpdate("UPDATE materialization.objects SET revision=2,state='DROPPED' "
                    + "WHERE tenant_id='" + tenant + "' AND object_name='c.s.downstream'");
            statement.executeUpdate("UPDATE materialization.objects SET revision=1,state='DROPPING' "
                    + "WHERE tenant_id='" + tenant + "' AND object_name='c.s.upstream'");

            assertThatThrownBy(() -> statement.executeUpdate("INSERT INTO "
                    + "materialization.dependencies (tenant_id,downstream_object,upstream_object,"
                    + "downstream_definition_revision,dependency_kind) VALUES ('" + tenant + "',"
                    + "'c.s.upstream','c.s.downstream',1,'DIRECT')"))
                    .hasMessageContaining("dependency must name a live upstream materialization");
        }
    }

    @Test
    void dependencyInsertSerializesWithAConcurrentDrop() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource(
                "materialization_v70_lock_" + java.util.UUID.randomUUID().toString().replace("-", ""));
        String tenant = "t_" + java.util.UUID.randomUUID().toString().replace("-", "");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate(objectInsert(tenant, "c.s.upstream"));
            statement.executeUpdate(objectInsert(tenant, "c.s.downstream"));
            statement.executeUpdate(definitionInsert(tenant, "c.s.upstream"));
            statement.executeUpdate(definitionInsert(tenant, "c.s.downstream"));
        }
        java.util.concurrent.atomic.AtomicReference<Throwable> dropFailure =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        Thread dropper;
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO materialization.dependencies (tenant_id,"
                        + "downstream_object,upstream_object,downstream_definition_revision,"
                        + "dependency_kind) VALUES ('" + tenant + "','c.s.downstream','c.s.upstream',1,"
                        + "'DIRECT')");
            }
            dropper = new Thread(() -> {
                try (var connection2 = dataSource.getConnection();
                        var statement2 = connection2.createStatement()) {
                    started.countDown();
                    statement2.executeUpdate("UPDATE materialization.objects SET revision=1,"
                            + "state='DROPPING' WHERE tenant_id='" + tenant
                            + "' AND object_name='c.s.upstream'");
                } catch (Throwable failure) {
                    dropFailure.set(failure);
                }
            });
            dropper.start();
            assertThat(started.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            Thread.sleep(500);
            assertThat(dropper.isAlive())
                    .as("drop waits for the dependency edge transaction").isTrue();
            connection.commit();
        }
        dropper.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
        assertThat(dropper.isAlive()).isFalse();
        assertThat(dropFailure.get()).hasMessageContaining("still has live dependents");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT state FROM materialization.objects "
                        + "WHERE tenant_id='" + tenant + "' AND object_name='c.s.upstream'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("state")).isEqualTo("CREATING");
        }
    }

    private static String objectInsert(String tenant, String name) {
        return "INSERT INTO materialization.objects (tenant_id,object_name,object_kind,"
                + "incarnation_id,definition_revision,revision,state,definer_principal,security_mode,"
                + "refresh_mode,created_at,updated_at) VALUES ('" + tenant + "','" + name + "',"
                + "'DYNAMIC_TABLE',gen_random_uuid(),1,0,'CREATING','u','DEFINER','FULL',"
                + "clock_timestamp(),clock_timestamp())";
    }

    private static String definitionInsert(String tenant, String name) {
        return "INSERT INTO materialization.definition_revisions (tenant_id,object_name,"
                + "revision,definition_sql,canonical_sql,plan_fingerprint,resolved,output_schema,"
                + "output_schema_hash,sources,policy_snapshot,security_mode,definer_principal,"
                + "created_at) VALUES ('" + tenant + "','" + name + "',1,'SELECT 1','SELECT 1',"
                + "repeat('a',64),FALSE,NULL,NULL,'[]'::jsonb,'{}'::jsonb,'DEFINER','u',"
                + "clock_timestamp())";
    }
}
