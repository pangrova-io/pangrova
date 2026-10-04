package org.pangrova.testing.e2e;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.ContainerTestBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the shared metadata database retains PostgreSQL durability guarantees. */
class SharedTestContainersConfigurationIntegrationTest extends ContainerTestBase {

    @Test
    void sharedServerAndReadinessProbeRequireAllDurabilitySettingsOn() throws Exception {
        var postgres = SharedTestContainers.POSTGRES;
        SharedTestContainers.probeJdbcReady(postgres, Duration.ofSeconds(30));
        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement statement = connection.createStatement()) {
            for (String setting : List.of("fsync", "synchronous_commit", "full_page_writes")) {
                try (ResultSet rows = statement.executeQuery("SHOW " + setting)) {
                    assertThat(rows.next()).as("setting %s exists", setting).isTrue();
                    assertThat(rows.getString(1)).as("actual %s", setting).isEqualTo("on");
                    assertThat(rows.next()).isFalse();
                }
            }
            SharedTestContainers.verifyDurability(connection);
        }
    }

    @Test
    void guardRejectsActualSessionWithAsynchronousCommit() throws Exception {
        var postgres = SharedTestContainers.POSTGRES;
        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("SET synchronous_commit = off");
            assertThatThrownBy(() -> SharedTestContainers.verifyDurability(connection))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("synchronous_commit=off");
            statement.execute("SET synchronous_commit = on");
            SharedTestContainers.verifyDurability(connection);
        }
    }
}
