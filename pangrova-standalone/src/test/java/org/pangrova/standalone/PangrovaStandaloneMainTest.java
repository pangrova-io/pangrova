package org.pangrova.standalone;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks standalone coordinator discovery before either server is started. */
class PangrovaStandaloneMainTest {

    @Test
    void workerUsesTheLocalCoordinatorPortWhenNoUrlIsConfigured() {
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                Map.of("worker.id", "standalone-worker"));

        var worker = PangrovaStandaloneMain.buildWorkerConfig(config, 18080);

        assertThat(worker.coordinatorUrl()).isEqualTo("http://localhost:18080");
    }

    @Test
    void explicitCoordinatorUrlTakesPrecedenceOverTheLocalPort() {
        var config = PangrovaConfiguration.fromYamlWithEnvOverride(
                Map.of("worker.id", "standalone-worker",
                        "worker.coordinator-url", "https://coordinator.example.test:8443"));

        var worker = PangrovaStandaloneMain.buildWorkerConfig(config, 18080);

        assertThat(worker.coordinatorUrl()).isEqualTo("https://coordinator.example.test:8443");
    }
}
