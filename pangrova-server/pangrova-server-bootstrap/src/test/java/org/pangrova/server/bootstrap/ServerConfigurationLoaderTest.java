package org.pangrova.server.bootstrap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServerConfigurationLoaderTest {
    @Test
    void parseConfigPath_returnsPathWhenPresent() {
        String[] args = {"--config", "/tmp/worker.yml", "--other", "value"};
        assertThat(ServerConfigurationLoader.parseConfigPath(args)).isEqualTo("/tmp/worker.yml");
    }

    @Test
    void parseConfigPath_returnsNullWhenAbsent() {
        String[] args = {"--other", "value"};
        assertThat(ServerConfigurationLoader.parseConfigPath(args)).isNull();
    }

    @Test
    void parseConfigPath_returnsNullForEmptyArgs() {
        assertThat(ServerConfigurationLoader.parseConfigPath(new String[0])).isNull();
    }

    @Test
    void parseConfigPath_returnsNullWhenConfigIsLastArg() {
        String[] args = {"--config"};
        assertThat(ServerConfigurationLoader.parseConfigPath(args)).isNull();
    }
}
