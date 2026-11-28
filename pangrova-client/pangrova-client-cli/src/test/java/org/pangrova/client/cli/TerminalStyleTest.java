package org.pangrova.client.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TerminalStyleTest {
    @Test
    void launcherRedirectedStreamsOverrideJavaConsoleDetection() {
        String terminal = System.getProperty("pangrova.cli.terminal");
        String columns = System.getProperty("pangrova.cli.columns");
        try {
            System.setProperty("pangrova.cli.terminal", "false");
            System.setProperty("pangrova.cli.columns", "60");
            var style = TerminalStyle.detect();
            assertThat(style.width()).isEqualTo(60);
            assertThat(style.accent("PANGROVA")).isEqualTo("PANGROVA");
        } finally {
            restore("pangrova.cli.terminal", terminal);
            restore("pangrova.cli.columns", columns);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
