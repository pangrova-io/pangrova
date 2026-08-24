package org.pangrova.client.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for PangrovaCli argument parsing, including the --query-timeout flag.
 */
class PangrovaCliArgsTest {

    @Test
    void defaultQueryTimeoutIsThirtyMinutes() {
        PangrovaCli.CliArgs args = PangrovaCli.parseArgs(new String[]{"--host", "myhost"});
        assertThat(args.queryTimeout).isEqualTo(Duration.ofMinutes(30));
    }

    @ParameterizedTest
    @CsvSource({
            "30s, 30",
            "5m, 300",
            "2h, 7200",
            "1h30m, 5400",
            "90, 90",
            "1h0m30s, 3630"
    })
    void parsesValidTimeoutFormats(String input, long expectedSeconds) {
        Duration result = PangrovaCli.parseTimeout(input);
        assertThat(result).isEqualTo(Duration.ofSeconds(expectedSeconds));
    }

    @Test
    void queryTimeoutPassedToShell() {
        PangrovaCli.CliArgs args = PangrovaCli.parseArgs(
                new String[]{"--host", "h", "--query-timeout", "10m"});
        assertThat(args.queryTimeout).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void rejectsBlankTimeout() {
        assertThatThrownBy(() -> PangrovaCli.parseTimeout(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void rejectsNegativeTimeout() {
        assertThatThrownBy(() -> PangrovaCli.parseTimeout("-5"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidFormat() {
        assertThatThrownBy(() -> PangrovaCli.parseTimeout("abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid query timeout format");
    }

    @Test
    void rejectsZeroTimeout() {
        assertThatThrownBy(() -> PangrovaCli.parseTimeout("0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void parsesHostAndPort() {
        PangrovaCli.CliArgs args = PangrovaCli.parseArgs(
                new String[]{"--host", "server1", "--port", "9090"});
        assertThat(args.host).isEqualTo("server1");
        assertThat(args.port).isEqualTo(9090);
    }
}
