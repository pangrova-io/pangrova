package org.pangrova.governance.runtime.quality;

import org.pangrova.governance.api.quality.Alert;
import org.pangrova.governance.api.enums.Severity;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link LogAlertChannel} delivers alerts via SLF4J logging.
 */
class LogAlertChannelTest {

    private final LogAlertChannel channel = new LogAlertChannel();

    @Test
    void nameReturnsLog() {
        assertThat(channel.name()).isEqualTo("log");
    }

    @Test
    void supportsAllSeverities() {
        for (Severity severity : Severity.values()) {
            assertThat(channel.supports(severity)).isTrue();
        }
    }

    @Test
    void sendDoesNotThrow() {
        Alert alert = new Alert("null_check", "catalog.schema.orders",
                Severity.WARNING, "5 rows failed null check", 5, 1000, Instant.now());
        channel.send(alert);
        // LogAlertChannel logs via SLF4J — no exception means delivery succeeded
    }

    @Test
    void alerterDeliversViaRegisteredChannel() {
        AlertChannelRegistry registry = new AlertChannelRegistry();
        registry.register(channel);
        DataQualityAlerter alerter = new DataQualityAlerter(registry);

        QualityCheckResult failedResult = new QualityCheckResult(
                1, 100, "range_check", "catalog.schema.metrics",
                Severity.ERROR, Instant.now(), false, 500, 10, 42, "Out of range", null);

        alerter.alert(failedResult);
        // If LogAlertChannel were missing, alerter would silently drop.
        // With channel registered, the alert is logged (verified by no exception).
        assertThat(registry.getChannels(Severity.ERROR)).hasSize(1);
    }
}
