package org.pangrova.observability.sdk.logging;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlowQueryLogTest {

    @Test
    void queryBelowThresholdIsNotLogged() {
        SlowQueryLog log = new SlowQueryLog(Duration.ofSeconds(5));
        SlowQueryLog.SlowQueryEntry entry = new SlowQueryLog.SlowQueryEntry(
                "q-1", "SELECT 1", "user1", Duration.ofSeconds(3),
                "TableScan", 100, 1024, 2048, 200);

        assertThat(log.evaluate(entry)).isFalse();
    }

    @Test
    void queryExceedingThresholdIsLogged() {
        SlowQueryLog log = new SlowQueryLog(Duration.ofSeconds(5));
        SlowQueryLog.SlowQueryEntry entry = new SlowQueryLog.SlowQueryEntry(
                "q-2", "SELECT * FROM big_table", "user2", Duration.ofSeconds(10),
                "HashJoin -> TableScan", 1_000_000, 1_073_741_824, 536_870_912, 8000);

        assertThat(log.evaluate(entry)).isTrue();
    }

    @Test
    void queryExactlyAtThresholdIsNotLogged() {
        SlowQueryLog log = new SlowQueryLog(Duration.ofSeconds(5));
        SlowQueryLog.SlowQueryEntry entry = new SlowQueryLog.SlowQueryEntry(
                "q-3", "SELECT 1", "user3", Duration.ofSeconds(5),
                "Scan", 10, 100, 200, 50);

        assertThat(log.evaluate(entry)).isFalse();
    }

    @Test
    void nullThresholdThrowsException() {
        assertThatThrownBy(() -> new SlowQueryLog(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void zeroThresholdThrowsException() {
        assertThatThrownBy(() -> new SlowQueryLog(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankQueryIdThrowsException() {
        assertThatThrownBy(() -> new SlowQueryLog.SlowQueryEntry(
                "", "SELECT 1", "user", Duration.ofSeconds(1),
                "plan", 0, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getThresholdReturnsConfiguredValue() {
        Duration threshold = Duration.ofMinutes(2);
        SlowQueryLog log = new SlowQueryLog(threshold);
        assertThat(log.getThreshold()).isEqualTo(threshold);
    }
}
