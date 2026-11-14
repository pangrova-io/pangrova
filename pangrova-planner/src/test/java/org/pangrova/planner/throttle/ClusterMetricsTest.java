package org.pangrova.planner.throttle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterMetricsTest {

    @Test
    void validMetrics_created() {
        var metrics = new ClusterMetrics(0.5, 0.6, 0.01);
        assertThat(metrics.cpuUsage()).isEqualTo(0.5);
        assertThat(metrics.memoryUsage()).isEqualTo(0.6);
        assertThat(metrics.errorRate()).isEqualTo(0.01);
    }

    @Test
    void negativeCpu_throws() {
        assertThatThrownBy(() -> new ClusterMetrics(-0.1, 0.5, 0.01))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cpuAboveOne_throws() {
        assertThatThrownBy(() -> new ClusterMetrics(1.1, 0.5, 0.01))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
