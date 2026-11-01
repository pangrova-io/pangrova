package org.pangrova.streaming.cdc;

import org.pangrova.streaming.watermark.LateDataPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcPipelinePoliciesTest {

    @Test
    void defaultsWhenNoStreamProperties() {
        CdcPipelinePolicies policies =
                CdcPipelinePolicies.fromStreamProperties(Map.of(), 7L);

        assertThat(policies.watermark().watermarkColumn()).isEqualTo("event_time");
        assertThat(policies.watermark().watermarkDelay()).isEqualTo(Duration.ofSeconds(10));
        assertThat(policies.watermark().idleTimeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(policies.lateData().policy()).isEqualTo(LateDataPolicy.UPDATE);
        assertThat(policies.lateData().allowedLateness()).isEqualTo(Duration.ZERO);
        assertThat(policies.deadLetter()).isEqualTo(DeadLetterPolicy.FAIL_PIPELINE);
        assertThat(policies.pipelineEpoch()).isEqualTo(7L);
    }

    @Test
    void parsesWatermarkDelayWithNumericSuffix() {
        CdcPipelinePolicies policies = CdcPipelinePolicies.fromStreamProperties(
                Map.of("watermark_delay", "30s"), 0L);

        assertThat(policies.watermark().watermarkDelay()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void parsesWatermarkDelayAsIsoDuration() {
        CdcPipelinePolicies policies = CdcPipelinePolicies.fromStreamProperties(
                Map.of("watermark_delay", "PT1M"), 0L);

        assertThat(policies.watermark().watermarkDelay()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void parsesWatermarkDelayAsBareMilliseconds() {
        CdcPipelinePolicies policies = CdcPipelinePolicies.fromStreamProperties(
                Map.of("watermark_delay", "500"), 0L);

        assertThat(policies.watermark().watermarkDelay()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void invalidWatermarkDelayFailsPipeline() {
        assertThatThrownBy(() -> CdcPipelinePolicies.fromStreamProperties(
                Map.of("watermark_delay", "fast"), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("watermark_delay");
    }

    @Test
    void parsesDropLateDataPolicyCaseInsensitively() {
        CdcPipelinePolicies policies = CdcPipelinePolicies.fromStreamProperties(
                Map.of("late_data_policy", "DROP"), 0L);

        assertThat(policies.lateData().policy()).isEqualTo(LateDataPolicy.DROP);
    }

    @Test
    void invalidLateDataPolicyFailsPipeline() {
        assertThatThrownBy(() -> CdcPipelinePolicies.fromStreamProperties(
                Map.of("late_data_policy", "purge"), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("late_data_policy");
    }

    @Test
    void parsesAllowedLateness() {
        CdcPipelinePolicies policies = CdcPipelinePolicies.fromStreamProperties(
                Map.of("allowed_lateness", "1m"), 0L);

        assertThat(policies.lateData().allowedLateness()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void invalidAllowedLatenessFailsPipeline() {
        assertThatThrownBy(() -> CdcPipelinePolicies.fromStreamProperties(
                Map.of("allowed_lateness", "soon"), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed_lateness");
    }

    @Test
    void blankPropertyValuesFallBackToDefaults() {
        CdcPipelinePolicies policies = CdcPipelinePolicies.fromStreamProperties(
                Map.of("watermark_delay", " ", "late_data_policy", ""), 0L);

        assertThat(policies.watermark().watermarkDelay()).isEqualTo(Duration.ofSeconds(10));
        assertThat(policies.lateData().policy()).isEqualTo(LateDataPolicy.UPDATE);
    }
}
