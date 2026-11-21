package org.pangrova.observability.sdk.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.config.PangrovaConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link ObservabilityConfig} — specifically the environment-aware
 * sampling probability defaults (F-OBS-06 fix).
 *
 * <p>Production defaults to 0.01 (1%) to avoid overwhelming trace backends;
 * non-production environments default to 1.0 for full observability.</p>
 */
class ObservabilityConfigTest {

    private static final Map<String, String> REQUIRED_KEYS = Map.of(
            "observability.service-name", "pangrova-test",
            "observability.otlp-endpoint", "http://localhost:4317");

    @Test
    void acceptsIndependentRateLimitAndParsesHeadersOnceWithoutLeakingValuesOnError() {
        var configured = new ObservabilityConfig(configWith(Map.of(
                "observability.sampling.strategy", "RATE_LIMITING", "observability.sampling.traces-per-second", "24",
                "observability.otlp-headers", "Authorization=Bearer secret==,x-scope=jobs")));
        assertThat(configured.getTracesPerSecond()).isEqualTo(24);
        assertThat(configured.getOtlpHeaders()).containsEntry("authorization", "Bearer secret==");
        assertThatThrownBy(() -> new ObservabilityConfig(configWith(Map.of(
                "observability.otlp-headers", "token=private,Token=other"))))
                .hasMessageContaining("distinct").hasMessageNotContaining("private");
        assertThatThrownBy(() -> new ObservabilityConfig(configWith(Map.of(
                "observability.sampling.traces-per-second", "NaN")))).hasMessageContaining("traces-per-second");
    }

    @Test
    void partialExporterSettingsFailBeforeAllocatingResources() {
        assertThat(ObservabilityConfig.fromDeployment(PangrovaConfiguration.fromYaml(Map.of()))).isEmpty();
        assertThat(ObservabilityConfig.fromDeployment(PangrovaConfiguration.fromYaml(Map.of(
                "observability.enabled", "false", "observability.service-name", "disabled")))).isEmpty();
        assertThatThrownBy(() -> ObservabilityConfig.fromDeployment(PangrovaConfiguration.fromYaml(
                Map.of("observability.service-name", "coordinator"))))
                .hasMessageContaining("observability.otlp-endpoint");
        assertThatThrownBy(() -> ObservabilityConfig.fromDeployment(configWith(
                Map.of("observability.service-name", "  "))))
                .hasMessageContaining("observability.service-name");
    }

    @Test
    void productionEnvironment_defaultsSamplingProbabilityTo001() {
        // When deployment-environment is "production" and no explicit sampling probability
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.deployment-environment", "production"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getSamplingProbability())
                .isEqualTo(ObservabilityConfig.DEFAULT_PRODUCTION_SAMPLING_PROBABILITY);
        assertThat(config.getSamplingProbability()).isEqualTo(0.01);
    }

    @Test
    void defaultEnvironmentIsProduction_soDefaultSamplingIs001() {
        // No explicit deployment-environment → defaults to "production" → sampling = 0.01
        PangrovaConfiguration pangrovaConfig = configWith(Map.of());

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getDeploymentEnvironment()).isEqualTo("production");
        assertThat(config.getSamplingProbability()).isEqualTo(0.01);
    }

    @ParameterizedTest
    @ValueSource(strings = {"development", "test", "staging", "local", "ci"})
    void nonProductionEnvironments_defaultSamplingProbabilityTo1(String environment) {
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.deployment-environment", environment));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getSamplingProbability())
                .isEqualTo(ObservabilityConfig.DEFAULT_NON_PRODUCTION_SAMPLING_PROBABILITY);
        assertThat(config.getSamplingProbability()).isEqualTo(1.0);
    }

    @Test
    void productionEnvironment_caseInsensitive() {
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.deployment-environment", "Production"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getSamplingProbability()).isEqualTo(0.01);
    }

    @Test
    void explicitSamplingProbability_overridesEnvironmentDefault_inProduction() {
        // Explicit probability in production overrides the 0.01 default
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.deployment-environment", "production",
                "observability.sampling.probability", "0.1"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getSamplingProbability()).isEqualTo(0.1);
    }

    @Test
    void explicitSamplingProbability_overridesEnvironmentDefault_inDevelopment() {
        // Explicit lower probability in development overrides the 1.0 default
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.deployment-environment", "development",
                "observability.sampling.probability", "0.5"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getSamplingProbability()).isEqualTo(0.5);
    }

    @Test
    void explicitSamplingProbability_zeroIsAllowed() {
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.sampling.probability", "0.0"));

        ObservabilityConfig config = new ObservabilityConfig(pangrovaConfig);

        assertThat(config.getSamplingProbability()).isEqualTo(0.0);
    }

    @Test
    void samplingProbabilityAboveOne_throws() {
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.sampling.probability", "1.5"));
        assertThatThrownBy(() -> new ObservabilityConfig(pangrovaConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Sampling probability");
    }

    @Test
    void negativeSamplingProbability_throws() {
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.sampling.probability", "-0.1"));
        assertThatThrownBy(() -> new ObservabilityConfig(pangrovaConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Sampling probability");
    }

    @Test
    void nonPositiveMetricsInterval_throws() {
        PangrovaConfiguration pangrovaConfig = configWith(Map.of(
                "observability.metrics.interval-ms", "0"));
        assertThatThrownBy(() -> new ObservabilityConfig(pangrovaConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Metrics interval");
    }

    private static PangrovaConfiguration configWith(Map<String, String> overrides) {
        Map<String, String> merged = new HashMap<>(REQUIRED_KEYS);
        merged.putAll(overrides);
        return PangrovaConfiguration.fromYaml(merged);
    }
}
