package org.pangrova.planner.core.adaptive;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link AdaptiveQueryConfig} default values, backward compatibility,
 * and validation constraints.
 *
 * <p><b>Validates: Requirements REQ-4</b></p>
 */
class AdaptiveQueryConfigTest {

    private static final long EXPECTED_256MB = 256L * 1024 * 1024;

    @Test
    void rejectsNonFiniteAndNonSkewRatios() {
        for (double ratio : new double[] {Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, 0.5, 1.0}) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new AdaptiveQueryConfig(true, 100, ratio, 0, 2))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void singleArgConstructor_usesDefaults() {
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true);

        assertThat(config.enabled()).isTrue();
        assertThat(config.targetPartitionBytes()).isEqualTo(EXPECTED_256MB);
        assertThat(config.skewPartitionThresholdRatio()).isEqualTo(5.0);
        assertThat(config.skewPartitionMinBytes()).isEqualTo(EXPECTED_256MB);
        assertThat(config.minPartitionsForCoalesce()).isEqualTo(2);
    }

    @Test
    void disabledConstant_hasCorrectDefaults() {
        assertThat(AdaptiveQueryConfig.DISABLED.enabled()).isFalse();
        assertThat(AdaptiveQueryConfig.DISABLED.targetPartitionBytes()).isEqualTo(EXPECTED_256MB);
        assertThat(AdaptiveQueryConfig.DISABLED.skewPartitionThresholdRatio()).isEqualTo(5.0);
        assertThat(AdaptiveQueryConfig.DISABLED.skewPartitionMinBytes()).isEqualTo(EXPECTED_256MB);
        assertThat(AdaptiveQueryConfig.DISABLED.minPartitionsForCoalesce()).isEqualTo(2);
    }

    @Test
    void enabledConstant_hasCorrectDefaults() {
        assertThat(AdaptiveQueryConfig.ENABLED.enabled()).isTrue();
        assertThat(AdaptiveQueryConfig.ENABLED.targetPartitionBytes()).isEqualTo(EXPECTED_256MB);
    }

    @Test
    void fullConstructor_customValues() {
        AdaptiveQueryConfig config = new AdaptiveQueryConfig(true, 128L * 1024 * 1024, 3.0, 64L * 1024 * 1024, 4);

        assertThat(config.targetPartitionBytes()).isEqualTo(128L * 1024 * 1024);
        assertThat(config.skewPartitionThresholdRatio()).isEqualTo(3.0);
        assertThat(config.skewPartitionMinBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(config.minPartitionsForCoalesce()).isEqualTo(4);
    }

    @Test
    void backwardCompatibility_singleArgMatchesDisabledConstant() {
        AdaptiveQueryConfig fromCtor = new AdaptiveQueryConfig(false);
        assertThat(fromCtor).isEqualTo(AdaptiveQueryConfig.DISABLED);
    }

    @Test
    void validation_targetPartitionBytesZero_throws() {
        assertThatThrownBy(() -> new AdaptiveQueryConfig(true, 0, 5.0, 256L, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetPartitionBytes");
    }

    @Test
    void validation_negativeSkewRatio_throws() {
        assertThatThrownBy(() -> new AdaptiveQueryConfig(true, 1024, -1.0, 256L, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("skewPartitionThresholdRatio");
    }

    @Test
    void validation_negativeSkewMinBytes_throws() {
        assertThatThrownBy(() -> new AdaptiveQueryConfig(true, 1024, 5.0, -1L, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("skewPartitionMinBytes");
    }

    @Test
    void validation_minPartitionsZero_throws() {
        assertThatThrownBy(() -> new AdaptiveQueryConfig(true, 1024, 5.0, 256L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minPartitionsForCoalesce");
    }
}
