package org.pangrova.spi.execution.udf;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link UdfExecutionMode} enum.
 */
class UdfExecutionModeTest {

    @Test
    void allModesAreDefined() {
        assertThat(UdfExecutionMode.values()).containsExactly(
                UdfExecutionMode.EXTERNAL_PROCESS,
                UdfExecutionMode.IN_ENGINE_JAVA,
                UdfExecutionMode.SCALAR_SQL,
                UdfExecutionMode.NATIVE_VECTORIZED);
    }

    @Test
    void valueOfRoundTrips() {
        for (UdfExecutionMode mode : UdfExecutionMode.values()) {
            assertThat(UdfExecutionMode.valueOf(mode.name())).isEqualTo(mode);
        }
    }

    @Test
    void externalProcessIsFirstDeclared() {
        assertThat(UdfExecutionMode.EXTERNAL_PROCESS.ordinal()).isZero();
    }

    @Test
    void configValueParsingFromSnakeCase() {
        String configValue = "in_engine_java";
        UdfExecutionMode resolved = UdfExecutionMode.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(UdfExecutionMode.IN_ENGINE_JAVA);
    }
}
