package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CodeGenerationStrategy} enum.
 */
class CodeGenerationStrategyTest {

    @Test
    void allStrategiesAreDefined() {
        assertThat(CodeGenerationStrategy.values())
                .containsExactly(
                        CodeGenerationStrategy.INTERPRETED,
                        CodeGenerationStrategy.JIT_COMPILED,
                        CodeGenerationStrategy.AOT_COMPILED);
    }

    @Test
    void valueOfRoundTrips() {
        for (CodeGenerationStrategy strategy : CodeGenerationStrategy.values()) {
            assertThat(CodeGenerationStrategy.valueOf(strategy.name())).isEqualTo(strategy);
        }
    }

    @Test
    void interpretedIsDefault() {
        // INTERPRETED should be ordinal 0 — the conventional default position
        assertThat(CodeGenerationStrategy.INTERPRETED.ordinal()).isZero();
    }

    @Test
    void configValueParsingFromLowerCase() {
        String configValue = "interpreted";
        CodeGenerationStrategy resolved =
                CodeGenerationStrategy.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(CodeGenerationStrategy.INTERPRETED);
    }

    @Test
    void configValueParsingJitCompiled() {
        String configValue = "jit_compiled";
        CodeGenerationStrategy resolved =
                CodeGenerationStrategy.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(CodeGenerationStrategy.JIT_COMPILED);
    }

    @Test
    void configValueParsingAotCompiled() {
        String configValue = "aot_compiled";
        CodeGenerationStrategy resolved =
                CodeGenerationStrategy.valueOf(configValue.toUpperCase());
        assertThat(resolved).isEqualTo(CodeGenerationStrategy.AOT_COMPILED);
    }
}
