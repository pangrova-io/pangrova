package org.pangrova.protocol.function;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.function.declaration.DeclaredFunctionOption;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeclaredFunctionOptionTest {
    @Test
    void windowTreatmentIsSelectedFromTheSoleDeclaration() {
        var option = FunctionBindingDeclarations.options("WINDOW", "window", "first_value").get(0);
        assertThat(option.behavior()).isEqualTo("NULL_TREATMENT");
        assertThat(option.select(true).getName()).isEqualTo("null_treatment");
        assertThat(option.select(true).getPreferenceList()).containsExactly("IGNORE_NULLS");
        assertThat(option.select(false).getPreferenceList()).containsExactly("RESPECT_NULLS");
        assertThat(FunctionBindingDeclarations.option("WINDOW", "window", "lead", "NULL_TREATMENT", true))
                .isEqualTo(option.select(true));
    }

    @Test
    void unsupportedBehaviorCannotBeInventedForAnotherFunction() {
        assertThat(FunctionBindingDeclarations.options("WINDOW", "window", "sum")).isEmpty();
        assertThatThrownBy(() -> FunctionBindingDeclarations.option("WINDOW", "window", "sum", "NULL_TREATMENT", true))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not support");
        assertThatThrownBy(() -> FunctionBindingDeclarations.option("WINDOW", "window", "first_value", "unknown", true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void malformedOptionsHaveNoDefaultRepair() {
        assertThatThrownBy(() -> new DeclaredFunctionOption("null_treatment", "unknown", "I", "R"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeclaredFunctionOption("null_treatment", "NULL_TREATMENT", "same", "same"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
