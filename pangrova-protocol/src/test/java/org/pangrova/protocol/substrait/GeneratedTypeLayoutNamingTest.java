package org.pangrova.protocol.substrait;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.substrait.generated.GeneratedTypeLayoutNaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedTypeLayoutNamingTest {
    @Test
    void namesAreGeneratedFromTheActualPhysicalBridgeDeclaration() {
        assertThat(GeneratedTypeLayoutNaming.anonymousComputedField(0)).isEqualTo("field_0");
        assertThat(GeneratedTypeLayoutNaming.anonymousComputedField(Integer.MAX_VALUE))
                .isEqualTo("field_2147483647");
    }

    @Test
    void negativeOrdinalsNeverBecomeADeclaredFieldName() {
        assertThatThrownBy(() -> GeneratedTypeLayoutNaming.anonymousComputedField(-1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nonnegative");
        assertThatThrownBy(() -> GeneratedTypeLayoutNaming.anonymousComputedField(Integer.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nonnegative");
    }
}
