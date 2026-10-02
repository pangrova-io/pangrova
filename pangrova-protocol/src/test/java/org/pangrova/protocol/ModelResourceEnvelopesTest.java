package org.pangrova.protocol;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CapabilityLimits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ModelResourceEnvelopesTest {
    @Test
    void tokenLimitsAreOnlyPerRequestAndDoNotDefineBodyByteBudgets() {
        var limits = limits();
        var resources = ModelResourceEnvelopes.resources(limits, 10, false);
        assertThat(resources.getProviderItems()).isEqualTo(10);
        assertThat(resources.getProviderAttempts()).isEqualTo(7);
        assertThat(resources.getProviderRequestBytes()).isEqualTo(7 * 4096);
        assertThat(resources.getProviderResponseBytes()).isEqualTo(7 * 2048);
        assertThat(ModelResourceEnvelopes.requestLimits(limits).getMaxInputTokens()).isEqualTo(8192);
        assertThat(ModelResourceEnvelopes.requestLimits(limits).getMaxRequestBytes()).isEqualTo(4096);
        var changed = limits.toBuilder().setMaxInputTokens(16384).build();
        assertThat(ModelResourceEnvelopes.resources(changed, 10, false)).isEqualTo(resources);
    }

    @Test
    void invalidBoundsCannotBeClampedOrDerivedFromTokens() {
        for (var invalid : java.util.List.of(limits().toBuilder().clearMaxRequestBytes().build(),
                limits().toBuilder().clearMaxResponseBytes().build(),
                limits().toBuilder().setMaxProviderAttemptsPerTask(-1).build())) {
            assertThatThrownBy(() -> ModelResourceEnvelopes.resources(invalid, 10, false))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> ModelResourceEnvelopes.resources(limits(), 11, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Logical provider items");
        assertThatThrownBy(() -> ModelResourceEnvelopes.resources(limits().toBuilder()
                .setMaxProviderAttemptsPerTask(1).build(), 10, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("physical attempt");
    }

    @Test
    void cumulativeAttemptBodyAndMediaOverflowAreRejected() {
        assertThatThrownBy(() -> ModelResourceEnvelopes.resources(limits().toBuilder()
                .setMaxRequestBytes(Long.MAX_VALUE).build(), 10, false)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> ModelResourceEnvelopes.resources(limits().toBuilder()
                .setMaxMediaBytes(Long.MAX_VALUE).build(), 10, true)).isInstanceOf(ArithmeticException.class);
    }

    private static CapabilityLimits limits() {
        return CapabilityLimits.newBuilder().setMaxBatchItems(4).setMaxConcurrency(2).setMaxInputTokens(8192)
                .setMaxRequestBytes(4096).setMaxResponseBytes(2048).setMaxProviderItemsPerTask(10)
                .setMaxProviderAttemptsPerTask(7).build();
    }
}
