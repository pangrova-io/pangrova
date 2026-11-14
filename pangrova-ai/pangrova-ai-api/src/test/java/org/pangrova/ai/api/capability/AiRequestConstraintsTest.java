package org.pangrova.ai.api.capability;

import org.pangrova.ai.api.exception.AiInvocationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiRequestConstraintsTest {
    @Test
    void requestAndResponseBodiesHaveIndependentExactInclusiveLimits() {
        var limits = new AiRequestConstraints(8192, 0, 100, 400);
        limits.requireRequestBytes(0);
        limits.requireRequestBytes(100);
        limits.requireResponseBytes(400);
        assertRejected(() -> limits.requireRequestBytes(101), "request");
        assertRejected(() -> limits.requireResponseBytes(401), "response");
        assertThat(limits.maxInputTokens()).isEqualTo(8192);
        assertThat(limits.maxOutputTokens()).isZero();
    }

    @Test
    void rejectsMissingNegativeAndZeroHardLimits() {
        for (long invalid : new long[]{Long.MIN_VALUE, -1, 0}) {
            assertThatThrownBy(() -> new AiRequestConstraints(invalid, 0, 1, 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new AiRequestConstraints(1, 0, invalid, 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new AiRequestConstraints(1, 0, 1, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new AiRequestConstraints(1, -1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void largeByteLimitsDoNotOverflowOrTurnIntoTokenEstimates() {
        var limits = new AiRequestConstraints(1, 0, Long.MAX_VALUE, Long.MAX_VALUE);
        limits.requireRequestBytes(Long.MAX_VALUE);
        limits.requireResponseBytes(Long.MAX_VALUE);
        assertThatThrownBy(() -> limits.requireRequestBytes(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limits.requireResponseBytes(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void capabilityRequiresBothLogicalItemsAndPhysicalAttempts() {
        var request = new AiRequestConstraints(1, 0, 1, 1);
        assertThatThrownBy(() -> new AiCapabilityDescriptor.ResourceLimits(null, 0, 1, 1, 1))
                .isInstanceOf(NullPointerException.class);
        for (long invalid : new long[]{-1, 0}) {
            assertThatThrownBy(() -> new AiCapabilityDescriptor.ResourceLimits(request, 0, 1, invalid, 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new AiCapabilityDescriptor.ResourceLimits(request, 0, 1, 1, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new AiCapabilityDescriptor.ResourceLimits(request, -1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AiCapabilityDescriptor.ResourceLimits(request, 0, 0, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertRejected(Runnable action, String direction) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(AiInvocationException.class, failure -> {
            assertThat(failure.category()).isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED);
            assertThat(failure.retryable()).isFalse();
        }).hasMessageContaining(direction + " body");
    }
}
