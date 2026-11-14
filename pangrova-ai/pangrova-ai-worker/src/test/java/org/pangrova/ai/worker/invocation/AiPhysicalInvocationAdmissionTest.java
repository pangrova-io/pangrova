package org.pangrova.ai.worker.invocation;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.capability.AiRequestConstraints;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real canonical registration/admission; no provider transport or distributed acceptance claim. */
class AiPhysicalInvocationAdmissionTest {
    @Test
    void chargesEveryAttemptAndRequestEntityIncludingRetries() {
        try (var budget = budget(2, 18, 12)) {
            try (var attempt = budget.openAttempt(9)) { attempt.acceptResponseBytes(1); }
            try (var attempt = budget.openAttempt(9)) { attempt.acceptResponseBytes(1); }
            assertBudgetFailure(() -> budget.openAttempt(1));
            assertThat(budget.usage().getProviderItems()).isEqualTo(1);
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
            assertThat(budget.usage().getProviderRequestBytes()).isEqualTo(18);
            assertThat(budget.usage().getProviderResponseBytes()).isEqualTo(2);
        }
    }

    @Test
    void rejectsPerRequestSizeBeforeChargingAttemptOrTakingPermits() {
        try (var budget = budget(2, 18, 12)) {
            assertBudgetFailure(() -> budget.openAttempt(11));
            assertThat(budget.usage().getProviderAttempts()).isZero();
            assertThat(budget.usage().getProviderRequestBytes()).isZero();
            try (var accepted = budget.openAttempt(10)) {
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
            }
        }
    }

    @Test
    void observesRejectedResponseChunksButNeverRetainsBeyondLimits() {
        try (var budget = budget(4, 40, 12)) {
            try (var first = budget.openAttempt(1)) {
                first.acceptResponseBytes(5);
                first.acceptResponseBytes(3);
                assertBudgetFailure(() -> first.acceptResponseBytes(1));
            }
            try (var second = budget.openAttempt(1)) {
                second.acceptResponseBytes(3);
                assertBudgetFailure(() -> second.acceptResponseBytes(2));
            }
            assertThat(budget.usage().getProviderResponseBytes()).isEqualTo(11);
            assertThat(budget.usage().getObservedProviderResponseBytes()).isEqualTo(14);
        }
    }

    @Test
    void physicalPermitsRemainOwnedUntilResponseAttemptCloses() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var budget = budget(4, 40, 32);
                var first = budget.openAttempt(1);
                var second = budget.openAttempt(1)) {
            var waiting = executor.submit(() -> budget.openAttempt(1));
            await().atMost(Duration.ofSeconds(5)).until(budget::hasQueuedAttempts);
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
            first.acceptResponseBytes(1);
            assertThat(waiting.isDone()).isFalse();
            first.close();
            try (var admitted = waiting.get(5, TimeUnit.SECONDS)) {
                admitted.acceptResponseBytes(1);
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(3);
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void cancellationWakesQueuedSendWithoutRetiringEvidenceOrClosingLiveAttempts() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var budget = budget(4, 40, 32);
                var first = budget.openAttempt(1);
                var second = budget.openAttempt(1)) {
            first.acceptResponseBytes(3);
            var waiting = executor.submit(() -> budget.openAttempt(1));
            await().atMost(Duration.ofSeconds(5)).until(budget::hasQueuedAttempts);
            budget.cancel();
            budget.cancel();
            assertThatThrownBy(() -> waiting.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .cause().isInstanceOf(AiInvocationException.class)
                    .extracting("category").isEqualTo(AiInvocationException.Category.AUTHORIZATION);
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
            assertThat(budget.usage().getProviderRequestBytes()).isEqualTo(2);
            assertThat(budget.usage().getProviderResponseBytes()).isEqualTo(3);
            assertThatThrownBy(() -> first.acceptResponseBytes(1))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("revoked");
            first.close();
            second.close();
            assertThat(budget.usage().getObservedProviderResponseBytes()).isEqualTo(4);
            assertThat(budget.usage().getProviderResponseBytes()).isEqualTo(3);
            assertThatThrownBy(() -> budget.openAttempt(1))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("revoked");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void taskTerminationRejectsMoreAttemptsAndResponseRetention() {
        try (var budget = budget(4, 40, 32); var inFlight = budget.openAttempt(1)) {
            budget.terminate();
            assertThatThrownBy(() -> budget.openAttempt(1))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("released");
            assertThatThrownBy(() -> inFlight.acceptResponseBytes(1))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("released");
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
            assertThat(budget.usage().getProviderResponseBytes()).isZero();
            assertThat(budget.usage().getObservedProviderResponseBytes()).isEqualTo(1);
        }
    }

    @Test
    void responseAndAttemptCloseAreStrictAndIdempotent() {
        try (var budget = budget(4, 40, 32)) {
            var attempt = budget.openAttempt(1);
            assertThatThrownBy(() -> attempt.acceptResponseBytes(-1)).isInstanceOf(IllegalArgumentException.class);
            assertThat(budget.usage().getProviderResponseBytes()).isZero();
            attempt.close();
            attempt.close();
            assertThatThrownBy(() -> attempt.acceptResponseBytes(1)).isInstanceOf(IllegalStateException.class);
            budget.close();
            assertBudgetFailure(() -> budget.openAttempt(1));
        }
    }

    private static AiTransportTestAdmission budget(long attempts, long requestBytes, long responseBytes) {
        var limits = new AiTransportTestAdmission.Limits(2, 10, attempts, requestBytes, responseBytes, 0,
                new AiRequestConstraints(100, 100, 10, 8));
        return new AiTransportTestAdmission(AiCapabilityDescriptor.Operation.TEXT_GENERATION, 0,
                1, 1, limits, Instant.now().plusSeconds(30));
    }

    private static void assertBudgetFailure(org.assertj.core.api.ThrowableAssert.ThrowingCallable operation) {
        assertThatThrownBy(operation).isInstanceOf(AiInvocationException.class).satisfies(failure ->
                assertThat(((AiInvocationException) failure).category())
                        .isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED));
    }
}
