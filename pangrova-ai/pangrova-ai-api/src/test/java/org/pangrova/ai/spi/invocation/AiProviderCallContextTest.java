package org.pangrova.ai.spi.invocation;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.error.AiInvocationException;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiProviderCallContextTest {

    @Test
    void capsTransportTimeoutToTaskDeadline() {
        Instant deadline = Instant.now().plusSeconds(5);

        Duration actual = AiProviderCallContext.callWith(
                "invocation", deadline,
                () -> AiProviderCallContext.remainingTimeout(Duration.ofSeconds(30)));

        assertThat(actual).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(5));
        assertThat(AiProviderCallContext.currentDeadline()).isEmpty();
        assertThat(AiProviderCallContext.currentIdempotencyKey()).isEmpty();
    }

    @Test
    void propagatesDeadlineAndStableOrdinalIntoNestedProviderRequest() {
        Instant deadline = Instant.now().plusSeconds(5);

        String key = AiProviderCallContext.callWith(
                "invocation", deadline,
                () -> AiProviderCallContext.callWithOrdinal(
                        "invocation:request-0", deadline, 3,
                        () -> AiProviderCallContext.currentIdempotencyKey().orElseThrow()));

        assertThat(key).isEqualTo("invocation:request-0:request-3");
    }

    @Test
    void rejectsExpiredProviderCallBeforeIo() {
        assertThatThrownBy(() -> AiProviderCallContext.callWith(
                "invocation", Instant.now().minusSeconds(1), () -> "unreachable"))
                .isInstanceOf(AiInvocationException.class)
                .satisfies(failure -> assertThat(
                        ((AiInvocationException) failure).category())
                        .isEqualTo(AiInvocationException.Category.TIMEOUT));
    }

    @Test
    void expiredDeadlineRejectsPhysicalSendBeforeChargingAdmission() {
        Instant deadline = Instant.now().plusSeconds(5);
        AtomicInteger admissions = new AtomicInteger();

        assertThatThrownBy(() -> AiProviderCallContext.callWith(
                "invocation", deadline, encodedBytes -> {
                    admissions.incrementAndGet();
                    throw new AssertionError("expired callback must not reach admission");
                }, () -> AiProviderCallContext.callWith("nested", Instant.now().minusSeconds(1),
                        () -> AiProviderCallContext.openAttempt(10))))
                .isInstanceOf(AiInvocationException.class)
                .satisfies(failure -> assertThat(
                        ((AiInvocationException) failure).category())
                        .isEqualTo(AiInvocationException.Category.TIMEOUT));
        assertThat(admissions).hasValue(0);
    }
}
