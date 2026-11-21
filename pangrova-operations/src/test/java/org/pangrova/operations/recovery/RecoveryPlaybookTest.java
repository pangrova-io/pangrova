package org.pangrova.operations.recovery;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link RecoveryPlaybook} verifying that handle() dispatches
 * to registered actions and escalates unregistered error codes.
 */
class RecoveryPlaybookTest {

    @Test
    void handle_invokesRegisteredAction() {
        RecoveryPlaybook framework = new RecoveryPlaybook();
        AtomicInteger invocations = new AtomicInteger(0);
        framework.register("TEST_ERROR", ctx -> invocations.incrementAndGet());

        ErrorContext ctx = new ErrorContext("TEST_ERROR", "something failed", Instant.now());
        framework.handle(ctx);

        assertThat(invocations.get()).isEqualTo(1);
    }

    @Test
    void handle_escalatesUnregisteredErrorCode() {
        RecoveryPlaybook framework = new RecoveryPlaybook();

        ErrorContext ctx = new ErrorContext("UNKNOWN", "no action", Instant.now());
        // Should not throw -- escalation is logged, not thrown
        assertThatCode(() -> framework.handle(ctx)).doesNotThrowAnyException();
    }

    @Test
    void handle_escalatesOnActionException() {
        RecoveryPlaybook framework = new RecoveryPlaybook();
        framework.register("BROKEN", ctx -> { throw new RuntimeException("action failed"); });

        ErrorContext ctx = new ErrorContext("BROKEN", "will fail", Instant.now());
        // Should not throw -- exception is caught and escalated
        assertThatCode(() -> framework.handle(ctx)).doesNotThrowAnyException();
    }

    @Test
    void handle_passesContextToAction() {
        RecoveryPlaybook framework = new RecoveryPlaybook();
        AtomicReference<ErrorContext> captured = new AtomicReference<>();
        framework.register("CTX_TEST", captured::set);

        ErrorContext ctx = new ErrorContext("CTX_TEST", "test message", Instant.now());
        framework.handle(ctx);

        assertThat(captured.get()).isSameAs(ctx);
        assertThat(captured.get().errorCode()).isEqualTo("CTX_TEST");
        assertThat(captured.get().message()).isEqualTo("test message");
    }

    @Test
    void handle_multipleRegistrations_dispatchCorrectly() {
        RecoveryPlaybook framework = new RecoveryPlaybook();
        AtomicInteger aCount = new AtomicInteger(0);
        AtomicInteger bCount = new AtomicInteger(0);
        framework.register("A", ctx -> aCount.incrementAndGet());
        framework.register("B", ctx -> bCount.incrementAndGet());

        framework.handle(new ErrorContext("A", "a1", Instant.now()));
        framework.handle(new ErrorContext("A", "a2", Instant.now()));
        framework.handle(new ErrorContext("B", "b1", Instant.now()));

        assertThat(aCount.get()).isEqualTo(2);
        assertThat(bCount.get()).isEqualTo(1);
    }

    @Test
    void register_overwritesPreviousAction() {
        RecoveryPlaybook framework = new RecoveryPlaybook();
        AtomicInteger firstCount = new AtomicInteger(0);
        AtomicInteger secondCount = new AtomicInteger(0);

        framework.register("CODE", ctx -> firstCount.incrementAndGet());
        framework.register("CODE", ctx -> secondCount.incrementAndGet());

        framework.handle(new ErrorContext("CODE", "msg", Instant.now()));

        // Second registration should overwrite first
        assertThat(firstCount.get()).isEqualTo(0);
        assertThat(secondCount.get()).isEqualTo(1);
    }

    @Test
    void errorContext_recordFields() {
        Instant now = Instant.now();
        ErrorContext ctx = new ErrorContext("ERR_CODE", "error message", now);

        assertThat(ctx.errorCode()).isEqualTo("ERR_CODE");
        assertThat(ctx.message()).isEqualTo("error message");
        assertThat(ctx.timestamp()).isEqualTo(now);
    }
}
