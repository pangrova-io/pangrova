package org.pangrova.scheduler.core.retry;

import org.pangrova.scheduler.api.placement.NativeFaultPolicy;
import org.pangrova.scheduler.spi.health.NativeHealthChecker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NativeCrashHandler} and {@link NativeFaultPolicy}.
 */
class NativeCrashHandlerTest {

    @Test
    void failFastPolicyAlwaysFailsTask() {
        NativeCrashHandler handler = new NativeCrashHandler(NativeFaultPolicy.FAIL_FAST, null);
        NativeCrashHandler.CrashAction action = handler.handleCrash("task-1", "SIGSEGV", "null deref");

        assertThat(action).isInstanceOf(NativeCrashHandler.CrashAction.FailTask.class);
        assertThat(action.taskId()).isEqualTo("task-1");
        assertThat(((NativeCrashHandler.CrashAction.FailTask) action).reason())
                .contains("SIGSEGV");
    }

    @Test
    void defaultConstructorUsesFailFast() {
        NativeCrashHandler handler = new NativeCrashHandler();
        assertThat(handler.policy()).isEqualTo(NativeFaultPolicy.FAIL_FAST);
    }

    @Test
    void continueWithHealthCheckPassesThroughWhenHealthy() {
        NativeHealthChecker checker = () ->
                NativeHealthChecker.NativeHealthResult.healthy(1);

        NativeCrashHandler handler = new NativeCrashHandler(
                NativeFaultPolicy.CONTINUE_WITH_HEALTH_CHECK, checker);

        NativeCrashHandler.CrashAction action = handler.handleCrash("task-2", "SIGABRT", "assert");
        assertThat(action).isInstanceOf(NativeCrashHandler.CrashAction.ContinueOnWorker.class);
        assertThat(action.taskId()).isEqualTo("task-2");
    }

    @Test
    void continueWithHealthCheckFailsWhenUnhealthy() {
        NativeHealthChecker checker = () ->
                NativeHealthChecker.NativeHealthResult.unhealthy("heap corrupted", 3);

        NativeCrashHandler handler = new NativeCrashHandler(
                NativeFaultPolicy.CONTINUE_WITH_HEALTH_CHECK, checker);

        NativeCrashHandler.CrashAction action = handler.handleCrash("task-3", "SIGSEGV", "bad ptr");
        assertThat(action).isInstanceOf(NativeCrashHandler.CrashAction.FailTask.class);
        assertThat(((NativeCrashHandler.CrashAction.FailTask) action).reason())
                .contains("health check failed");
    }

    @Test
    void continueWithoutHealthCheckerFallsBackToFailFast() {
        NativeCrashHandler handler = new NativeCrashHandler(
                NativeFaultPolicy.CONTINUE_WITH_HEALTH_CHECK, null);

        NativeCrashHandler.CrashAction action = handler.handleCrash("task-4", "SIGSEGV", "oops");
        assertThat(action).isInstanceOf(NativeCrashHandler.CrashAction.FailTask.class);
    }

    @Test
    void nullPolicyThrows() {
        assertThatThrownBy(() -> new NativeCrashHandler(null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void healthResultFactories() {
        var healthy = NativeHealthChecker.NativeHealthResult.healthy(0);
        assertThat(healthy.healthy()).isTrue();
        assertThat(healthy.crashCount()).isZero();

        var unhealthy = NativeHealthChecker.NativeHealthResult.unhealthy("bad", 5);
        assertThat(unhealthy.healthy()).isFalse();
        assertThat(unhealthy.details()).isEqualTo("bad");
    }
}
