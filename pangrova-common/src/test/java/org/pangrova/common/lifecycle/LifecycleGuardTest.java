package org.pangrova.common.lifecycle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LifecycleGuard}.
 *
 * <p>Validates: Requirements 10.1-10.5
 */
class LifecycleGuardTest {

    // ── Initial state ──

    @Test
    void newGuard_isInactive() {
        var guard = new LifecycleGuard();

        assertThat(guard.isActive()).isFalse();
    }

    // ── tryActivate ──

    @Test
    void tryActivate_firstCall_returnsTrue() {
        var guard = new LifecycleGuard();

        assertThat(guard.tryActivate()).isTrue();
        assertThat(guard.isActive()).isTrue();
    }

    @Test
    void tryActivate_secondCall_returnsFalse() {
        var guard = new LifecycleGuard();
        guard.tryActivate();

        assertThat(guard.tryActivate()).isFalse();
        assertThat(guard.isActive()).isTrue();
    }

    // ── tryDeactivate ──

    @Test
    void tryDeactivate_afterActivate_returnsTrue() {
        var guard = new LifecycleGuard();
        guard.tryActivate();

        assertThat(guard.tryDeactivate()).isTrue();
        assertThat(guard.isActive()).isFalse();
    }

    @Test
    void tryDeactivate_whenAlreadyInactive_returnsFalse() {
        var guard = new LifecycleGuard();

        assertThat(guard.tryDeactivate()).isFalse();
        assertThat(guard.isActive()).isFalse();
    }

    // ── Full cycle ──

    @Test
    void fullCycle_activateDeactivateActivateAgain() {
        var guard = new LifecycleGuard();

        assertThat(guard.tryActivate()).isTrue();
        assertThat(guard.isActive()).isTrue();

        assertThat(guard.tryDeactivate()).isTrue();
        assertThat(guard.isActive()).isFalse();

        assertThat(guard.tryActivate()).isTrue();
        assertThat(guard.isActive()).isTrue();
    }
}
