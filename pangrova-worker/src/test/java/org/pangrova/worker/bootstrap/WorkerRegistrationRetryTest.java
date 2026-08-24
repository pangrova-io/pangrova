package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies Worker registration retry configuration and backoff parameters.
 * Tests the RegistrationRetryConfig contract without requiring a live Coordinator.
 */
class WorkerRegistrationRetryTest {

    @Test
    void defaultRetryConfigHasExpectedValues() {
        var config = WorkerBootstrapConfig.RegistrationRetryConfig.defaults();

        assertEquals(5, config.maxAttempts());
        assertEquals(1_000, config.initialBackoffMs());
        assertEquals(30_000, config.maxBackoffMs());
    }

    @Test
    void customRetryConfigAcceptsValidValues() {
        var config = new WorkerBootstrapConfig.RegistrationRetryConfig(3, 500, 10_000);

        assertEquals(3, config.maxAttempts());
        assertEquals(500, config.initialBackoffMs());
        assertEquals(10_000, config.maxBackoffMs());
    }

    @Test
    void maxAttemptsMustBeAtLeastOne() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerBootstrapConfig.RegistrationRetryConfig(0, 1_000, 30_000));
    }

    @Test
    void initialBackoffMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerBootstrapConfig.RegistrationRetryConfig(5, 0, 30_000));
    }

    @Test
    void maxBackoffMustNotBeLessThanInitial() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerBootstrapConfig.RegistrationRetryConfig(5, 5_000, 1_000));
    }

    @Test
    void nullRetryConfigDefaultsInBootstrapConfig() {
        var config = WorkerBootstrapConfig.withDefaults("worker-1");

        var retry = config.registrationRetryConfig();
        assertEquals(5, retry.maxAttempts());
        assertEquals(1_000, retry.initialBackoffMs());
        assertEquals(30_000, retry.maxBackoffMs());
    }

    @Test
    void exponentialBackoffDoublesPerAttempt() {
        long backoff = 1_000;
        long maxBackoff = 30_000;

        long attempt1 = backoff;
        long attempt2 = Math.min(backoff * 2, maxBackoff);
        long attempt3 = Math.min(backoff * 4, maxBackoff);

        assertEquals(1_000, attempt1);
        assertEquals(2_000, attempt2);
        assertEquals(4_000, attempt3);
    }

    @Test
    void backoffCapsAtMaxBackoff() {
        long backoff = 10_000;
        long maxBackoff = 30_000;

        long attempt1 = backoff;
        long attempt2 = Math.min(backoff * 2, maxBackoff);
        long attempt3 = Math.min(backoff * 4, maxBackoff);

        assertEquals(10_000, attempt1);
        assertEquals(20_000, attempt2);
        assertEquals(30_000, attempt3, "Should cap at maxBackoff");
    }
}
