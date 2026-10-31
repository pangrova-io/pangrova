package org.pangrova.worker.exchange;

import org.pangrova.worker.exchange.source.ExchangeSourceRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates ExchangeSourceRegistry commit notification tracking.
 * The registry tracks upstream commit state for the HTTP endpoint contract.
 */
class ExchangeSourceRegistryTest {

    private ExchangeSourceRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ExchangeSourceRegistry();
    }

    @Test
    void notifyCommitDoesNotThrow() {
        assertDoesNotThrow(() -> registry.notifyCommit("q1", 1));
    }

    @Test
    void cleanupDoesNotThrow() {
        registry.notifyCommit("q1", 1);
        assertDoesNotThrow(() -> registry.cleanup("q1"));
    }

    @Test
    void cleanupIsIdempotent() {
        registry.cleanup("nonexistent");
        assertDoesNotThrow(() -> registry.cleanup("nonexistent"));
    }
}
