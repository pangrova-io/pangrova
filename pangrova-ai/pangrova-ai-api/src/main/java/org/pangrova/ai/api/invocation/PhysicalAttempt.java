package org.pangrova.ai.api.invocation;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Concurrency ownership and byte admission for exactly one provider send.
 * Implementations serialize concurrent body callbacks and close. The transport must
 * stop delivery before closing; closing does not cancel or join a client exchange.
 */
@Experimental
public interface PhysicalAttempt extends AutoCloseable {
    /**
     * Charges a nonnegative body-byte increment before retaining it. Rejects closed
     * attempts and resource-limit violations; the caller must abort delivery on failure.
     */
    void acceptResponseBytes(long bytes);

    /**
     * Registers cancellation ownership with the exact task attempt before sending.
     * Rejects a closed attempt, revoked task or duplicate exchange registration.
     * The transport owns the returned registration and removes it after finalization.
     */
    ExchangeRegistration registerExchange(ClientExchange exchange);

    /** Releases concurrency exactly once. Repeated close is harmless. */
    @Override
    void close();
}
