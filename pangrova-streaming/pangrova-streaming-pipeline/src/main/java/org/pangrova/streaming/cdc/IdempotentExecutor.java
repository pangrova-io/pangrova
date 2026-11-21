package org.pangrova.streaming.cdc;

import java.util.function.Supplier;

/**
 * Strategy for idempotent event processing.
 * Implementations ensure that the supplier is executed at most once
 * for a given key within a deduplication window.
 *
 * <p>The wiring layer maps this to the concrete IdempotencyManager
 * in common.resilience, keeping pangrova-streaming free of that dependency.</p>
 */
@FunctionalInterface
public interface IdempotentExecutor {

    /**
     * Executes the supplier idempotently for the given key.
     *
     * @param key      deduplication key (e.g. CDC event composite ID)
     * @param supplier the action to execute if not already processed
     * @param <T>      result type
     * @return the result of the supplier (cached or fresh)
     */
    <T> T execute(String key, Supplier<T> supplier);

    /** No-op executor that always runs the supplier (no deduplication). */
    static IdempotentExecutor passThrough() {
        return new IdempotentExecutor() {
            @Override
            public <T> T execute(String key, Supplier<T> supplier) {
                return supplier.get();
            }
        };
    }
}
