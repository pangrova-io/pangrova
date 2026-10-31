/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.worker.function;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Micro-batch accumulator for amortizing per-row costs across multiple
 * concurrent callers.
 *
 * <p>Each caller submits an input via {@link #submit} and blocks until the
 * batch fires. A batch fires when {@code maxBatchSize} inputs accumulate, or
 * after {@code flushTimeoutMs} milliseconds if the batch is still partial.</p>
 *
 * <p>Thread-safe: designed for concurrent JNI callback threads filling
 * the buffer simultaneously.</p>
 *
 * @param <I> input type per row
 * @param <O> output type per row
 */
public final class AiBatchBuffer<I, O> {

    private static final long DEFAULT_FLUSH_TIMEOUT_MS = 100;

    private final int maxBatchSize;
    private final long flushTimeoutMs;
    private final Function<List<I>, List<O>> batchExecutor;
    private final ConcurrentLinkedQueue<PendingItem<I, O>> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);

    /**
     * Creates a buffer with the default flush timeout.
     *
     * @param maxBatchSize  maximum inputs accumulated before firing a batch
     * @param batchExecutor function executing one batch of inputs
     */
    public AiBatchBuffer(int maxBatchSize, Function<List<I>, List<O>> batchExecutor) {
        this(maxBatchSize, batchExecutor, DEFAULT_FLUSH_TIMEOUT_MS);
    }

    /**
     * Creates a buffer with a custom flush timeout.
     *
     * @param maxBatchSize   maximum inputs accumulated before firing a batch
     * @param batchExecutor  function executing one batch of inputs
     * @param flushTimeoutMs milliseconds before a partial batch is flushed
     */
    public AiBatchBuffer(int maxBatchSize, Function<List<I>, List<O>> batchExecutor,
                         long flushTimeoutMs) {
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be >= 1");
        }
        if (flushTimeoutMs <= 0) {
            throw new IllegalArgumentException("flushTimeoutMs must be positive");
        }
        this.maxBatchSize = maxBatchSize;
        this.flushTimeoutMs = flushTimeoutMs;
        this.batchExecutor = batchExecutor;
    }

    /**
     * Submits one input and blocks until the batch containing it completes.
     *
     * @param input the row input to submit
     * @return the computed output for the input
     */
    public O submit(I input) {
        PendingItem<I, O> item = new PendingItem<>(input);
        queue.add(item);
        int count = pendingCount.incrementAndGet();
        if (count >= maxBatchSize) {
            drainAndExecute();
        }
        try {
            return item.future.get(flushTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Partial batch: flush now so the last rows of a query are not stranded.
            drainAndExecute();
            return item.future.join();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException("Batch execution failed", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for batch result", e);
        }
    }

    private void drainAndExecute() {
        List<PendingItem<I, O>> batch = new ArrayList<>(maxBatchSize);
        PendingItem<I, O> item;
        while (batch.size() < maxBatchSize && (item = queue.poll()) != null) {
            batch.add(item);
            pendingCount.decrementAndGet();
        }
        if (batch.isEmpty()) {
            return;
        }
        try {
            List<I> inputs = batch.stream().map(p -> p.input).toList();
            List<O> outputs = batchExecutor.apply(inputs);
            for (int i = 0; i < batch.size(); i++) {
                if (i < outputs.size()) {
                    batch.get(i).future.complete(outputs.get(i));
                } else {
                    batch.get(i).future.completeExceptionally(new IllegalStateException(
                            "Batch executor returned fewer results than inputs: expected "
                                    + batch.size() + ", got " + outputs.size()));
                }
            }
        } catch (Exception e) {
            for (PendingItem<I, O> p : batch) {
                p.future.completeExceptionally(e);
            }
        }
    }

    public void flush() {
        drainAndExecute();
    }

    private static final class PendingItem<I, O> {
        final I input;
        final CompletableFuture<O> future = new CompletableFuture<>();

        PendingItem(I input) {
            this.input = input;
        }
    }
}
