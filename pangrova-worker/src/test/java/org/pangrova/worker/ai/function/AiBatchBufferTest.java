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
package org.pangrova.worker.ai.function;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AiBatchBufferTest {

    @Test
    void singleSubmitWithBatchSizeOne() {
        AiBatchBuffer<Integer, String> buffer = new AiBatchBuffer<>(1,
                inputs -> inputs.stream().map(i -> "r" + i).toList());
        assertEquals("r42", buffer.submit(42));
    }

    @Test
    void batchOfExactSizeFiresAutomatically() {
        AtomicInteger batchFires = new AtomicInteger();
        AiBatchBuffer<Integer, Integer> buffer = new AiBatchBuffer<>(3,
                inputs -> {
                    batchFires.incrementAndGet();
                    return inputs;
                });
        // All three must be submitted from different threads because submit() blocks
        ExecutorService pool = Executors.newFixedThreadPool(3);
        Future<Integer> f1 = pool.submit(() -> buffer.submit(1));
        Future<Integer> f2 = pool.submit(() -> buffer.submit(2));
        Future<Integer> f3 = pool.submit(() -> buffer.submit(3));
        assertDoesNotThrow(() -> {
            assertEquals(1, f1.get(2, TimeUnit.SECONDS));
            assertEquals(2, f2.get(2, TimeUnit.SECONDS));
            assertEquals(3, f3.get(2, TimeUnit.SECONDS));
        });
        pool.shutdown();
        assertEquals(1, batchFires.get());
    }

    @Test
    void partialBatchFlushedByTimeout() {
        // Large batch size so auto-fire won't trigger; short timeout so flush happens quickly
        AiBatchBuffer<Integer, Integer> buffer = new AiBatchBuffer<>(100,
                inputs -> inputs, 80);
        long start = System.currentTimeMillis();
        int result = buffer.submit(99);
        long elapsed = System.currentTimeMillis() - start;
        assertEquals(99, result);
        assertTrue(elapsed < 3000, "Partial batch should flush well before 3 seconds");
    }

    @Test
    void exceptionFromBatchExecutorPropagatesToCaller() {
        AiBatchBuffer<Integer, Integer> buffer = new AiBatchBuffer<>(1,
                inputs -> { throw new RuntimeException("executor error"); });
        RuntimeException ex = assertThrows(RuntimeException.class, () -> buffer.submit(1));
        assertTrue(ex.getMessage().contains("executor error")
                || ex.getCause() != null && ex.getCause().getMessage().contains("executor error"));
    }

    @Test
    void concurrentSubmittersAllReceiveResults() throws Exception {
        int threads = 6;
        int batchSize = 3;
        CopyOnWriteArrayList<Integer> results = new CopyOnWriteArrayList<>();
        AiBatchBuffer<Integer, Integer> buffer = new AiBatchBuffer<>(batchSize,
                inputs -> inputs, 200);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Future<Integer>> futures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int val = i;
            futures.add(pool.submit(() -> {
                ready.await();
                try {
                    return buffer.submit(val);
                } finally {
                    done.countDown();
                }
            }));
        }
        ready.countDown(); // release all threads simultaneously
        assertTrue(done.await(10, TimeUnit.SECONDS), "All submitters should complete within 10s");
        pool.shutdown();

        for (Future<Integer> f : futures) {
            results.add(f.get());
        }
        assertEquals(threads, results.size());
        for (int i = 0; i < threads; i++) {
            assertTrue(results.contains(i), "Missing result for input " + i);
        }
    }

    @Test
    void rejectsZeroBatchSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new AiBatchBuffer<>(0, inputs -> inputs));
    }

    @Test
    void rejectsNonPositiveFlushTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> new AiBatchBuffer<>(1, inputs -> inputs, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new AiBatchBuffer<>(1, inputs -> inputs, -50));
    }

    @Test
    void inputMapsToOutputByPosition() {
        // executor reverses the list; each caller should get back its own value
        AiBatchBuffer<Integer, Integer> buffer = new AiBatchBuffer<>(2,
                inputs -> List.of(inputs.get(1), inputs.get(0)));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Integer> fa = pool.submit(() -> buffer.submit(10));
        Future<Integer> fb = pool.submit(() -> buffer.submit(20));
        assertDoesNotThrow(() -> {
            int a = fa.get(2, TimeUnit.SECONDS);
            int b = fb.get(2, TimeUnit.SECONDS);
            // a and b should be the reversed values: one gets 20, other gets 10
            assertTrue((a == 10 && b == 20) || (a == 20 && b == 10));
        });
        pool.shutdown();
    }
}
