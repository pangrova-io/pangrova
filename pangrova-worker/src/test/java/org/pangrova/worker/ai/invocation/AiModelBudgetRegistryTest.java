package org.pangrova.worker.ai.invocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.AiInvocationException;

class AiModelBudgetRegistryTest {

    @Test
    void sharesExactDeploymentSemaphoreAndRetiresItAfterLastLease() {
        AiModelBudgetRegistry registry = new AiModelBudgetRegistry();
        var first = registry.retain("model", "v1", "sha", "text", 2);
        var second = registry.retain("model", "v1", "sha", "text", 2);
        try {
            assertThat(second.concurrency()).isSameAs(first.concurrency());
            assertThat(registry.size()).isEqualTo(1);
            assertThatThrownBy(() -> registry.retain(
                    "model", "v1", "sha", "text", 3))
                    .isInstanceOf(AiInvocationException.class)
                    .extracting("category")
                    .isEqualTo(AiInvocationException.Category.CAPABILITY_MISMATCH);
            first.close();
            assertThat(registry.size()).isEqualTo(1);
        } finally {
            first.close();
            second.close();
        }
        assertThat(registry.size()).isZero();
    }

    @Test
    void concurrentRetainsNeverCreateIndependentDeploymentSemaphores() throws Exception {
        AiModelBudgetRegistry registry = new AiModelBudgetRegistry();
        try (var anchor = registry.retain("model", "v1", "sha", "text", 1)) {
            var workers = Executors.newFixedThreadPool(8);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    futures.add(workers.submit(() -> {
                        start.await();
                        try (var lease = registry.retain(
                                "model", "v1", "sha", "text", 1)) {
                            return lease.concurrency() == anchor.concurrency();
                        }
                    }));
                }
                start.countDown();
                for (Future<Boolean> future : futures) {
                    assertThat(future.get(10, TimeUnit.SECONDS)).isTrue();
                }
                assertThat(registry.size()).isEqualTo(1);
            } finally {
                workers.shutdownNow();
                assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
        assertThat(registry.size()).isZero();
    }
}
