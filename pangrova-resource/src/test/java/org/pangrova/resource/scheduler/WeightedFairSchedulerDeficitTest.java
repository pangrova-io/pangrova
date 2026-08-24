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
package org.pangrova.resource.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.pangrova.resource.QueueType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies P2-5: {@link WeightedFairScheduler} performs true deficit-weighted
 * scheduling — the accumulated {@code cpuTimeNanos} now drives dispatch selection
 * (normalized as {@code cpuTimeNanos / weight}) instead of being recorded and
 * ignored. Tests the pure selection function deterministically (no timing).
 *
 * <p>Weights: INTERACTIVE=3.0, BATCH=1.0, BACKGROUND=0.5.</p>
 */
class WeightedFairSchedulerDeficitTest {

    @Test
    @DisplayName("selects the queue furthest behind its fair share (normalized by weight)")
    void selectsUnderservedByNormalizedServiceNotAbsoluteCpu() {
        // INTERACTIVE consumed MORE absolute CPU (150 vs 100) but, per its 3x weight,
        // is under-served: 150/3 = 50 < 100/1 = 100. It must be selected.
        Map<QueueType, Long> cpu = Map.of(
                QueueType.INTERACTIVE, 150L,
                QueueType.BATCH, 100L);
        QueueType selected = WeightedFairScheduler.selectUnderservedQueue(
                Set.of(QueueType.INTERACTIVE, QueueType.BATCH), cpu);
        assertThat(selected).isEqualTo(QueueType.INTERACTIVE);
    }

    @Test
    @DisplayName("deprioritizes a queue that has over-consumed relative to its weight")
    void deprioritizesOverServedQueue() {
        // INTERACTIVE: 600/3 = 200; BATCH: 50/1 = 50 -> BATCH is behind, gets served.
        Map<QueueType, Long> cpu = Map.of(
                QueueType.INTERACTIVE, 600L,
                QueueType.BATCH, 50L);
        QueueType selected = WeightedFairScheduler.selectUnderservedQueue(
                Set.of(QueueType.INTERACTIVE, QueueType.BATCH), cpu);
        assertThat(selected).isEqualTo(QueueType.BATCH);
    }

    @Test
    @DisplayName("idle queues tie-break toward the higher-weight queue")
    void idleTieBreaksTowardHigherWeight() {
        QueueType selected = WeightedFairScheduler.selectUnderservedQueue(
                Set.of(QueueType.INTERACTIVE, QueueType.BATCH, QueueType.BACKGROUND),
                Map.of());
        assertThat(selected).isEqualTo(QueueType.INTERACTIVE);
    }

    @Test
    @DisplayName("empty active set returns null")
    void emptyActiveReturnsNull() {
        assertThat(WeightedFairScheduler.selectUnderservedQueue(Set.of(), Map.of())).isNull();
    }

    @Test
    @DisplayName("a single active queue is always selected regardless of its CPU")
    void singleActiveQueueAlwaysSelected() {
        QueueType selected = WeightedFairScheduler.selectUnderservedQueue(
                Set.of(QueueType.BACKGROUND), Map.of(QueueType.BACKGROUND, 1_000_000L));
        assertThat(selected).isEqualTo(QueueType.BACKGROUND);
    }
}
