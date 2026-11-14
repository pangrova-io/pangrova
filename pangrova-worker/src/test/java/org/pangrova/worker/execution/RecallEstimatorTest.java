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
package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RecallEstimatorTest {

    @Test
    void perfectRecall() {
        List<Long> ann = List.of(1L, 2L, 3L);
        List<Long> truth = List.of(1L, 2L, 3L);
        assertEquals(1.0, RecallEstimator.recallAtK(ann, truth, 3), 1e-9);
    }

    @Test
    void partialRecall() {
        List<Long> ann = List.of(1L, 5L, 3L);
        List<Long> truth = List.of(1L, 2L, 3L);
        assertEquals(2.0 / 3.0, RecallEstimator.recallAtK(ann, truth, 3), 1e-9);
    }

    @Test
    void zeroRecall() {
        List<Long> ann = List.of(10L, 20L, 30L);
        List<Long> truth = List.of(1L, 2L, 3L);
        assertEquals(0.0, RecallEstimator.recallAtK(ann, truth, 3), 1e-9);
    }

    @Test
    void emptyGroundTruth() {
        assertEquals(1.0, RecallEstimator.recallAtK(List.of(1L), List.of(), 3), 1e-9);
    }

    @Test
    void negativeKThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> RecallEstimator.recallAtK(List.of(1L), List.of(1L), -1));
    }

    @Test
    void zeroKThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> RecallEstimator.recallAtK(List.of(1L), List.of(1L), 0));
    }

    @Test
    void annResultsBeyondKCountTowardRecall() {
        // ANN returned 5 results (wider than k=3), but result at index 4 is in truth set.
        // Recall must count it even though it's beyond position k.
        List<Long> ann = List.of(10L, 20L, 30L, 40L, 3L);
        List<Long> truth = List.of(1L, 2L, 3L);
        double recall = RecallEstimator.recallAtK(ann, truth, 3);
        assertEquals(1.0 / 3.0, recall, 1e-9);
    }

    @Test
    void scoreSpreadWithVariation() {
        List<Double> distances = List.of(1.0, 2.0, 3.0);
        double spread = RecallEstimator.scoreSpread(distances);
        assertTrue(spread > 0.0);
        assertEquals(Math.sqrt(2.0 / 3.0), spread, 1e-9);
    }

    @Test
    void scoreSpreadSingleElement() {
        assertEquals(0.0, RecallEstimator.scoreSpread(List.of(1.0)));
    }

    @Test
    void scoreSpreadIdenticalValues() {
        assertEquals(0.0, RecallEstimator.scoreSpread(List.of(5.0, 5.0, 5.0)), 1e-9);
    }
}
