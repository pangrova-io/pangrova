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
package org.pangrova.coordinator.ai.layout;

import org.pangrova.planner.core.advisor.QueryPatternRecorder;
import org.pangrova.planner.core.advisor.QueryPatternRecorder.QueryPattern;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DataLayoutAdvisorTest {

    private static final String TABLE = "cat.schema.orders";

    @Test
    void constructorRejectsNullRecorder() {
        assertThrows(IllegalArgumentException.class, () -> new DataLayoutAdvisor(null));
    }

    @Test
    void returnsEmptyWhenInsufficientPatterns() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        // Record only 10 patterns — below MIN_SAMPLE_SIZE of 50
        for (int i = 0; i < 10; i++) {
            recorder.record(pattern(TABLE, Set.of("col_a"), Set.of(), false));
        }

        Optional<DataLayoutAdvisor.LayoutRecommendation> result = advisor.recommend(TABLE);
        assertTrue(result.isEmpty(), "Should return empty when patterns < MIN_SAMPLE_SIZE");
    }

    @Test
    void returnsEmptyForDifferentTable() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        // Record 50 patterns for a DIFFERENT table
        for (int i = 0; i < 50; i++) {
            recorder.record(pattern("cat.schema.other", Set.of("col_a"), Set.of(), false));
        }

        Optional<DataLayoutAdvisor.LayoutRecommendation> result = advisor.recommend(TABLE);
        assertTrue(result.isEmpty(), "Should return empty when no patterns for requested table");
    }

    @Test
    void recommendsTopFilterColumnAsPartitionKey() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        // col_a appears in 40 patterns, col_b in 10 — col_a should win
        for (int i = 0; i < 40; i++) {
            recorder.record(pattern(TABLE, Set.of("col_a"), Set.of(), false));
        }
        for (int i = 0; i < 10; i++) {
            recorder.record(pattern(TABLE, Set.of("col_b"), Set.of(), false));
        }

        DataLayoutAdvisor.LayoutRecommendation rec = advisor.recommend(TABLE).orElseThrow();
        assertEquals("col_a", rec.recommendedPartitionKey());
        assertEquals(TABLE, rec.tableName());
        assertEquals(50, rec.sampleSize());
    }

    @Test
    void recommendsTopSortColumnAsSortKey() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        // ts appears as sort column in 45 patterns, id in 5
        for (int i = 0; i < 45; i++) {
            recorder.record(pattern(TABLE, Set.of("region"), Set.of("ts"), false));
        }
        for (int i = 0; i < 5; i++) {
            recorder.record(pattern(TABLE, Set.of("region"), Set.of("id"), false));
        }

        DataLayoutAdvisor.LayoutRecommendation rec = advisor.recommend(TABLE).orElseThrow();
        assertEquals("ts", rec.recommendedSortKey());
    }

    @Test
    void writeRatioComputedCorrectly() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        // 20 writes, 30 reads = 60 total
        for (int i = 0; i < 20; i++) {
            recorder.record(pattern(TABLE, Set.of("col_a"), Set.of(), true));
        }
        for (int i = 0; i < 30; i++) {
            recorder.record(pattern(TABLE, Set.of("col_a"), Set.of(), false));
        }

        DataLayoutAdvisor.LayoutRecommendation rec = advisor.recommend(TABLE).orElseThrow();
        assertEquals(20.0 / 50.0, rec.writeRatio(), 1e-9);
    }

    @Test
    void filterColumnFrequencyMapIsImmutable() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        for (int i = 0; i < 50; i++) {
            recorder.record(pattern(TABLE, Set.of("col_a"), Set.of(), false));
        }

        DataLayoutAdvisor.LayoutRecommendation rec = advisor.recommend(TABLE).orElseThrow();
        assertThrows(UnsupportedOperationException.class,
                () -> rec.filterColumnFrequency().put("injected", 999));
    }

    @Test
    void nullPartitionKeyWhenNoFilterColumns() {
        QueryPatternRecorder recorder = new QueryPatternRecorder(1000);
        DataLayoutAdvisor advisor = new DataLayoutAdvisor(recorder);

        // No filter columns — partition key should be null
        for (int i = 0; i < 50; i++) {
            recorder.record(pattern(TABLE, Set.of(), Set.of("ts"), false));
        }

        DataLayoutAdvisor.LayoutRecommendation rec = advisor.recommend(TABLE).orElseThrow();
        assertNull(rec.recommendedPartitionKey());
        assertNotNull(rec.recommendedSortKey());
    }

    private static QueryPattern pattern(String table, Set<String> filters,
                                        Set<String> sorts, boolean isWrite) {
        return new QueryPattern(table, filters, Set.of(), sorts, Set.of(), isWrite, Instant.now());
    }
}
