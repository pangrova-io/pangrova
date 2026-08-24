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
package org.pangrova.testing.e2e.ai;

import org.pangrova.coordinator.ai.vector.GlobalTopKMerger;
import org.pangrova.coordinator.ai.vector.GlobalTopKMerger.MergeResult;
import org.pangrova.coordinator.ai.vector.GlobalTopKMerger.ScoredRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for the GlobalTopKMerger — distributed vector search result merging.
 *
 * <p>Validates the coordinator-side global Top-K merge from multiple workers:
 * <ul>
 *   <li>Multi-worker merge produces correct global Top-K (ascending distance)</li>
 *   <li>Deduplication across workers (same filePath:rowId)</li>
 *   <li>Edge cases: empty workers, single worker, all-empty results</li>
 *   <li>Large-scale merge (many workers, many rows)</li>
 *   <li>Input validation (topK, null checks)</li>
 * </ul>
 */
@DisplayName("Distributed Vector Search E2E — GlobalTopKMerger")
class GlobalTopKMergerE2ETest {

    // -----------------------------------------------------------------------
    // Basic Multi-Worker Merge
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multi-Worker Merge")
    class MultiWorkerMerge {

        @Test
        @DisplayName("Two workers' results are merged into correct global Top-K")
        void twoWorkersMergeCorrectly() {
            List<ScoredRow> worker1 = List.of(
                    new ScoredRow("w1", 1, 0.1, "/data/w1/file1.parquet"),
                    new ScoredRow("w1", 2, 0.3, "/data/w1/file1.parquet"),
                    new ScoredRow("w1", 3, 0.5, "/data/w1/file2.parquet")
            );
            List<ScoredRow> worker2 = List.of(
                    new ScoredRow("w2", 1, 0.2, "/data/w2/file1.parquet"),
                    new ScoredRow("w2", 2, 0.4, "/data/w2/file1.parquet"),
                    new ScoredRow("w2", 3, 0.6, "/data/w2/file2.parquet")
            );

            MergeResult result = GlobalTopKMerger.merge(List.of(worker1, worker2), 4);

            assertThat(result.rows()).hasSize(4);
            assertThat(result.workerCount()).isEqualTo(2);
            assertThat(result.totalScanned()).isEqualTo(6);
            // Rows should be sorted by ascending distance
            for (int i = 0; i < result.rows().size() - 1; i++) {
                assertThat(result.rows().get(i).distance())
                        .isLessThanOrEqualTo(result.rows().get(i + 1).distance());
            }
            // Top 4 distances should be: 0.1, 0.2, 0.3, 0.4
            assertThat(result.rows().stream().mapToDouble(ScoredRow::distance).toArray())
                    .containsExactly(0.1, 0.2, 0.3, 0.4);
        }

        @Test
        @DisplayName("Three workers merge preserves global ordering")
        void threeWorkersMerge() {
            List<ScoredRow> w1 = List.of(
                    new ScoredRow("w1", 10, 0.05, "/w1/f.pq"),
                    new ScoredRow("w1", 20, 0.50, "/w1/f.pq")
            );
            List<ScoredRow> w2 = List.of(
                    new ScoredRow("w2", 10, 0.15, "/w2/f.pq"),
                    new ScoredRow("w2", 20, 0.45, "/w2/f.pq")
            );
            List<ScoredRow> w3 = List.of(
                    new ScoredRow("w3", 10, 0.25, "/w3/f.pq"),
                    new ScoredRow("w3", 20, 0.35, "/w3/f.pq")
            );

            MergeResult result = GlobalTopKMerger.merge(List.of(w1, w2, w3), 5);

            assertThat(result.rows()).hasSize(5);
            assertThat(result.workerCount()).isEqualTo(3);
            // Ascending distance
            double[] distances = result.rows().stream().mapToDouble(ScoredRow::distance).toArray();
            assertThat(distances).isSorted();
            assertThat(distances[0]).isEqualTo(0.05);
        }

        @Test
        @DisplayName("topK larger than total rows returns all rows sorted")
        void topKLargerThanTotalRows() {
            List<ScoredRow> w1 = List.of(
                    new ScoredRow("w1", 1, 0.3, "/w1/f.pq"),
                    new ScoredRow("w1", 2, 0.1, "/w1/f.pq")
            );

            MergeResult result = GlobalTopKMerger.merge(List.of(w1), 100);

            assertThat(result.rows()).hasSize(2);
            assertThat(result.rows().get(0).distance()).isEqualTo(0.1);
            assertThat(result.rows().get(1).distance()).isEqualTo(0.3);
        }

        @Test
        @DisplayName("topK=1 returns only the closest row globally")
        void topKOneReturnsClosest() {
            List<ScoredRow> w1 = List.of(new ScoredRow("w1", 1, 0.5, "/w1/f.pq"));
            List<ScoredRow> w2 = List.of(new ScoredRow("w2", 1, 0.2, "/w2/f.pq"));
            List<ScoredRow> w3 = List.of(new ScoredRow("w3", 1, 0.8, "/w3/f.pq"));

            MergeResult result = GlobalTopKMerger.merge(List.of(w1, w2, w3), 1);

            assertThat(result.rows()).hasSize(1);
            assertThat(result.rows().get(0).distance()).isEqualTo(0.2);
            assertThat(result.rows().get(0).workerId()).isEqualTo("w2");
        }
    }

    // -----------------------------------------------------------------------
    // Deduplication
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Deduplication Across Workers")
    class Deduplication {

        @Test
        @DisplayName("Duplicate rows (same filePath:rowId) are deduplicated")
        void duplicateRowsAreDeduplicated() {
            // Same file/row appears on two workers (e.g., replicated data)
            List<ScoredRow> w1 = List.of(
                    new ScoredRow("w1", 100, 0.1, "/shared/file1.pq"),
                    new ScoredRow("w1", 200, 0.3, "/shared/file1.pq")
            );
            List<ScoredRow> w2 = List.of(
                    new ScoredRow("w2", 100, 0.15, "/shared/file1.pq"),  // duplicate of w1 row 100
                    new ScoredRow("w2", 300, 0.2, "/shared/file2.pq")
            );

            MergeResult result = GlobalTopKMerger.merge(List.of(w1, w2), 10);

            // row 100 in /shared/file1.pq should appear only once (first seen wins)
            long uniqueCount = result.rows().stream()
                    .map(r -> r.filePath() + ":" + r.rowId())
                    .distinct()
                    .count();
            assertThat(uniqueCount).isEqualTo(result.rows().size());
            assertThat(result.rows()).hasSize(3); // 100, 200, 300 (deduplicated)
        }

        @Test
        @DisplayName("Same rowId in different files are NOT deduplicated")
        void sameRowIdDifferentFilesKeptSeparate() {
            List<ScoredRow> w1 = List.of(
                    new ScoredRow("w1", 1, 0.1, "/file_a.pq")
            );
            List<ScoredRow> w2 = List.of(
                    new ScoredRow("w2", 1, 0.2, "/file_b.pq")  // same rowId, different file
            );

            MergeResult result = GlobalTopKMerger.merge(List.of(w1, w2), 10);

            assertThat(result.rows()).hasSize(2);
        }
    }

    // -----------------------------------------------------------------------
    // Edge Cases
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Empty worker results are handled gracefully")
        void emptyWorkerResults() {
            List<ScoredRow> w1 = List.of(new ScoredRow("w1", 1, 0.5, "/f.pq"));
            List<ScoredRow> w2 = List.of();  // empty
            List<ScoredRow> w3 = List.of(new ScoredRow("w3", 2, 0.3, "/f.pq"));

            MergeResult result = GlobalTopKMerger.merge(List.of(w1, w2, w3), 5);

            assertThat(result.rows()).hasSize(2);
            assertThat(result.workerCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("All workers empty returns empty result")
        void allWorkersEmpty() {
            MergeResult result = GlobalTopKMerger.merge(
                    List.of(List.of(), List.of(), List.of()), 5);

            assertThat(result.rows()).isEmpty();
            assertThat(result.totalScanned()).isEqualTo(0);
            assertThat(result.workerCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("Single worker returns its results sorted")
        void singleWorkerSorted() {
            List<ScoredRow> w1 = List.of(
                    new ScoredRow("w1", 3, 0.9, "/f.pq"),
                    new ScoredRow("w1", 1, 0.1, "/f.pq"),
                    new ScoredRow("w1", 2, 0.5, "/f.pq")
            );

            MergeResult result = GlobalTopKMerger.merge(List.of(w1), 3);

            assertThat(result.rows()).hasSize(3);
            double[] distances = result.rows().stream().mapToDouble(ScoredRow::distance).toArray();
            assertThat(distances).isSorted();
        }

        @Test
        @DisplayName("Empty worker list returns empty result")
        void emptyWorkerList() {
            MergeResult result = GlobalTopKMerger.merge(List.of(), 5);

            assertThat(result.rows()).isEmpty();
            assertThat(result.workerCount()).isEqualTo(0);
        }
    }

    // -----------------------------------------------------------------------
    // Large Scale
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Large Scale Merge")
    class LargeScale {

        @Test
        @DisplayName("10 workers × 100 rows merges correctly into Top-20")
        void tenWorkersHundredRows() {
            List<List<ScoredRow>> workerResults = new ArrayList<>();
            Set<Double> allDistances = new HashSet<>();

            for (int w = 0; w < 10; w++) {
                List<ScoredRow> rows = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    double distance = (w * 100.0 + r) / 1000.0;
                    allDistances.add(distance);
                    rows.add(new ScoredRow("w" + w, r, distance,
                            "/data/w" + w + "/file" + (r / 10) + ".pq"));
                }
                workerResults.add(rows);
            }

            MergeResult result = GlobalTopKMerger.merge(workerResults, 20);

            assertThat(result.rows()).hasSize(20);
            assertThat(result.totalScanned()).isEqualTo(1000);
            assertThat(result.workerCount()).isEqualTo(10);

            // Verify sorted ascending
            double[] distances = result.rows().stream().mapToDouble(ScoredRow::distance).toArray();
            assertThat(distances).isSorted();

            // Top-20 should be the 20 smallest distances (from worker 0)
            assertThat(distances[0]).isEqualTo(0.0);
            assertThat(distances[19]).isLessThan(0.02);
        }

        @Test
        @DisplayName("Merge with many duplicates across workers")
        void manyDuplicates() {
            List<List<ScoredRow>> workerResults = new ArrayList<>();

            // 5 workers all report the same 10 rows (replicated data scenario)
            for (int w = 0; w < 5; w++) {
                List<ScoredRow> rows = new ArrayList<>();
                for (int r = 0; r < 10; r++) {
                    rows.add(new ScoredRow("w" + w, r, r * 0.1,
                            "/shared/data.pq"));
                }
                workerResults.add(rows);
            }

            MergeResult result = GlobalTopKMerger.merge(workerResults, 10);

            // After dedup, only 10 unique rows
            assertThat(result.rows()).hasSize(10);
            assertThat(result.totalScanned()).isEqualTo(50); // scanned 50, kept 10
        }
    }

    // -----------------------------------------------------------------------
    // Input Validation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Input Validation")
    class InputValidation {

        @Test
        @DisplayName("topK=0 throws IllegalArgumentException")
        void topKZeroThrows() {
            assertThatThrownBy(() -> GlobalTopKMerger.merge(List.of(), 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("positive");
        }

        @Test
        @DisplayName("Negative topK throws IllegalArgumentException")
        void negativeTopKThrows() {
            assertThatThrownBy(() -> GlobalTopKMerger.merge(List.of(), -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Null workerResults throws NullPointerException")
        void nullWorkerResultsThrows() {
            assertThatThrownBy(() -> GlobalTopKMerger.merge(null, 5))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("ScoredRow requires non-null workerId")
        void scoredRowRequiresWorkerId() {
            assertThatThrownBy(() -> new ScoredRow(null, 1, 0.5, "/f.pq"))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("ScoredRow requires non-null filePath")
        void scoredRowRequiresFilePath() {
            assertThatThrownBy(() -> new ScoredRow("w1", 1, 0.5, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    // -----------------------------------------------------------------------
    // MergeResult Immutability
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("MergeResult Immutability")
    class MergeResultImmutability {

        @Test
        @DisplayName("MergeResult rows list is immutable")
        void rowsListIsImmutable() {
            List<ScoredRow> w1 = List.of(new ScoredRow("w1", 1, 0.5, "/f.pq"));
            MergeResult result = GlobalTopKMerger.merge(List.of(w1), 5);

            assertThatThrownBy(() -> result.rows().add(new ScoredRow("w2", 2, 0.1, "/g.pq")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
