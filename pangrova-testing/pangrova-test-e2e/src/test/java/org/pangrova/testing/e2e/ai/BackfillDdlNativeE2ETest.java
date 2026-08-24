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

import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillRequest;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillResult;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillState;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillStatus;
import org.pangrova.coordinator.ai.embedding.BackfillExecutionException;
import org.pangrova.coordinator.ai.vector.GlobalTopKMerger;
import org.pangrova.streaming.cdc.StreamEmbeddingInterceptor;
import org.pangrova.streaming.cdc.StreamEmbeddingInterceptor.EmbeddingColumnConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for BACKFILL DDL structures and AI Native Component contracts.
 *
 * <p>Validates:
 * <ul>
 *   <li>BackfillRequest/Result/Status record contracts and validation</li>
 *   <li>BackfillState enum lifecycle completeness</li>
 *   <li>BackfillExecutionException context propagation</li>
 *   <li>Cross-component integration: CDC → Backfill → Vector Search pipeline readiness</li>
 *   <li>Native component boundary types (ScoredRow, MergeResult immutability)</li>
 * </ul>
 */
@DisplayName("BACKFILL DDL + Native Component Integration E2E")
class BackfillDdlNativeE2ETest {

    // -----------------------------------------------------------------------
    // BackfillRequest Validation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BackfillRequest Record Validation")
    class BackfillRequestValidation {

        @Test
        @DisplayName("Valid BackfillRequest is constructed correctly")
        void validRequestConstructed() {
            BackfillRequest req = new BackfillRequest(
                    "db.schema.articles", "content_embedding", "text-embedding-3-small",
                    "content_embedding IS NULL", 500, 4);

            assertThat(req.tableName()).isEqualTo("db.schema.articles");
            assertThat(req.columnName()).isEqualTo("content_embedding");
            assertThat(req.modelName()).isEqualTo("text-embedding-3-small");
            assertThat(req.filter()).isEqualTo("content_embedding IS NULL");
            assertThat(req.batchSize()).isEqualTo(500);
            assertThat(req.concurrency()).isEqualTo(4);
        }

        @Test
        @DisplayName("Null tableName throws IllegalArgumentException")
        void nullTableNameThrows() {
            assertThatThrownBy(() -> new BackfillRequest(null, "col", "model", null, 100, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tableName");
        }

        @Test
        @DisplayName("Blank tableName throws IllegalArgumentException")
        void blankTableNameThrows() {
            assertThatThrownBy(() -> new BackfillRequest("  ", "col", "model", null, 100, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tableName");
        }

        @Test
        @DisplayName("Null columnName throws IllegalArgumentException")
        void nullColumnNameThrows() {
            assertThatThrownBy(() -> new BackfillRequest("table", null, "model", null, 100, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("columnName");
        }

        @Test
        @DisplayName("Null modelName throws IllegalArgumentException")
        void nullModelNameThrows() {
            assertThatThrownBy(() -> new BackfillRequest("table", "col", null, null, 100, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("modelName");
        }

        @Test
        @DisplayName("Zero batchSize throws IllegalArgumentException")
        void zeroBatchSizeThrows() {
            assertThatThrownBy(() -> new BackfillRequest("table", "col", "model", null, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchSize");
        }

        @Test
        @DisplayName("Negative concurrency throws IllegalArgumentException")
        void negativeConcurrencyThrows() {
            assertThatThrownBy(() -> new BackfillRequest("table", "col", "model", null, 100, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("concurrency");
        }

        @Test
        @DisplayName("Null filter is allowed (means all NULL rows)")
        void nullFilterAllowed() {
            BackfillRequest req = new BackfillRequest("t", "c", "m", null, 100, 1);
            assertThat(req.filter()).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // BackfillResult Record
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BackfillResult Record")
    class BackfillResultTests {

        @Test
        @DisplayName("BackfillResult captures all metrics correctly")
        void resultCapturesMetrics() {
            BackfillResult result = new BackfillResult(
                    "db.articles", "embedding_col",
                    10000, 9950, 50,
                    Duration.ofMinutes(5), false, 12345L);

            assertThat(result.tableName()).isEqualTo("db.articles");
            assertThat(result.columnName()).isEqualTo("embedding_col");
            assertThat(result.totalRows()).isEqualTo(10000);
            assertThat(result.processedRows()).isEqualTo(9950);
            assertThat(result.failedRows()).isEqualTo(50);
            assertThat(result.elapsedTime()).isEqualTo(Duration.ofMinutes(5));
            assertThat(result.cancelled()).isFalse();
            assertThat(result.snapshotId()).isEqualTo(12345L);
        }

        @Test
        @DisplayName("Cancelled backfill result has cancelled=true")
        void cancelledResult() {
            BackfillResult result = new BackfillResult(
                    "t", "c", 5000, 2000, 0,
                    Duration.ofSeconds(30), true, 99L);

            assertThat(result.cancelled()).isTrue();
            assertThat(result.processedRows()).isLessThan(result.totalRows());
        }
    }

    // -----------------------------------------------------------------------
    // BackfillStatus Record
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BackfillStatus Record")
    class BackfillStatusTests {

        @Test
        @DisplayName("BackfillStatus correctly reports running state")
        void statusReportsRunningState() {
            BackfillStatus status = new BackfillStatus(
                    "db.articles", "content_vec",
                    BackfillState.RUNNING,
                    10000, 3000, 10,
                    Instant.now().minusSeconds(60),
                    "checkpoint-42", 555L);

            assertThat(status.state()).isEqualTo(BackfillState.RUNNING);
            assertThat(status.processedRows()).isEqualTo(3000);
            assertThat(status.lastCheckpoint()).isEqualTo("checkpoint-42");
        }
    }

    // -----------------------------------------------------------------------
    // BackfillState Enum
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BackfillState Lifecycle")
    class BackfillStateLifecycle {

        @Test
        @DisplayName("All 5 lifecycle states are defined")
        void allStatesExist() {
            BackfillState[] states = BackfillState.values();
            assertThat(states).hasSize(5);
            assertThat(states).containsExactlyInAnyOrder(
                    BackfillState.SCANNING,
                    BackfillState.RUNNING,
                    BackfillState.CANCELLING,
                    BackfillState.COMPLETED,
                    BackfillState.FAILED
            );
        }

        @Test
        @DisplayName("State enum values are resolvable by name")
        void statesByName() {
            assertThat(BackfillState.valueOf("SCANNING")).isEqualTo(BackfillState.SCANNING);
            assertThat(BackfillState.valueOf("RUNNING")).isEqualTo(BackfillState.RUNNING);
            assertThat(BackfillState.valueOf("COMPLETED")).isEqualTo(BackfillState.COMPLETED);
        }
    }

    // -----------------------------------------------------------------------
    // BackfillExecutionException
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BackfillExecutionException Context")
    class BackfillExceptionContext {

        @Test
        @DisplayName("Exception carries table and column context")
        void exceptionCarriesContext() {
            BackfillExecutionException ex = new BackfillExecutionException(
                    "db.articles", "embedding", "model endpoint timeout",
                    new RuntimeException("connection refused"));

            assertThat(ex.getMessage())
                    .contains("db.articles")
                    .contains("embedding")
                    .contains("model endpoint timeout");
            assertThat(ex.getCause())
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("connection refused");
        }
    }

    // -----------------------------------------------------------------------
    // Cross-Component Pipeline Readiness
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("CDC → Backfill → Vector Search Pipeline")
    class CrossComponentPipeline {

        @Test
        @DisplayName("CDC interceptor config aligns with BackfillRequest model fields")
        void cdcConfigAlignsWithBackfill() {
            // CDC uses model name to trigger embedding
            EmbeddingColumnConfig cdcConfig = new EmbeddingColumnConfig(
                    "content", "text-embedding-3-small", "content_vec");

            // Backfill uses same model name for batch embedding
            BackfillRequest backfillReq = new BackfillRequest(
                    "db.articles", "content_vec", "text-embedding-3-small",
                    null, 1000, 4);

            // Both reference same model
            assertThat(cdcConfig.modelName()).isEqualTo(backfillReq.modelName());
            // CDC vectorColumn matches backfill columnName
            assertThat(cdcConfig.vectorColumn()).isEqualTo(backfillReq.columnName());
        }

        @Test
        @DisplayName("Embedding pipeline → Vector Search pipeline data types are compatible")
        void embeddingToVectorSearchCompatible() {
            // EmbeddingWorkRequest produces file paths
            StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                    Map.of("db.t", new EmbeddingColumnConfig("text", "model", "vec")),
                    req -> {
                        // After embedding, files contain vector data searchable by GlobalTopKMerger
                        assertThat(req.filePaths()).isNotEmpty();
                        // Each file path can be referenced in ScoredRow
                        for (String filePath : req.filePaths()) {
                            GlobalTopKMerger.ScoredRow row =
                                    new GlobalTopKMerger.ScoredRow("w1", 0, 0.1, filePath);
                            assertThat(row.filePath()).isEqualTo(filePath);
                        }
                    });

            interceptor.onCommit("db.t", java.util.List.of("/data/part-0001.parquet"));
        }
    }

    // -----------------------------------------------------------------------
    // Native Component Boundary Types
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Native Component Boundary Types")
    class NativeComponentBoundaryTypes {

        @Test
        @DisplayName("ScoredRow record fields are accessible and valid")
        void scoredRowFields() {
            GlobalTopKMerger.ScoredRow row = new GlobalTopKMerger.ScoredRow(
                    "worker-1", 42, 0.123, "/data/vectors/chunk-00.lance");

            assertThat(row.workerId()).isEqualTo("worker-1");
            assertThat(row.rowId()).isEqualTo(42);
            assertThat(row.distance()).isEqualTo(0.123);
            assertThat(row.filePath()).isEqualTo("/data/vectors/chunk-00.lance");
        }

        @Test
        @DisplayName("MergeResult.rows() is sorted ascending by distance")
        void mergeResultIsSorted() {
            var w1 = java.util.List.of(
                    new GlobalTopKMerger.ScoredRow("w1", 1, 0.9, "/f.pq"),
                    new GlobalTopKMerger.ScoredRow("w1", 2, 0.1, "/f.pq"),
                    new GlobalTopKMerger.ScoredRow("w1", 3, 0.5, "/f.pq")
            );

            GlobalTopKMerger.MergeResult result = GlobalTopKMerger.merge(java.util.List.of(w1), 3);

            double prev = -1;
            for (GlobalTopKMerger.ScoredRow row : result.rows()) {
                assertThat(row.distance()).isGreaterThanOrEqualTo(prev);
                prev = row.distance();
            }
        }

        @Test
        @DisplayName("EmbeddingWorkRequest fields form complete embedding task descriptor")
        void embeddingWorkRequestIsComplete() {
            var config = new EmbeddingColumnConfig("body", "bge-base", "body_vec");
            StreamEmbeddingInterceptor interceptor = new StreamEmbeddingInterceptor(
                    Map.of("ns.docs", config),
                    req -> {
                        assertThat(req.tableName()).isEqualTo("ns.docs");
                        assertThat(req.textColumn()).isEqualTo("body");
                        assertThat(req.modelName()).isEqualTo("bge-base");
                        assertThat(req.vectorColumn()).isEqualTo("body_vec");
                        assertThat(req.filePaths()).containsExactly("/f1.pq", "/f2.pq");
                    });

            interceptor.onCommit("ns.docs", java.util.List.of("/f1.pq", "/f2.pq"));
        }
    }
}
