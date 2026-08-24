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

import org.pangrova.streaming.cdc.StreamEmbeddingInterceptor;
import org.pangrova.streaming.cdc.StreamEmbeddingInterceptor.EmbeddingColumnConfig;
import org.pangrova.streaming.cdc.StreamEmbeddingInterceptor.EmbeddingWorkRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for the CDC → Embedding Pipeline.
 *
 * <p>Validates the full StreamEmbeddingInterceptor lifecycle:
 * <ul>
 *   <li>CDC commit events trigger embedding work requests</li>
 *   <li>Non-configured tables are silently skipped</li>
 *   <li>Batch file paths are correctly propagated</li>
 *   <li>Concurrent CDC commits are thread-safe</li>
 *   <li>Edge cases (null table, empty files) are handled gracefully</li>
 * </ul>
 */
@DisplayName("Embedding Pipeline E2E — CDC → Auto-Embedding → Batch Processing")
class EmbeddingPipelineE2ETest {

    private List<EmbeddingWorkRequest> capturedRequests;
    private StreamEmbeddingInterceptor interceptor;

    @BeforeEach
    void setUp() {
        capturedRequests = new CopyOnWriteArrayList<>();

        Map<String, EmbeddingColumnConfig> enabledTables = Map.of(
                "db.schema.articles", new EmbeddingColumnConfig("content", "text-embedding-ada-002", "content_vec"),
                "db.schema.products", new EmbeddingColumnConfig("description", "bge-small-en", "desc_embedding"),
                "db.schema.reviews", new EmbeddingColumnConfig("review_text", "multilingual-e5", "review_vec")
        );

        interceptor = new StreamEmbeddingInterceptor(enabledTables, capturedRequests::add);
    }

    // -----------------------------------------------------------------------
    // CDC Commit → Embedding Trigger
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("CDC Commit Triggers Embedding")
    class CdcCommitTrigger {

        @Test
        @DisplayName("Single file commit triggers embedding work request with correct fields")
        void singleFileCommitTriggersEmbedding() {
            List<String> files = List.of("/data/articles/part-00001.parquet");

            interceptor.onCommit("db.schema.articles", files);

            assertThat(capturedRequests).hasSize(1);
            EmbeddingWorkRequest req = capturedRequests.get(0);
            assertThat(req.tableName()).isEqualTo("db.schema.articles");
            assertThat(req.textColumn()).isEqualTo("content");
            assertThat(req.modelName()).isEqualTo("text-embedding-ada-002");
            assertThat(req.vectorColumn()).isEqualTo("content_vec");
            assertThat(req.filePaths()).containsExactly("/data/articles/part-00001.parquet");
        }

        @Test
        @DisplayName("Multi-file batch commit includes all file paths")
        void multiFileBatchCommit() {
            List<String> files = List.of(
                    "/data/products/part-00001.parquet",
                    "/data/products/part-00002.parquet",
                    "/data/products/part-00003.parquet",
                    "/data/products/part-00004.parquet",
                    "/data/products/part-00005.parquet"
            );

            interceptor.onCommit("db.schema.products", files);

            assertThat(capturedRequests).hasSize(1);
            EmbeddingWorkRequest req = capturedRequests.get(0);
            assertThat(req.tableName()).isEqualTo("db.schema.products");
            assertThat(req.textColumn()).isEqualTo("description");
            assertThat(req.modelName()).isEqualTo("bge-small-en");
            assertThat(req.vectorColumn()).isEqualTo("desc_embedding");
            assertThat(req.filePaths()).hasSize(5);
        }

        @Test
        @DisplayName("Multiple commits to same table generate separate requests")
        void multipleCommitsSameTable() {
            interceptor.onCommit("db.schema.reviews", List.of("/data/reviews/batch1.parquet"));
            interceptor.onCommit("db.schema.reviews", List.of("/data/reviews/batch2.parquet"));
            interceptor.onCommit("db.schema.reviews", List.of("/data/reviews/batch3.parquet"));

            assertThat(capturedRequests).hasSize(3);
            assertThat(capturedRequests).extracting(EmbeddingWorkRequest::tableName)
                    .containsOnly("db.schema.reviews");
            assertThat(capturedRequests).extracting(r -> r.filePaths().get(0))
                    .containsExactly(
                            "/data/reviews/batch1.parquet",
                            "/data/reviews/batch2.parquet",
                            "/data/reviews/batch3.parquet");
        }

        @Test
        @DisplayName("Commits to different tables generate separate requests per table")
        void commitsToDifferentTables() {
            interceptor.onCommit("db.schema.articles", List.of("/data/articles/a.parquet"));
            interceptor.onCommit("db.schema.products", List.of("/data/products/p.parquet"));
            interceptor.onCommit("db.schema.reviews", List.of("/data/reviews/r.parquet"));

            assertThat(capturedRequests).hasSize(3);
            assertThat(capturedRequests).extracting(EmbeddingWorkRequest::modelName)
                    .containsExactlyInAnyOrder("text-embedding-ada-002", "bge-small-en", "multilingual-e5");
        }
    }

    // -----------------------------------------------------------------------
    // Non-Configured Tables Skipped
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Non-Configured Tables Are Skipped")
    class NonConfiguredTablesSkipped {

        @Test
        @DisplayName("Commit to non-configured table produces no work request")
        void nonConfiguredTableSkipped() {
            interceptor.onCommit("db.schema.logs", List.of("/data/logs/part-00001.parquet"));

            assertThat(capturedRequests).isEmpty();
        }

        @Test
        @DisplayName("Similar table name (prefix match) does not falsely trigger")
        void similarTableNameNotTriggered() {
            interceptor.onCommit("db.schema.articles_archive", List.of("/data/archive/a.parquet"));

            assertThat(capturedRequests).isEmpty();
        }

        @Test
        @DisplayName("enabledTableNames returns configured table set")
        void enabledTableNamesReturnsConfiguredSet() {
            assertThat(interceptor.enabledTableNames())
                    .containsExactlyInAnyOrder("db.schema.articles", "db.schema.products", "db.schema.reviews");
        }
    }

    // -----------------------------------------------------------------------
    // Edge Cases and Null Safety
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Edge Cases and Null Safety")
    class EdgeCases {

        @Test
        @DisplayName("Null table name does not produce embedding request")
        void nullTableNameHandled() {
            // Depending on the Map implementation backing enabledTables:
            // - HashMap tolerates null key → returns silently
            // - Map.of() throws NPE on null key lookup
            // Either way, no embedding request should be enqueued.
            try {
                interceptor.onCommit(null, List.of("/data/file.parquet"));
            } catch (NullPointerException ignored) {
                // Expected for Map.of() backed implementations
            }
            assertThat(capturedRequests).isEmpty();
        }

        @Test
        @DisplayName("Null file list is safely ignored for configured table")
        void nullFileListIgnored() {
            interceptor.onCommit("db.schema.articles", null);

            assertThat(capturedRequests).isEmpty();
        }

        @Test
        @DisplayName("Empty file list is safely ignored for configured table")
        void emptyFileListIgnored() {
            interceptor.onCommit("db.schema.articles", List.of());

            assertThat(capturedRequests).isEmpty();
        }

        @Test
        @DisplayName("EmbeddingColumnConfig rejects null textColumn")
        void configRejectsNullTextColumn() {
            assertThatThrownBy(() -> new EmbeddingColumnConfig(null, "model", "vec"))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("EmbeddingColumnConfig rejects null modelName")
        void configRejectsNullModelName() {
            assertThatThrownBy(() -> new EmbeddingColumnConfig("text", null, "vec"))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("EmbeddingColumnConfig rejects null vectorColumn")
        void configRejectsNullVectorColumn() {
            assertThatThrownBy(() -> new EmbeddingColumnConfig("text", "model", null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("EmbeddingWorkRequest file paths are immutable copies")
        void workRequestFilePathsAreImmutable() {
            List<String> mutableFiles = new ArrayList<>(List.of("/data/file1.parquet", "/data/file2.parquet"));
            interceptor.onCommit("db.schema.articles", mutableFiles);

            // Mutate original list — should not affect captured request
            mutableFiles.add("/data/file3.parquet");

            assertThat(capturedRequests.get(0).filePaths()).hasSize(2);
        }

        @Test
        @DisplayName("Constructor rejects null enabledTables map")
        void constructorRejectsNullEnabledTables() {
            assertThatThrownBy(() -> new StreamEmbeddingInterceptor(null, req -> {}))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("Constructor rejects null embeddingEnqueuer")
        void constructorRejectsNullEnqueuer() {
            assertThatThrownBy(() -> new StreamEmbeddingInterceptor(Map.of(), null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    // -----------------------------------------------------------------------
    // Concurrent CDC Commits (Thread Safety)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Concurrent CDC Commits")
    class ConcurrentCommits {

        @Test
        @DisplayName("100 concurrent commits to same table are all captured without loss")
        void concurrentCommitsAreThreadSafe() throws InterruptedException {
            int threadCount = 100;
            CountDownLatch latch = new CountDownLatch(threadCount);
            ExecutorService executor = Executors.newFixedThreadPool(10);

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        interceptor.onCommit("db.schema.articles",
                                List.of("/data/articles/concurrent-" + idx + ".parquet"));
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(10, TimeUnit.SECONDS);
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            assertThat(capturedRequests).hasSize(threadCount);
            assertThat(capturedRequests).extracting(EmbeddingWorkRequest::tableName)
                    .containsOnly("db.schema.articles");
        }

        @Test
        @DisplayName("Concurrent commits to different tables are correctly routed")
        void concurrentCommitsDifferentTables() throws InterruptedException {
            int commitsPerTable = 30;
            String[] tables = {"db.schema.articles", "db.schema.products", "db.schema.reviews"};
            CountDownLatch latch = new CountDownLatch(commitsPerTable * tables.length);
            ExecutorService executor = Executors.newFixedThreadPool(9);

            for (String table : tables) {
                for (int i = 0; i < commitsPerTable; i++) {
                    final int idx = i;
                    executor.submit(() -> {
                        try {
                            interceptor.onCommit(table,
                                    List.of("/data/concurrent-" + idx + ".parquet"));
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }

            latch.await(10, TimeUnit.SECONDS);
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            assertThat(capturedRequests).hasSize(commitsPerTable * tables.length);
        }
    }

    // -----------------------------------------------------------------------
    // Empty Configuration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Empty Configuration")
    class EmptyConfiguration {

        @Test
        @DisplayName("Interceptor with empty config never triggers embedding")
        void emptyConfigNeverTriggers() {
            StreamEmbeddingInterceptor emptyInterceptor =
                    new StreamEmbeddingInterceptor(Map.of(), capturedRequests::add);

            emptyInterceptor.onCommit("any.table", List.of("/data/file.parquet"));
            emptyInterceptor.onCommit("db.schema.articles", List.of("/data/file.parquet"));

            assertThat(capturedRequests).isEmpty();
            assertThat(emptyInterceptor.enabledTableNames()).isEmpty();
        }
    }
}
