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
package org.pangrova.testing.integration.e2e.index.vector;

import org.pangrova.protocol.v2.MemorySnapshot;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.index.VectorIndexE2ETestBase;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real SQL-to-Native DiskANN functional acceptance on the in-process Worker.
 * Every unfiltered search requires READY/PUBLISHED, the issued ANN binding,
 * Native completion, and checksum-verified Native artifact materialization.
 * The WHERE case requires the real filtered Native ANN owner.
 * Local fixtures and timings do not qualify multi-node scale or SF100/1000 performance.
 */
@Tag("diskann")
@DisplayName("DiskANN Distributed Adaptation E2E")
class DiskAnnDistributedE2ETest extends VectorIndexE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int DIM = 16;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    private void waitForNativeTaskCleanup() {
        try {
            TimeUnit.MILLISECONDS.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Generates a deterministic vector for the given row id and dimension.
     */
    private static String generateVector(int id, int dim) {
        StringBuilder sb = new StringBuilder("ARRAY[");
        for (int d = 0; d < dim; d++) {
            if (d > 0) {
                sb.append(",");
            }
            double val = Math.sin(id * 0.3 + d * 0.7);
            sb.append(String.format(java.util.Locale.ROOT, "%.6f", val));
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Builds a bulk INSERT statement with N vectors of given dimension.
     */
    private static String buildInsertStatement(String fqn, int count, int dim) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
        for (int i = 1; i <= count; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("(").append(i).append(", ");
            if (dim == DIM) {
                // Include category column for tables that have it
                sb.append("'cat_").append(i % 4).append("', ");
            }
            sb.append(generateVector(i, dim)).append(")");
        }
        return sb.toString();
    }

    private static String buildInsertWithCategory(String fqn, int count, int dim) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
        for (int i = 1; i <= count; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("(").append(i).append(", '")
                    .append("cat_").append(i % 4).append("', ")
                    .append(generateVector(i, dim)).append(")");
        }
        return sb.toString();
    }

    private static String buildInsertNoCategory(String fqn, int count, int dim) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
        for (int i = 1; i <= count; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("(").append(i).append(", ")
                    .append(generateVector(i, dim)).append(")");
        }
        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 1: diskAnnBuildAndSearch — validates P0-1, P2-3, P1-1
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 1: DiskANN build and basic ANN search")
    void diskAnnBuildAndSearch(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_basic_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertNoCategory(fqn, 200, DIM));

        // Create DiskANN index
        sqlRunner.executeDdl("CREATE INDEX idx_diskann_basic ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_basic", fqn);

        // Query vector = row 1's vector (should be nearest neighbor to itself)
        String queryVec = generateVector(1, DIM);

        List<TypedRow> rows = executeAnn(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 5", publication);

        assertThat(rows).hasSize(5);
        // Row 1 should be the exact match (distance ~0)
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        double firstDist = ((Number) rows.get(0).getRaw("dist")).doubleValue();
        assertThat(firstDist).isLessThan(0.001);

        // Results should be ordered by distance
        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP INDEX idx_diskann_basic ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 2: diskAnnBuildOutputPath — validates P0-1
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 2: Published objects are actually materialized by Native")
    void diskAnnBuildOutputPath(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_path_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertNoCategory(fqn, 50, DIM));

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_path ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_path", fqn);

        // Require the actual published ANN task and Native object checks below.
        String queryVec = generateVector(1, DIM);
        List<TypedRow> rows = executeAnn(
                "SELECT id FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 3", publication);
        assertThat(rows).isNotEmpty();

        // Successful rows alone are insufficient; executeAnn verifies the issued binding
        // and bytes in the runtime-owned content-addressed cache.
        sqlRunner.executeDdl("DROP INDEX idx_diskann_path ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 3: diskAnnIndexDistribution — validates P0-2
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 3: Published ANN binding is executed by the Worker")
    void diskAnnIndexDistribution(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_dist_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertNoCategory(fqn, 100, DIM));

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_dist ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_dist", fqn);

        // Search verifies that the Worker successfully received and loaded the index
        String queryVec = generateVector(50, DIM);
        List<TypedRow> rows = executeAnn(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 5", publication);

        assertThat(rows).hasSize(5);
        // Row 50 should be nearest to its own vector
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(50L);

        sqlRunner.executeDdl("DROP INDEX idx_diskann_dist ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 4: diskAnnShardRegistration — validates P0-3
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 4: Repeated searches use the published ANN generation")
    void diskAnnShardRegistration(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_shard_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertNoCategory(fqn, 150, DIM));

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_shard ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_shard", fqn);

        // Multiple queries to verify routing consistency
        for (int q = 1; q <= 3; q++) {
            String queryVec = generateVector(q * 10, DIM);
            List<TypedRow> rows = executeAnn(
                    "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                            + " FROM " + fqn
                            + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                            + " LIMIT 3", publication);

            assertThat(rows).hasSize(3);
            // First result should be the exact match
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo((long) (q * 10));
        }

        sqlRunner.executeDdl("DROP INDEX idx_diskann_shard ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 5: diskAnnCachedSearch — validates P1-1
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 5: Repeated ANN searches retain the existing local timing bound")
    void diskAnnCachedSearch(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_cache_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertNoCategory(fqn, 200, DIM));

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_cache ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_cache", fqn);

        String queryVec = generateVector(1, DIM);
        String sql = "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                + " FROM " + fqn
                + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                + " LIMIT 5";

        // First query — cold cache (loads index from disk)
        long start1 = System.nanoTime();
        List<TypedRow> rows1 = executeAnn(sql, publication);
        long elapsed1 = System.nanoTime() - start1;

        assertThat(rows1).hasSize(5);

        // Subsequent queries — warm cache
        long totalWarm = 0;
        int warmRuns = 10;
        for (int i = 0; i < warmRuns; i++) {
            long s = System.nanoTime();
            List<TypedRow> rows = executeAnn(sql, publication);
            totalWarm += System.nanoTime() - s;
            assertThat(rows).hasSize(5);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        }
        long avgWarm = totalWarm / warmRuns;

        // Retain the existing local timing bound. These small samples include
        // verification overhead and do not establish a calibrated performance benefit.
        assertThat(avgWarm).isLessThan(elapsed1 * 5);

        sqlRunner.executeDdl("DROP INDEX idx_diskann_cache ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 6: diskAnnParallelResultCollection — validates P1-2
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 6: Native ANN ordered result collection")
    void diskAnnParallelResultCollection(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_parallel_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        // Larger dataset to potentially span multiple splits/workers
        sqlRunner.execute(buildInsertNoCategory(fqn, 300, DIM));

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_par ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_par", fqn);

        String queryVec = generateVector(100, DIM);
        List<TypedRow> rows = executeAnn(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 10", publication);

        assertThat(rows).hasSize(10);
        // Row 100 should be the closest (exact match)
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(100L);
        // Results are ordered
        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP INDEX idx_diskann_par ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    @DisplayName("Bulk VECTOR VALUES INSERT writes 1000 rows and releases Native task memory")
    void bulkVectorValuesReleaseNativeTaskMemory() throws Exception {
        String fqn = FQN_PREFIX + "t_vector_bulk_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + DIM + "))");
        MemorySnapshot baseline = nativeMemorySnapshot();

        sqlRunner.execute(buildInsertNoCategory(fqn, 1000, DIM));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            MemorySnapshot after = nativeMemorySnapshot();
            assertThat(after.getActivePoolCount()).isEqualTo(baseline.getActivePoolCount());
            assertThat(after.getCurrentUsedBytes()).isLessThanOrEqualTo(baseline.getCurrentUsedBytes());
        });
        List<TypedRow> rows = sqlRunner.execute("SELECT id FROM " + fqn + " ORDER BY id");
        assertThat(rows).hasSize(1000);
        assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
        assertThat(rows.get(999).getLong("id")).isEqualTo(1000L);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    private static MemorySnapshot nativeMemorySnapshot() throws Exception {
        byte[] bytes = NativeRuntime.nativeGetMemorySnapshot();
        assertThat(bytes).as("Native runtime memory snapshot").isNotNull();
        return MemorySnapshot.parseFrom(bytes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 7: diskAnnRecommendation — validates P1-3
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 7: Explicit DISK_ANN supports a multi-file 64-dimensional fixture")
    void diskAnnRecommendation(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_recommend_" + System.nanoTime();
        int dim = 64;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + dim + "))");

        // This functional fixture spans multiple real source files. DISK_ANN is
        // explicitly requested; no algorithm recommendation or scale claim follows.
        int batchSize = 50;
        for (int batch = 0; batch < 500 / batchSize; batch++) {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
            for (int j = 0; j < batchSize; j++) {
                int i = batch * batchSize + j + 1;
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append("(").append(i).append(", ").append(generateVector(i, dim)).append(")");
            }
            sqlRunner.execute(sb.toString());
        }

        // Create index with DISK_ANN explicitly — validates the algorithm is available
        sqlRunner.executeDdl("CREATE INDEX idx_diskann_rec ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_rec", fqn);

        // Verify the explicit algorithm reaches the issued Native ANN task.
        String queryVec = generateVector(1, dim);
        List<TypedRow> rows = executeAnn(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 5", publication);

        assertThat(rows).hasSize(5);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);

        sqlRunner.executeDdl("DROP INDEX idx_diskann_rec ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 8: diskAnnFilteredSearch — validates P2-1
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 8: Native filtered DiskANN preserves SQL semantics with a READY index")
    void diskAnnFilteredSearch(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_filter_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertWithCategory(fqn, 200, DIM));

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_filter ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_filter", fqn);

        // Filtered query — only rows with category = 'cat_0' (ids: 4, 8, 12, ...)
        String queryVec = generateVector(4, DIM);
        var filtered = executeObserved(
                "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                        + " FROM " + fqn
                        + " WHERE category = 'cat_0'"
                        + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                        + " LIMIT 5");
        assertFilteredAnn(publication, filtered);
        List<TypedRow> rows = filtered.rows();

        assertThat(rows).hasSize(5);
        // All results should belong to category 'cat_0' (id % 4 == 0)
        for (TypedRow row : rows) {
            long id = ((Number) row.getRaw("id")).longValue();
            assertThat(id % 4).isEqualTo(0L);
        }
        // First result should be row 4 (exact match of query vector within filtered set)
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(4L);

        // Results are ordered by distance
        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP INDEX idx_diskann_filter ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    @Test
    @DisplayName("Native filtered DiskANN returns no rows and drains when no source row matches")
    void diskAnnEmptyFilteredResultDrainsItsNativeOwner() {
        String fqn = FQN_PREFIX + "t_diskann_empty_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertWithCategory(fqn, 64, DIM));
        sqlRunner.executeDdl("CREATE INDEX idx_diskann_empty ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_empty", fqn);
        String queryVector = generateVector(1, DIM);
        // Both values exist in the file, but never in the same row. Independent
        // column statistics cannot prune this supported conjunctive predicate.
        var empty = executeObserved("SELECT id, l2_distance(embedding, " + queryVector + ") AS dist"
                + " FROM " + fqn + " WHERE category = 'cat_0' AND id = 1"
                + " ORDER BY l2_distance(embedding, " + queryVector + ") ASC LIMIT 5");
        assertFilteredAnn(publication, empty);
        assertThat(empty.rows()).isEmpty();
        sqlRunner.executeDdl("DROP INDEX idx_diskann_empty ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }

    @Test
    @DisplayName("Metadata-pruned empty DiskANN input issues no ANN work")
    void diskAnnMetadataPrunedEmptyInputDoesNotIssueAnnWork() {
        String fqn = FQN_PREFIX + "t_diskann_pruned_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(" + DIM + "))");
        sqlRunner.execute(buildInsertWithCategory(fqn, 64, DIM));
        sqlRunner.executeDdl("CREATE INDEX idx_diskann_pruned ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        awaitPublishedVectorIndex("idx_diskann_pruned", fqn);
        String queryVector = generateVector(1, DIM);
        var empty = executeObserved("SELECT id, l2_distance(embedding, " + queryVector + ") AS dist"
                + " FROM " + fqn + " WHERE category = 'absent-category'"
                + " ORDER BY l2_distance(embedding, " + queryVector + ") ASC LIMIT 5");
        assertThat(empty.rows()).isEmpty();
        assertThat(empty.tasks().stream().flatMap(task -> task.request().getOperatorBindingsList().stream())
                .map(binding -> binding.getCapability().getCapabilityId()).toList())
                .doesNotContain(org.pangrova.protocol.capability.BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID,
                        org.pangrova.protocol.capability.BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID);
        sqlRunner.executeDdl("DROP INDEX idx_diskann_pruned ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 9: diskAnnRerankPrecision — validates P2-3
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Case 9: Native ANN exact distance rescore retains top-1 accuracy")
    void diskAnnRerankPrecision(TestInfo testInfo) {
        String fqn = FQN_PREFIX + "t_diskann_rerank_" + System.nanoTime();
        int dim = 32;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(" + dim + "))");

        // Insert vectors with controlled distribution
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
        for (int i = 1; i <= 300; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("(").append(i).append(", ").append(generateVector(i, dim)).append(")");
        }
        sqlRunner.execute(sb.toString());

        sqlRunner.executeDdl("CREATE INDEX idx_diskann_rerank ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2')");
        var publication = awaitPublishedVectorIndex("idx_diskann_rerank", fqn);

        // Run search for multiple query points and verify top-1 accuracy
        int correctTop1 = 0;
        int totalQueries = 10;
        for (int q = 1; q <= totalQueries; q++) {
            int targetId = q * 30; // query points: 30, 60, 90, ..., 300
            String queryVec = generateVector(targetId, dim);
            List<TypedRow> rows = executeAnn(
                    "SELECT id, l2_distance(embedding, " + queryVec + ") AS dist"
                            + " FROM " + fqn
                            + " ORDER BY l2_distance(embedding, " + queryVec + ") ASC"
                            + " LIMIT 1", publication);

            assertThat(rows).hasSize(1);
            long topId = ((Number) rows.get(0).getRaw("id")).longValue();
            if (topId == targetId) {
                correctTop1++;
            }
        }

        // Retain the original ten-query functional top-1 bound. This is distance
        // rescore, and does not demonstrate model reranking or a PQ comparison.
        assertThat(correctTop1).isGreaterThanOrEqualTo(9);

        sqlRunner.executeDdl("DROP INDEX idx_diskann_rerank ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }
}
