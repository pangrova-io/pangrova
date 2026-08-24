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
package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive index lifecycle performance E2E test covering all index types.
 *
 * <p>Each nested class exercises the full lifecycle for one index type:
 * <ol>
 *   <li>Create table + bulk insert 200 rows across 10 data files</li>
 *   <li>Query without index (measure time, verify correctness)</li>
 *   <li>Create index + verify via SHOW INDEXES</li>
 *   <li>Query with index (measure time, verify superset invariant)</li>
 *   <li>Drop index + wait for async metadata cleanup</li>
 *   <li>Query after drop (measure time, verify correctness)</li>
 * </ol>
 *
 * <p>Index types covered: BTREE, BLOOM_FILTER, BITMAP, NGRAM_BLOOM_FILTER,
 * GIN, R_TREE, VECTOR (HNSW + IVFPQ).
 */
@DisplayName("Index Lifecycle Performance E2E")
class IndexLifecyclePerformanceE2ETest extends SqlE2ETestBase {

    private static final Logger LOG = LoggerFactory.getLogger(IndexLifecyclePerformanceE2ETest.class);

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int BATCHES = 50;
    private static final int ROWS_PER_BATCH = 200;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    /**
     * F-E2E-04 fix: replaced ad-hoc for-loop+Thread.sleep with Awaitility deterministic
     * polling (CLAUDE.md C3 anti-busy-wait rule).
     */
    private static void waitForIndexReady(String indexName, String fqn) {
        org.awaitility.Awaitility.await("index-ready: " + indexName + " on " + fqn)
                .atMost(java.time.Duration.ofSeconds(30))
                .pollInterval(java.time.Duration.ofMillis(500))
                .pollDelay(java.time.Duration.ZERO)
                .ignoreExceptions()
                .until(() -> {
                    List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn);
                    for (TypedRow r : indexes) {
                        if (!indexName.equals(r.getRaw("index_name").toString())) continue;
                        String state = r.getRaw("state").toString();
                        if ("READY".equals(state)) {
                            LOG.info("event=index_ready index={}", indexName);
                            return true;
                        }
                        if ("STALE".equals(state)) {
                            LOG.warn("event=index_stale index={} fqn={} "
                                    + "reason=build_completed_with_failures", indexName, fqn);
                            return true;
                        }
                    }
                    return false;
                });
    }

    /** F-E2E-04 fix: Awaitility-based deterministic polling. */
    private static void waitForIndexDrop(String indexName, String fqn) {
        org.awaitility.Awaitility.await("index-drop: " + indexName + " on " + fqn)
                .atMost(java.time.Duration.ofSeconds(10))
                .pollInterval(java.time.Duration.ofMillis(500))
                .pollDelay(java.time.Duration.ZERO)
                .ignoreExceptions()
                .until(() -> {
                    List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn);
                    boolean stillActive = indexes.stream()
                            .anyMatch(r -> indexName.equals(r.getRaw("index_name").toString())
                                    && !"DROPPED".equals(r.getRaw("state").toString()));
                    return !stillActive;
                });
    }

    // -----------------------------------------------------------------------
    // BTREE Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BTREE Index Lifecycle")
    class BTreeLifecycle {

        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_btree_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, amount DOUBLE, category VARCHAR)");

            for (int batch = 0; batch < BATCHES; batch++) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    int rowId = batch * ROWS_PER_BATCH + i + 1;
                    double amount = batch * 100.0 + i * 5.0;
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(rowId).append(", ").append(amount)
                      .append(", 'cat_").append(batch).append("')");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("BTREE: create → query → index → query → drop → query")
        void fullLifecycle() throws Exception {
            String query = "SELECT * FROM " + fqn() + " WHERE amount BETWEEN 50.0 AND 70.0";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_btree ON " + fqn()
                    + "(amount) USING BTREE");
            waitForIndexReady("idx_btree", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_btree".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).containsAll(baseline);

            LOG.info("event=index_perf type=BTREE noIndex={}ms withIndex={}ms", noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_btree ON " + fqn());
            waitForIndexDrop("idx_btree", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).containsAll(baseline);

            LOG.info("event=index_lifecycle_complete type=BTREE noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }

    // -----------------------------------------------------------------------
    // BLOOM_FILTER Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BLOOM_FILTER Index Lifecycle")
    class BloomFilterLifecycle {

        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_bf_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (user_id BIGINT, region VARCHAR, score DOUBLE)");

            for (int batch = 0; batch < BATCHES; batch++) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    long userId = (long) batch * ROWS_PER_BATCH + i + 1;
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(userId).append(", 'region_").append(batch)
                      .append("', ").append(userId * 1.5).append(")");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("BLOOM_FILTER: create → query → index → query → drop → query")
        void fullLifecycle() throws Exception {
            String query = "SELECT * FROM " + fqn() + " WHERE user_id = 42";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            waitForIndexReady("idx_bloom", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_bloom".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).containsAll(baseline);

            LOG.info("event=index_perf type=BLOOM_FILTER noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_bloom ON " + fqn());
            waitForIndexDrop("idx_bloom", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).containsAll(baseline);

            LOG.info("event=index_lifecycle_complete type=BLOOM_FILTER noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }

    // -----------------------------------------------------------------------
    // BITMAP Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BITMAP Index Lifecycle")
    class BitmapLifecycle {

        private static final String[] STATUSES = {"active", "inactive", "pending", "archived"};
        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_bm_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, status VARCHAR, value DOUBLE)");

            for (int batch = 0; batch < BATCHES; batch++) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    int rowId = batch * ROWS_PER_BATCH + i + 1;
                    String status = STATUSES[(batch * ROWS_PER_BATCH + i) % STATUSES.length];
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(rowId).append(", '").append(status)
                      .append("', ").append(rowId * 2.0).append(")");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("BITMAP: create → query → index → query → drop → query")
        void fullLifecycle() throws Exception {
            String query = "SELECT * FROM " + fqn() + " WHERE status = 'active'";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_bitmap ON " + fqn()
                    + "(status) USING BITMAP");
            waitForIndexReady("idx_bitmap", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_bitmap".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).containsAll(baseline);

            LOG.info("event=index_perf type=BITMAP noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_bitmap ON " + fqn());
            waitForIndexDrop("idx_bitmap", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).containsAll(baseline);

            LOG.info("event=index_lifecycle_complete type=BITMAP noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }

    // -----------------------------------------------------------------------
    // NGRAM_BLOOM_FILTER Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("NGRAM_BLOOM_FILTER Index Lifecycle")
    class NgramBloomLifecycle {

        private static final String[] NAMES = {
            "alice", "bob", "charlie", "diana", "edward",
            "fiona", "george", "helen", "ivan", "julia"
        };
        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_ngram_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, email VARCHAR)");

            for (int batch = 0; batch < BATCHES; batch++) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    int rowId = batch * ROWS_PER_BATCH + i + 1;
                    String name = NAMES[batch % NAMES.length] + "_" + batch + "_" + i;
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(rowId).append(", '").append(name)
                      .append("', '").append(name).append("@test.com')");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("NGRAM_BLOOM_FILTER: create → query → index → query → drop → query")
        void fullLifecycle() throws Exception {
            String query = "SELECT * FROM " + fqn() + " WHERE name LIKE '%alice%'";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_ngram ON " + fqn()
                    + "(name) USING NGRAM_BLOOM_FILTER");
            waitForIndexReady("idx_ngram", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_ngram".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).containsAll(baseline);

            LOG.info("event=index_perf type=NGRAM_BLOOM_FILTER noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_ngram ON " + fqn());
            waitForIndexDrop("idx_ngram", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).containsAll(baseline);

            LOG.info("event=index_lifecycle_complete type=NGRAM_BLOOM_FILTER noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }

    // -----------------------------------------------------------------------
    // GIN Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("GIN Index Lifecycle")
    class GinLifecycle {

        private static final String[][] TAG_SETS = {
            {"alpha,beta"},      {"gamma,delta"},
            {"epsilon,zeta"},    {"eta,theta"},
            {"alpha,gamma"},     {"beta,zeta"},
            {"delta,eta"},       {"theta,alpha"},
            {"gamma,epsilon"},   {"zeta,beta"}
        };
        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_gin_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, tags VARCHAR, data VARCHAR)");

            for (int batch = 0; batch < BATCHES; batch++) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    int rowId = batch * ROWS_PER_BATCH + i + 1;
                    String tags = TAG_SETS[batch % TAG_SETS.length][0];
                    if (i % 2 == 1) {
                        String[] parts = tags.split(",");
                        tags = parts[1] + "," + parts[0];
                    }
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(rowId).append(", '").append(tags)
                      .append("', 'data_").append(rowId).append("')");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("GIN: create → query → index → query → drop → query")
        void fullLifecycle() throws Exception {
            String query = "SELECT * FROM " + fqn() + " WHERE tags LIKE '%alpha%'";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_gin ON " + fqn()
                    + "(tags) USING GIN");
            waitForIndexReady("idx_gin", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_gin".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).containsAll(baseline);

            LOG.info("event=index_perf type=GIN noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_gin ON " + fqn());
            waitForIndexDrop("idx_gin", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).containsAll(baseline);

            LOG.info("event=index_lifecycle_complete type=GIN noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }

    // -----------------------------------------------------------------------
    // R_TREE Index Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R_TREE Index Lifecycle")
    class RTreeLifecycle {

        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_rtree_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, x DOUBLE, y DOUBLE, label VARCHAR)");

            for (int batch = 0; batch < BATCHES; batch++) {
                double xBase = batch * 100.0;
                double yBase = batch * 100.0;
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    int rowId = batch * ROWS_PER_BATCH + i + 1;
                    double x = xBase + (i % 5) * 2.0;
                    double y = yBase + (i / 5) * 2.0;
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(rowId).append(", ").append(x).append(", ").append(y)
                      .append(", 'q").append(batch).append("_").append(i).append("')");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("R_TREE: create → query → index → query → drop → query")
        void fullLifecycle() throws Exception {
            String query = "SELECT * FROM " + fqn()
                    + " WHERE (x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_rtree ON " + fqn()
                    + "(x, y) USING R_TREE");
            waitForIndexReady("idx_rtree", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_rtree".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).containsAll(baseline);

            LOG.info("event=index_perf type=R_TREE noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_rtree ON " + fqn());
            waitForIndexDrop("idx_rtree", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).containsAll(baseline);

            LOG.info("event=index_lifecycle_complete type=R_TREE noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }

    // -----------------------------------------------------------------------
    // VECTOR Index Lifecycle (HNSW + IVFPQ)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("VECTOR Index Lifecycle")
    class VectorLifecycle {

        private String tableName;

        @BeforeEach
        void createTableAndBulkInsert(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            tableName = "t_perf_vec_" + method + "_" + System.nanoTime();
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, embedding VECTOR(4), label VARCHAR)");

            for (int batch = 0; batch < BATCHES; batch++) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(fqn()).append(" VALUES ");
                for (int i = 0; i < ROWS_PER_BATCH; i++) {
                    int rowId = batch * ROWS_PER_BATCH + i + 1;
                    double angle = (rowId - 1) * (2.0 * Math.PI / (BATCHES * ROWS_PER_BATCH));
                    double v0 = Math.cos(angle);
                    double v1 = Math.sin(angle);
                    double v2 = Math.cos(angle * 2.0) * 0.5;
                    double v3 = Math.sin(angle * 2.0) * 0.5;
                    if (i > 0) sb.append(", ");
                    sb.append("(").append(rowId).append(", ARRAY[")
                      .append(String.format(Locale.US, "%.4f, %.4f, %.4f, %.4f", v0, v1, v2, v3))
                      .append("], 'v_").append(rowId).append("')");
                }
                sqlRunner.execute(sb.toString());
            }
        }

        private String fqn() {
            return FQN_PREFIX + tableName;
        }

        @Test
        @DisplayName("HNSW: create → query → index → query → drop → query")
        void hnswFullLifecycle() throws Exception {
            String query = "SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.5, 0.0]) AS dist"
                    + " FROM " + fqn()
                    + " ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.5, 0.0]) ASC"
                    + " LIMIT 5";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();
            assertThat(baseline).hasSizeLessThanOrEqualTo(5);

            sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");
            waitForIndexReady("idx_hnsw", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_hnsw".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).isNotEmpty();

            Set<String> baselineIds = baseline.stream()
                    .map(r -> r.getRaw("id").toString()).collect(Collectors.toSet());
            Set<String> indexedIds = indexed.stream()
                    .map(r -> r.getRaw("id").toString()).collect(Collectors.toSet());
            long overlap = indexedIds.stream().filter(baselineIds::contains).count();
            double recall = (double) overlap / baselineIds.size();
            assertThat(recall).as("HNSW recall vs brute-force").isGreaterThanOrEqualTo(0.6);

            LOG.info("event=index_perf type=HNSW noIndex={}ms withIndex={}ms recall={}",
                    noIndexMs, withIndexMs, String.format(Locale.US, "%.2f", recall));

            sqlRunner.executeDdl("DROP INDEX idx_hnsw ON " + fqn());
            waitForIndexDrop("idx_hnsw", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).isNotEmpty();

            LOG.info("event=index_lifecycle_complete type=HNSW noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }

        @Test
        @DisplayName("IVFPQ: create → query → index → query → drop → query")
        void ivfpqFullLifecycle() throws Exception {
            String query = "SELECT id, l2_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.5]) AS dist"
                    + " FROM " + fqn()
                    + " ORDER BY l2_distance(embedding, ARRAY[0.0, 1.0, 0.0, 0.5]) ASC"
                    + " LIMIT 5";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_ivfpq ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2',"
                    + " 'ivfpq.nlist'='4', 'ivfpq.m'='2', 'ivfpq.nbits'='8')");
            waitForIndexReady("idx_ivfpq", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_ivfpq".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).isNotEmpty();

            LOG.info("event=index_perf type=IVFPQ noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_ivfpq ON " + fqn());
            waitForIndexDrop("idx_ivfpq", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).isNotEmpty();

            LOG.info("event=index_lifecycle_complete type=IVFPQ noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }

        @Test
        @DisplayName("HNSW Cosine: create → query → index → query → drop → query")
        void hnswCosineFullLifecycle() throws Exception {
            String query = "SELECT id, cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist"
                    + " FROM " + fqn()
                    + " ORDER BY cosine_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC"
                    + " LIMIT 5";

            long t1 = System.nanoTime();
            List<TypedRow> baseline = sqlRunner.execute(query);
            long noIndexMs = (System.nanoTime() - t1) / 1_000_000;
            assertThat(baseline).isNotEmpty();

            sqlRunner.executeDdl("CREATE INDEX idx_cos ON " + fqn()
                    + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='COSINE')");
            waitForIndexReady("idx_cos", fqn());
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r -> "idx_cos".equals(r.getRaw("index_name").toString()));

            long t2 = System.nanoTime();
            List<TypedRow> indexed = sqlRunner.execute(query);
            long withIndexMs = (System.nanoTime() - t2) / 1_000_000;
            assertThat(indexed).isNotEmpty();

            LOG.info("event=index_perf type=HNSW_COSINE noIndex={}ms withIndex={}ms",
                    noIndexMs, withIndexMs);

            sqlRunner.executeDdl("DROP INDEX idx_cos ON " + fqn());
            waitForIndexDrop("idx_cos", fqn());

            long t3 = System.nanoTime();
            List<TypedRow> afterDrop = sqlRunner.execute(query);
            long afterDropMs = (System.nanoTime() - t3) / 1_000_000;
            assertThat(afterDrop).isNotEmpty();

            LOG.info("event=index_lifecycle_complete type=HNSW_COSINE noIndex={}ms withIndex={}ms afterDrop={}ms",
                    noIndexMs, withIndexMs, afterDropMs);
        }
    }
}
