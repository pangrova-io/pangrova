package org.pangrova.testing.integration.e2e.property;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based E2E tests for vector index native SIMD distance computation.
 *
 * <p>Exercises the full native pipeline: SQL → Parser → Planner → Substrait →
 * VeloxPlanConverter → VectorDistanceFunctions.cpp → VectorDistance.cpp (NEON/AVX2
 * SIMD) → Transport → Client.
 *
 * <p>Separated from {@link IndexE2EPropertyTest} because Velox native runtime
 * shutdown can cause SIGSEGV in AlignedBuffer::freeToPool() after vector distance
 * tests complete. With {@code reuseForks=false}, this class runs in its own JVM
 * process, isolating the crash from bloom filter property tests.
 *
 * <p>Validates: Requirements 20.5, 29.1
 */
class VectorIndexE2EPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    /**
     * Waits for Velox CPUThreadPoolExecutor to finish Task destruction.
     * Without this delay, rapid consecutive native queries cause SIGSEGV
     * in AlignedBuffer::freeToPool() due to Task destructor racing with
     * new buffer allocations.
     */
    private void waitForNativeTaskCleanup() {
        try {
            TimeUnit.MILLISECONDS.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Property E2E-4: Native L2 Distance Triangle Inequality.
     * For any query vector Q and data vectors A, B:
     * l2_distance(A, B) <= l2_distance(A, Q) + l2_distance(Q, B).
     *
     * <p>Exercises native path: Substrait → VeloxPlanConverter → VectorDistanceFunctions
     * → VectorDistance.cpp (NEON/AVX2 SIMD).
     */
    @Property(tries = 3)
    void nativeL2DistanceTriangleInequality(
            @ForAll @DoubleRange(min = -10.0, max = 10.0) double qx,
            @ForAll @DoubleRange(min = -10.0, max = 10.0) double qy) {
        String table = "t_prop_l2tri_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0]),"
                + " (2, ARRAY[0.0, 1.0])");

        String query = String.format(
                "SELECT id, l2_distance(embedding, ARRAY[%f, %f]) AS dist"
                        + " FROM %s ORDER BY id", qx, qy, fqn);
        List<TypedRow> rows = sqlRunner.execute(query);
        assertThat(rows).hasSize(2);

        double distAQ = ((Number) rows.get(0).getRaw("dist")).doubleValue();
        double distBQ = ((Number) rows.get(1).getRaw("dist")).doubleValue();

        // A-B distance: l2([1,0], [0,1]) = sqrt(2)
        double distAB = Math.sqrt(2.0);

        // Triangle inequality: d(A,B) <= d(A,Q) + d(Q,B)
        assertThat(distAB).isLessThanOrEqualTo(distAQ + distBQ + 1e-6);

        assertThat(distAQ).isGreaterThanOrEqualTo(0.0);
        assertThat(distBQ).isGreaterThanOrEqualTo(0.0);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    /**
     * Property E2E-5: Native ANN Top-K Ordering Invariant.
     * For any query vector, the nearest neighbor (top-1) distance must be
     * less than or equal to the second nearest (top-2) distance.
     *
     * <p>Exercises: HNSW vector index creation + native l2_distance + ORDER BY + LIMIT.
     */
    @Property(tries = 3)
    void nativeAnnTopKOrdering(
            @ForAll @DoubleRange(min = -5.0, max = 5.0) double qx,
            @ForAll @DoubleRange(min = -5.0, max = 5.0) double qy) {
        String table = "t_prop_annord_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES"
                + " (1, ARRAY[1.0, 0.0]),"
                + " (2, ARRAY[0.0, 1.0]),"
                + " (3, ARRAY[-1.0, 0.0]),"
                + " (4, ARRAY[0.0, -1.0])");

        sqlRunner.executeDdl("CREATE INDEX idx_hnsw ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2')");

        String query = String.format(
                "SELECT id, l2_distance(embedding, ARRAY[%f, %f]) AS dist"
                        + " FROM %s"
                        + " ORDER BY l2_distance(embedding, ARRAY[%f, %f]) ASC"
                        + " LIMIT 4",
                qx, qy, fqn, qx, qy);
        List<TypedRow> rows = sqlRunner.execute(query);
        assertThat(rows).hasSizeGreaterThanOrEqualTo(2);

        for (int i = 0; i < rows.size() - 1; i++) {
            double d1 = ((Number) rows.get(i).getRaw("dist")).doubleValue();
            double d2 = ((Number) rows.get(i + 1).getRaw("dist")).doubleValue();
            assertThat(d1).isLessThanOrEqualTo(d2 + 1e-6);
        }

        sqlRunner.executeDdl("DROP INDEX idx_hnsw ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }

    /**
     * Property E2E-6: Native Cosine Distance Symmetry.
     * cosine_distance(A, B) == cosine_distance(B, A) for any vectors A, B.
     *
     * <p>Exercises native cosine_distance SIMD path.
     */
    @Property(tries = 3)
    void nativeCosineDistanceSymmetry(
            @ForAll @DoubleRange(min = 0.1, max = 10.0) double ax,
            @ForAll @DoubleRange(min = 0.1, max = 10.0) double ay) {
        String table = "t_prop_cossym_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;

        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute(String.format(
                "INSERT INTO %s VALUES (1, ARRAY[%f, %f])", fqn, ax, ay));

        String queryAB = String.format(
                "SELECT cosine_distance(embedding, ARRAY[1.0, 0.0]) AS dist FROM %s", fqn);
        List<TypedRow> rowsAB = sqlRunner.execute(queryAB);
        double distAB = ((Number) rowsAB.get(0).getRaw("dist")).doubleValue();

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, embedding VECTOR(2))");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (1, ARRAY[1.0, 0.0])");

        String queryBA = String.format(
                "SELECT cosine_distance(embedding, ARRAY[%f, %f]) AS dist FROM %s",
                ax, ay, fqn);
        List<TypedRow> rowsBA = sqlRunner.execute(queryBA);
        double distBA = ((Number) rowsBA.get(0).getRaw("dist")).doubleValue();

        assertThat(Math.abs(distAB - distBA)).isLessThan(1e-6);
        assertThat(distAB).isBetween(0.0 - 1e-6, 2.0 + 1e-6);

        sqlRunner.executeDdl("DROP TABLE " + fqn);
        waitForNativeTaskCleanup();
    }
}
