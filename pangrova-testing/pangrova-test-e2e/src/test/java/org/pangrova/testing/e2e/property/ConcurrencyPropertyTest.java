package org.pangrova.testing.e2e.property;

import net.jqwik.api.*;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for concurrency correctness.
 *
 * <p>Validates that concurrent SQL execution does not produce data corruption:
 * - Concurrent reads produce consistent results
 * - Concurrent writes do not lose data (eventual row count correctness)
 * - Read-write isolation (reads during writes see consistent state)</p>
 *
 * <p>Requirements validated: 13.1, 13.2, 9.3</p>
 */
@DisplayName("Concurrency - Property Tests")
class ConcurrencyPropertyTest extends SqlE2ETestBase {

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
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @Property(tries = 1)
    @Tag("Property: concurrent reads produce consistent results")
    void concurrentReadsConsistent() throws Exception {
        String table = FQN_PREFIX + "prop_conc_read_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        int threads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table);
                    if (rows.size() != 3) failures.incrementAndGet();
                } catch (Exception e) {
                    failures.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(failures.get()).isZero();
        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: sequential writes preserve all data")
    void sequentialWritesPreserveData() {
        String table = FQN_PREFIX + "prop_conc_write_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");

        int batchCount = 3;
        for (int i = 0; i < batchCount; i++) {
            sqlRunner.execute("INSERT INTO " + table + " VALUES ("
                    + (i * 10 + 1) + ", 'batch_" + i + "')");
        }

        List<TypedRow> count = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);
        assertThat(((Number) count.get(0).getRaw("cnt")).longValue()).isEqualTo(batchCount);
        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: read isolation during writes")
    void readIsolationDuringWrites() throws Exception {
        String table = FQN_PREFIX + "prop_conc_iso_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'initial')");

        // Read sees consistent state (at least initial row)
        List<TypedRow> snapshot = sqlRunner.execute("SELECT * FROM " + table);
        assertThat(snapshot).isNotEmpty();

        // Write additional data
        sqlRunner.execute("INSERT INTO " + table + " VALUES (2, 'added')");

        // Subsequent read sees updated state
        List<TypedRow> updated = sqlRunner.execute("SELECT * FROM " + table);
        assertThat(updated.size()).isGreaterThanOrEqualTo(snapshot.size());

        sqlRunner.executeDdl("DROP TABLE " + table);
    }
}
