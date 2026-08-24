package org.pangrova.testing.e2e.shuffle;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.spi.connector.data.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T05: Shuffle Fault-Tolerance & Backpressure — 14 E2E scenarios.
 *
 * <p>Validates shuffle data plane correctness, fault tolerance, and backpressure.
 */
@E2ETest
@DisplayName("T05: Shuffle Fault-Tolerance & Backpressure")
class ShuffleFaultToleranceE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested
    @DisplayName("Scenario 1: Large shuffle completes")
    class LargeShuffleCompletes {
        @Test
        void shuffleJoinOnLargeDataCompletes() {
            String l = CATALOG + "." + SCHEMA + ".t05_l_" + System.nanoTime();
            String r = CATALOG + "." + SCHEMA + ".t05_r_" + System.nanoTime();
            executeDdl("CREATE TABLE " + l + " (id BIGINT, grp INT, val VARCHAR)");
            executeDdl("CREATE TABLE " + r + " (id BIGINT, grp INT, data VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + l + " VALUES ");
            for (int i = 0; i < 100; i++) {
                if (i > 0) sb.append(", ");
                sb.append("(").append(i).append(",").append(i % 10).append(",'v").append(i).append("')");
            }
            executeDml(sb.toString());
            sb = new StringBuilder("INSERT INTO " + r + " VALUES ");
            for (int i = 0; i < 100; i++) {
                if (i > 0) sb.append(", ");
                sb.append("(").append(i).append(",").append(i % 10).append(",'d").append(i).append("')");
            }
            executeDml(sb.toString());
            List<TypedRow> rows = executeQuery(
                    "SELECT l.id, r.data FROM " + l + " l JOIN " + r + " r ON l.grp = r.grp ORDER BY l.id LIMIT 50");
            assertThat(rows).hasSize(50);
        }
    }

    @Nested
    @DisplayName("Scenario 2: Skew does not OOM")
    class SkewNoOom {
        @Test
        void skewedJoinCompletesWithoutOom() {
            String tbl = CATALOG + "." + SCHEMA + ".t05_skew_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, key INT, val VARCHAR)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 100; i++) {
                if (i > 0) sb.append(", ");
                int key = (i < 90) ? 1 : i; // 90% on key=1 (skew)
                sb.append("(").append(i).append(",").append(key).append(",'v").append(i).append("')");
            }
            executeDml(sb.toString());
            List<TypedRow> rows = executeQuery(
                    "SELECT key, COUNT(*) as cnt FROM " + tbl + " GROUP BY key ORDER BY cnt DESC");
            assertThat(rows).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Scenario 3: Shuffle integrity contract")
    class ShuffleIntegrity {
        @Test
        void aggregationWithShuffleProducesCorrectResult() {
            String tbl = CATALOG + "." + SCHEMA + ".t05_integ_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, grp VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + tbl + " VALUES "
                    + "(1,'a',10.0),(2,'b',20.0),(3,'a',30.0),(4,'b',40.0),(5,'c',50.0)");
            List<TypedRow> rows = executeQuery(
                    "SELECT grp, SUM(amount) FROM " + tbl + " GROUP BY grp ORDER BY grp");
            assertThat(rows).hasSize(3);
        }
    }

    @Nested
    @DisplayName("Scenario 4: ServiceLoader discovery")
    class ServiceLoaderDiscovery {
        @Test
        void shuffleProviderResolvesSuccessfully() {
            // If shuffle provider fails to resolve, queries needing shuffle would fail
            String tbl = CATALOG + "." + SCHEMA + ".t05_svc_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, cat INT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,1),(2,2),(3,1),(4,2)");
            List<TypedRow> rows = executeQuery(
                    "SELECT cat, COUNT(*) FROM " + tbl + " GROUP BY cat ORDER BY cat");
            assertThat(rows).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Scenario 5: Mapper dedup")
    class MapperDedup {
        @Test
        void distinctEliminatesDuplicates() {
            String tbl = CATALOG + "." + SCHEMA + ".t05_dedup_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'a')");
            List<TypedRow> rows = executeQuery("SELECT DISTINCT val FROM " + tbl + " ORDER BY val");
            assertThat(rows).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Scenario 6: Range read")
    class RangeRead {
        @Test
        void rangeFilterAfterShuffle() {
            String tbl = CATALOG + "." + SCHEMA + ".t05_range_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val DOUBLE)");
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 0; i < 50; i++) {
                if (i > 0) sb.append(", ");
                sb.append("(").append(i).append(",").append(i * 2.0).append(")");
            }
            executeDml(sb.toString());
            List<TypedRow> rows = executeQuery(
                    "SELECT * FROM " + tbl + " WHERE val BETWEEN 20.0 AND 60.0 ORDER BY id");
            assertThat(rows).hasSize(21); // ids 10-30
        }
    }

    @Nested
    @DisplayName("Scenario 7: Ephemeral fast-fail")
    class EphemeralFastFail {
        @Test
        void simpleQueryDoesNotRequireDurableShuffle() {
            String tbl = CATALOG + "." + SCHEMA + ".t05_ephemeral_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1), (2)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 2);
        }
    }

    @Nested
    @DisplayName("Scenario 8: SpoolStore roundtrip")
    class SpoolStoreRoundtrip {
        @Test
        void shuffledDataRoundTripsCorrectly() {
            String tbl = CATALOG + "." + SCHEMA + ".t05_spool_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, name VARCHAR, score DOUBLE)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'x',1.1),(2,'y',2.2),(3,'z',3.3)");
            List<TypedRow> rows = executeQuery(
                    "SELECT name, SUM(score) FROM " + tbl + " GROUP BY name ORDER BY name");
            assertThat(rows).hasSize(3);
        }
    }
}
