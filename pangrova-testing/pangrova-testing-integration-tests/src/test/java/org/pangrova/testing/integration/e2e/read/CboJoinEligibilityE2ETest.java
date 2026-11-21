package org.pangrova.testing.integration.e2e.read;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SQL/Native regression for CBO plans containing two different join conditions. */
class CboJoinEligibilityE2ETest extends SqlE2ETestBase {

    private static final String PREFIX = "iceberg_catalog.default.cbo_join_eligibility_";
    private static final List<String> CREATED_INPUTS = new ArrayList<>();

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                "iceberg_catalog", "default", icebergWarehouse().toString()));
    }

    @BeforeAll
    static void createInputs() {
        isDockerReady();
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                "iceberg_catalog", "default", icebergWarehouse().toString())));
        for (String name : List.of("a", "b", "c")) {
            sqlRunner.executeDdl("CREATE TABLE " + PREFIX + name + " (id BIGINT)");
            CREATED_INPUTS.add(name);
        }
        sqlRunner.execute("INSERT INTO " + PREFIX + "a VALUES (1), (20), (20), (NULL)");
        sqlRunner.execute("INSERT INTO " + PREFIX + "b VALUES (10), (20), (20), (NULL)");
        sqlRunner.execute("INSERT INTO " + PREFIX + "c VALUES (20), (20), (30), (NULL)");
    }

    @AfterAll
    static void dropInputs() {
        for (String name : CREATED_INPUTS) {
            sqlRunner.executeDdl("DROP TABLE " + PREFIX + name);
        }
        CREATED_INPUTS.clear();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aggregateCountGuardPreservesEmptyAndNonEmptySum(boolean empty) {
        var rows = sqlRunner.execute("SELECT SUM(id) AS total FROM " + PREFIX + "a"
                + (empty ? " WHERE id > 100" : ""));
        assertThat(rows).singleElement().satisfies(row ->
                assertThat(row.getRaw("total")).isEqualTo(empty ? null : 41L));
    }

    enum Shape { CROSS, THETA, EQUALITY_WITH_RESIDUAL }

    @ParameterizedTest
    @EnumSource(Shape.class)
    void cboJoinsPreserveCompleteResultsIncludingDuplicatesAndNulls(Shape shape) {
        String predicate = switch (shape) {
            case CROSS -> "bb.id = cc.id";
            case THETA -> "aa.id < bb.id AND bb.id = cc.id";
            case EQUALITY_WITH_RESIDUAL -> "aa.id = bb.id AND bb.id = cc.id AND aa.id + bb.id > cc.id";
        };
        var rows = sqlRunner.execute("SELECT aa.id AS a_id, bb.id AS b_id, cc.id AS c_id FROM "
                + PREFIX + "a aa, " + PREFIX + "b bb, " + PREFIX + "c cc WHERE " + predicate);
        List<List<Object>> expected = new ArrayList<>();
        for (Long value : Arrays.asList(1L, 20L, 20L, null)) {
            boolean selected = switch (shape) {
                case CROSS -> true;
                case THETA -> value != null && value < 20;
                case EQUALITY_WITH_RESIDUAL -> value != null && value == 20;
            };
            if (selected) {
                // Two matching rows on each of B and C produce four copies per A row.
                for (int copy = 0; copy < 4; copy++) {
                    expected.add(Arrays.asList(value, 20L, 20L));
                }
            }
        }
        assertThat(rows).extracting(row -> Arrays.asList(
                row.getRaw("a_id"), row.getRaw("b_id"), row.getRaw("c_id")))
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
