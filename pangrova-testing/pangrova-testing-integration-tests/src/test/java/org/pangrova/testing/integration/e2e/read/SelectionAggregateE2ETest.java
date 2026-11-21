package org.pangrova.testing.integration.e2e.read;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real table scans keep selection aggregates in Native execution, not literal-folded Java plans. */
class SelectionAggregateE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "selection_aggregates";
    private String table;

    @Override protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeEach void createSource() {
        table = CATALOG + ".default.selection_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + table
                + " (group_id INT, source_id BIGINT, spec_id INT, sort_key BIGINT, payload VARCHAR)");
    }

    @AfterEach void drainAndDropSource() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(queryManager.getActiveQueryIds()).isEmpty();
            assertThat(queryManager.getActiveQueryCount()).isZero();
        });
        sqlRunner.executeDdl("DROP TABLE IF EXISTS " + table);
    }

    @ParameterizedTest
    @ValueSource(strings = {"MAX_BY", "MIN_BY", "ARG_MAX", "ARG_MIN"})
    void returnsOneCompleteRecordAndPreservesGroupedNullKeys(String function) {
        sqlRunner.execute("INSERT INTO " + table
                + " VALUES (1, 101, 9, 1, 'first'), (1, 202, 3, 2, 'second'), (2, 303, 7, NULL, 'unknown')");
        boolean maximum = function.contains("MAX");
        var scalar = sqlRunner.execute("SELECT " + function + "(source_id, sort_key) AS chosen FROM " + table);
        assertThat(scalar).singleElement().satisfies(row ->
                assertThat(row.getLong("chosen")).isEqualTo(maximum ? 202 : 101));
        var grouped = sqlRunner.execute("SELECT group_id, " + function
                + "(ROW(source_id, spec_id), sort_key) AS chosen FROM " + table + " GROUP BY group_id");
        assertThat(grouped).hasSize(2).allSatisfy(row -> {
            if (row.getInt("group_id") == 2) {
                assertThat(row.getRaw("chosen")).isNull();
            } else {
                var chosen = (Map<?, ?>) row.getRaw("chosen");
                assertThat(new ArrayList<Object>(chosen.values())).containsExactlyElementsOf(
                        maximum ? List.of(202L, 3) : List.of(101L, 9));
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"MAX_BY", "MIN_BY"})
    void nullPayloadTiesCompositeKeysAndEmptyInputFollowTheNativeContract(String function) {
        boolean maximum = function.equals("MAX_BY");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 101, 9, 7, "
                + (maximum ? "'known'" : "NULL") + "), (1, 202, 3, 7, "
                + (maximum ? "NULL" : "'known'") + ")");
        var tied = sqlRunner.execute("SELECT " + function
                + "(ROW(source_id, spec_id), sort_key) AS chosen FROM " + table).get(0);
        var pair = new ArrayList<>(((Map<?, ?>) tied.getRaw("chosen")).values());
        assertThat(pair.equals(List.of(101L, 9)) || pair.equals(List.of(202L, 3))).isTrue();
        var selected = sqlRunner.execute("SELECT " + function
                + "(payload, ROW(sort_key, source_id)) AS chosen FROM " + table);
        assertThat(selected).singleElement().satisfies(row -> assertThat(row.getRaw("chosen")).isNull());
        var empty = sqlRunner.execute("SELECT " + function
                + "(ROW(source_id, spec_id), sort_key) AS chosen FROM " + table + " WHERE group_id = 99");
        assertThat(empty).singleElement().satisfies(row -> assertThat(row.getRaw("chosen")).isNull());
        assertThat(sqlRunner.execute("SELECT group_id, " + function
                + "(source_id, sort_key) AS chosen FROM " + table
                + " WHERE group_id = 99 GROUP BY group_id")).isEmpty();
    }
}
