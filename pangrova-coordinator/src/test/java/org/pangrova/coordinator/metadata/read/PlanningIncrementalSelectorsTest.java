package org.pangrova.coordinator.metadata.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.spi.connector.metadata.table.SnapshotRange;

/** Trusted incremental-range selectors derived from a proven change set. */
class PlanningIncrementalSelectorsTest {

    @Test
    void trustedRangesBindWhileClientSelectorsAreStripped() {
        var context = new CoordinatorSessionContext("query", Map.of(
                "catalog", "cat",
                "pangrova.source_incremental.cat.db.forged", "1:9"), null, "tenant")
                .withSourceIncrementalRanges(Map.of(
                        PlanningSnapshotSelectors.tableKey("cat", "db", "orders"),
                        new SnapshotRange(7, 9)));

        var session = SessionContextConverter.toConnectorSession(context);

        assertThat(PlanningIncrementalSelectors.range(session, "cat", "db", "orders"))
                .contains(new SnapshotRange(7, 9));
        assertThat(session.getProperties())
                .doesNotContainKey("pangrova.source_incremental.cat.db.forged");
    }

    @Test
    void rangeLookupIsCaseNormalizedAndKeepsLiteralDotsDistinct() {
        var session = SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "query", Map.of(), null, "tenant"));
        var selected = PlanningIncrementalSelectors.bind(session, Map.of(
                PlanningSnapshotSelectors.tableKey("Cat", "A.B", "Rows"),
                new SnapshotRange(2, 5),
                PlanningSnapshotSelectors.tableKey("Cat", "A", "B.Rows"),
                new SnapshotRange(3, 6)));

        assertThat(PlanningIncrementalSelectors.range(selected, "cat", "a.b", "rows"))
                .contains(new SnapshotRange(2, 5));
        assertThat(PlanningIncrementalSelectors.range(selected, "cat", "a", "b.rows"))
                .contains(new SnapshotRange(3, 6));
        assertThat(PlanningIncrementalSelectors.range(selected, "cat", "a", "rows")).isEmpty();
    }

    @Test
    void replacingRangesDoesNotCarryAnEarlierRangeForward() {
        var session = SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "query", Map.of(), null, "tenant"));
        var selected = PlanningIncrementalSelectors.bind(session, Map.of(
                PlanningSnapshotSelectors.tableKey("cat", "db", "first"), new SnapshotRange(1, 2)));
        var replaced = PlanningIncrementalSelectors.bind(selected, Map.of(
                PlanningSnapshotSelectors.tableKey("cat", "db", "second"), new SnapshotRange(3, 4)));

        assertThat(PlanningIncrementalSelectors.range(replaced, "cat", "db", "first")).isEmpty();
        assertThat(PlanningIncrementalSelectors.range(replaced, "cat", "db", "second"))
                .contains(new SnapshotRange(3, 4));
    }

    @Test
    void exactSnapshotsAndRangesForDifferentSourcesCoexist() {
        var context = new CoordinatorSessionContext("query", Map.of("catalog", "cat"), null, "tenant")
                .withSourceSnapshots(Map.of(
                        PlanningSnapshotSelectors.tableKey("cat", "db", "customers"), 11L))
                .withSourceIncrementalRanges(Map.of(
                        PlanningSnapshotSelectors.tableKey("cat", "db", "orders"),
                        new SnapshotRange(7, 9)));

        var session = SessionContextConverter.toConnectorSession(context);

        assertThat(PlanningSnapshotSelectors.snapshot(session, "cat", "db", "customers"))
                .hasValue(11);
        assertThat(PlanningIncrementalSelectors.range(session, "cat", "db", "orders"))
                .contains(new SnapshotRange(7, 9));
    }

    @Test
    void oneSourceCannotBindBothAnExactSnapshotAndARange() {
        String key = PlanningSnapshotSelectors.tableKey("cat", "db", "orders");

        assertThatThrownBy(() -> new CoordinatorSessionContext(
                "query", Map.of(), null, "tenant")
                .withSourceSnapshots(Map.of(key, 7L))
                .withSourceIncrementalRanges(Map.of(key, new SnapshotRange(7, 9))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both");
    }

    @Test
    void alreadyPrefixedKeysAreRejectedBeforeBinding() {
        var session = SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "query", Map.of(), null, "tenant"));

        assertThatThrownBy(() -> PlanningIncrementalSelectors.bind(session, Map.of(
                PlanningIncrementalSelectors.PROPERTY_PREFIX
                        + PlanningSnapshotSelectors.tableKey("cat", "db", "orders"),
                new SnapshotRange(7, 9))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unqualified table key");
    }

    @Test
    void invalidRangesAreRejectedBeforeBinding() {
        var session = SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(
                "query", Map.of(), null, "tenant"));

        assertThatThrownBy(() -> PlanningIncrementalSelectors.bind(session, Map.of(" ", new SnapshotRange(1, 2))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid");
        assertThatThrownBy(() -> new SnapshotRange(2, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differ");
        assertThatThrownBy(() -> SnapshotRange.parse("1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("from:to");
    }
}
