package org.pangrova.coordinator.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.parser.api.ast.AllColumns;
import org.pangrova.parser.api.ast.CreateDynamicTableStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.TableRef;

class DynamicTableRefreshModeMappingTest {

    private static final Identifier NAME = Identifier.of("lake.sales.orders_dt");

    @Test
    void absentModeKeepsFullRefresh() {
        assertThat(KernelDynamicTableService.refreshMode(statement(Optional.empty())))
                .isEqualTo(RefreshMode.FULL);
    }

    @Test
    void snapshotModeMapsToFull() {
        assertThat(KernelDynamicTableService.refreshMode(
                statement(Optional.of(org.pangrova.parser.api.enums.RefreshMode.SNAPSHOT))))
                .isEqualTo(RefreshMode.FULL);
    }

    @Test
    void autoModeMapsToIncremental() {
        assertThat(KernelDynamicTableService.refreshMode(
                statement(Optional.of(org.pangrova.parser.api.enums.RefreshMode.AUTO))))
                .isEqualTo(RefreshMode.INCREMENTAL);
    }

    @Test
    void fullAndIncrementalModesMapDirectly() {
        assertThat(KernelDynamicTableService.refreshMode(
                statement(Optional.of(org.pangrova.parser.api.enums.RefreshMode.FULL))))
                .isEqualTo(RefreshMode.FULL);
        assertThat(KernelDynamicTableService.refreshMode(
                statement(Optional.of(org.pangrova.parser.api.enums.RefreshMode.INCREMENTAL))))
                .isEqualTo(RefreshMode.INCREMENTAL);
    }

    @Test
    void absentStrategyMeansAutomaticSelection() {
        assertThat(KernelDynamicTableService.incrementalStrategy(Optional.empty())).isEmpty();
        assertThat(KernelDynamicTableService.incrementalStrategy(
                Optional.of(org.pangrova.parser.api.enums.IncrementalStrategy.AUTO))).isEmpty();
    }

    @Test
    void pinnedStrategiesMapToKernelStrategies() {
        assertThat(KernelDynamicTableService.incrementalStrategy(
                Optional.of(org.pangrova.parser.api.enums.IncrementalStrategy.APPEND_ONLY)))
                .contains(org.pangrova.materialization.api.domain.StrategyId.APPEND_ONLY);
        assertThat(KernelDynamicTableService.incrementalStrategy(
                Optional.of(org.pangrova.parser.api.enums.IncrementalStrategy.PARTITION_REPLACE)))
                .contains(org.pangrova.materialization.api.domain.StrategyId.PARTITION_REPLACE);
        assertThat(KernelDynamicTableService.incrementalStrategy(
                Optional.of(org.pangrova.parser.api.enums.IncrementalStrategy.AFFECTED_KEY)))
                .contains(org.pangrova.materialization.api.domain.StrategyId.AFFECTED_KEY);
    }

    @Test
    void cdcStrategyIsRejected() {
        assertThatThrownBy(() -> KernelDynamicTableService.incrementalStrategy(
                Optional.of(org.pangrova.parser.api.enums.IncrementalStrategy.CDC)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("CDC");
    }

    @Test
    void cdcModeIsRejected() {
        assertThatThrownBy(() -> KernelDynamicTableService.refreshMode(
                statement(Optional.of(org.pangrova.parser.api.enums.RefreshMode.CDC))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("CDC");
    }

    private static CreateDynamicTableStatement statement(
            Optional<org.pangrova.parser.api.enums.RefreshMode> mode) {
        return new CreateDynamicTableStatement(NAME, query(), Duration.ofMinutes(5),
                mode, Optional.empty(), Map.of());
    }

    private static Query query() {
        return new Query(false, List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of("lake.sales.orders"))),
                Optional.empty(), List.of(), Optional.empty(), List.of(), Optional.empty());
    }
}
