package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates the aggregation-pushdown SPI surface (G-CONN-06): the Trino-style
 * {@link ApplicationResult} guaranteed flag, the {@link AggregationApplication} descriptor,
 * and the {@link ConnectorMetadata#applyAggregation} default + override contract.
 */
class AggregationPushdownTest {

    private record TestColumn(String columnName) implements ColumnHandle {}

    private record TestTable(String name) implements ConnectorTableHandle {}

    /** Minimal ConnectorMetadata implementing only the abstract methods; pushdown uses defaults. */
    private static class StubMetadata implements ConnectorMetadata {
        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of();
        }
    }

    @Test
    void applyAggregationDefaultsToEmpty() {
        ConnectorMetadata metadata = new StubMetadata();
        AggregationApplication agg = new AggregationApplication(
                List.of(new AggregationApplication.AggregateFunction("count", List.of(), false)),
                List.of());

        assertThat(metadata.applyAggregation(null, new TestTable("t"), agg)).isEmpty();
    }

    @Test
    void overridingConnectorReportsGuaranteedResult() {
        ConnectorTableHandle rewritten = new TestTable("t$agg");
        ConnectorMetadata metadata = new StubMetadata() {
            @Override
            public Optional<ApplicationResult<ConnectorTableHandle>> applyAggregation(
                    ConnectorSession session, ConnectorTableHandle table, AggregationApplication aggregation) {
                return Optional.of(ApplicationResult.guaranteed(rewritten));
            }
        };

        Optional<ApplicationResult<ConnectorTableHandle>> result =
                metadata.applyAggregation(null, new TestTable("t"),
                        new AggregationApplication(
                                List.of(new AggregationApplication.AggregateFunction(
                                        "sum", List.of(new TestColumn("amount")), false)),
                                List.of(new TestColumn("region"))));

        assertThat(result).isPresent();
        assertThat(result.get().handle()).isSameAs(rewritten);
        assertThat(result.get().guaranteed()).isTrue();
    }

    @Test
    void residualResultSignalsEngineMustKeepOperator() {
        ConnectorTableHandle handle = new TestTable("t");
        ApplicationResult<ConnectorTableHandle> residual = ApplicationResult.residual(handle);

        assertThat(residual.guaranteed()).isFalse();
        assertThat(residual.handle()).isSameAs(handle);
    }

    @Test
    void applicationResultRejectsNullHandle() {
        assertThatThrownBy(() -> new ApplicationResult<>(null, true))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void globalAggregateHasNoGroupingColumns() {
        AggregationApplication agg = new AggregationApplication(
                List.of(new AggregationApplication.AggregateFunction("count", List.of(), false)),
                List.of());

        assertThat(agg.isGlobal()).isTrue();
        assertThat(agg.groupingColumns()).isEmpty();
    }

    @Test
    void groupedAggregateRetainsGroupingColumns() {
        AggregationApplication agg = new AggregationApplication(
                List.of(new AggregationApplication.AggregateFunction("max", List.of(), false)),
                List.of(new TestColumn("region"), new TestColumn("day")));

        assertThat(agg.isGlobal()).isFalse();
        assertThat(agg.groupingColumns()).hasSize(2);
    }

    @Test
    void aggregationApplicationRejectsEmptyAggregates() {
        assertThatThrownBy(() -> new AggregationApplication(List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aggregateFunctionRejectsBlankName() {
        assertThatThrownBy(() ->
                new AggregationApplication.AggregateFunction("  ", List.of(), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aggregationApplicationIsDefensivelyCopied() {
        List<AggregationApplication.AggregateFunction> aggs = new java.util.ArrayList<>();
        aggs.add(new AggregationApplication.AggregateFunction("count", List.of(), false));
        AggregationApplication agg = new AggregationApplication(aggs, List.of());

        aggs.clear();

        assertThat(agg.aggregates()).hasSize(1);
    }
}
