package org.pangrova.planner.rule;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlanResult;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the planner-side wiring of connector LIMIT pushdown:
 *
 * <ul>
 *   <li>{@link LimitPushdownRule#INSTANCE} is a {@code RelOptRule} the HEP program registers.</li>
 *   <li>The rule fires on {@code LIMIT N} over a {@link ConnectorPushdownTable}-aware scan
 *       and forwards the limit to the table's {@link ConnectorPushdownTable#pushLimit(long)}.</li>
 *   <li>A {@link ConnectorPushdownTable.PushdownResult#RESIDUAL} response keeps the {@code Sort}
 *       in the plan; a {@link ConnectorPushdownTable.PushdownResult#GUARANTEED} response
 *       drops it.</li>
 *   <li>Tables that do not implement {@link ConnectorPushdownTable} are untouched.</li>
 * </ul>
 */
class LimitPushdownRuleTest {

    @BeforeEach
    void setUp() {
        LimitPushdownRule.resetFired();
    }

    @Test
    void rule_isRegisteredAsRelOptRule() {
        assertThat(LimitPushdownRule.INSTANCE)
                .isInstanceOf(org.apache.calcite.plan.RelOptRule.class);
        assertThat(LimitPushdownRule.INSTANCE.toString()).contains("LimitPushdownRule");
    }

    @Test
    void rule_firesOnLimitOverPushdownTable_residual_keepsSort() {
        StubPushdownTable table = new StubPushdownTable(ConnectorPushdownTable.PushdownResult.RESIDUAL);
        PangrovaPlanner planner = new PangrovaPlanner(buildSchema("orders", table), List.of(""));

        PlanResult result = planner.plan("SELECT id FROM orders LIMIT 10");

        assertThat(table.lastLimit.get()).isEqualTo(10L);
        assertThat(LimitPushdownRule.didFire()).isTrue();
        // RESIDUAL: planner keeps the Sort to enforce the limit.
        String plan = result.plan().explain();
        assertThat(plan).containsIgnoringCase("Sort");
    }

    @Test
    void rule_firesOnLimitOverPushdownTable_guaranteed_dropsSort() {
        StubPushdownTable table = new StubPushdownTable(ConnectorPushdownTable.PushdownResult.GUARANTEED);
        PangrovaPlanner planner = new PangrovaPlanner(buildSchema("orders_g", table), List.of(""));

        PlanResult result = planner.plan("SELECT id FROM orders_g LIMIT 25");

        assertThat(table.lastLimit.get()).isEqualTo(25L);
        assertThat(LimitPushdownRule.didFire()).isTrue();
        // GUARANTEED: planner drops the Sort entirely; the connector strictly caps to N rows.
        String plan = result.plan().explain();
        assertThat(plan).doesNotContainIgnoringCase("Sort");
    }

    @Test
    void rule_doesNotFire_whenConnectorRejectsLimit() {
        StubPushdownTable table = new StubPushdownTable(/* result */ null);
        PangrovaPlanner planner = new PangrovaPlanner(buildSchema("orders_r", table), List.of(""));

        PlanResult result = planner.plan("SELECT id FROM orders_r LIMIT 5");

        // The rule attempted to push but the connector returned empty — sort retained.
        assertThat(table.lastLimit.get()).isEqualTo(5L);
        assertThat(LimitPushdownRule.didFire()).isFalse();
        String plan = result.plan().explain();
        assertThat(plan).containsIgnoringCase("Sort");
    }

    @Test
    void rule_doesNotFire_whenTableDoesNotImplementPushdownTable() {
        Table plain = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("id", SqlTypeName.INTEGER)
                        .build();
            }
        };
        PangrovaPlanner planner = new PangrovaPlanner(buildSchema("plain", plain), List.of(""));

        PlanResult result = planner.plan("SELECT id FROM plain LIMIT 7");

        assertThat(LimitPushdownRule.didFire()).isFalse();
        assertThat(result).isNotNull();
    }

    @Test
    void rule_doesNotFire_onOrderByLimit_topNLeftToSeparateRule() {
        StubPushdownTable table = new StubPushdownTable(ConnectorPushdownTable.PushdownResult.GUARANTEED);
        PangrovaPlanner planner = new PangrovaPlanner(buildSchema("orders_t", table), List.of(""));

        // ORDER BY .. LIMIT — top-N, deliberately handled by a different rule (not this one).
        PlanResult result = planner.plan("SELECT id FROM orders_t ORDER BY id LIMIT 3");

        assertThat(LimitPushdownRule.didFire()).isFalse();
        assertThat(table.lastLimit.get()).isEqualTo(0L); // pushLimit never called
        assertThat(result).isNotNull();
    }

    private CalciteSchema buildSchema(String tableName, Table table) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        SchemaPlus schemaPlus = rootSchema.plus();
        schemaPlus.add(tableName, table);
        return rootSchema;
    }

    /** Minimal Calcite {@link Table} that records the limit pushdown attempt. */
    private static final class StubPushdownTable extends AbstractTable
            implements ConnectorPushdownTable {

        private final ConnectorPushdownTable.PushdownResult result;
        private final AtomicLong lastLimit = new AtomicLong(0);

        StubPushdownTable(ConnectorPushdownTable.PushdownResult result) {
            this.result = result;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .build();
        }

        @Override
        public Optional<PushdownResult> pushLimit(long limit) {
            lastLimit.set(limit);
            return Optional.ofNullable(result);
        }
    }
}
