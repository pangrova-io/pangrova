package org.pangrova.planner.substrait;

import io.substrait.relation.Rel;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;

import com.google.common.collect.ImmutableList;

import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that PangrovaExchange operators are correctly converted to
 * the official Substrait physical ExchangeRel variants.
 */
class ExchangeSubstraitConversionTest {

    private PlanNodeConverter converter;
    private RelOptCluster cluster;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(RexUtil.EXECUTOR);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(planner, rexBuilder);

        TypeMapper typeMapper = new TypeMapper();
        FunctionMapper functionMapper = new FunctionMapper();
        converter = new PlanNodeConverter(typeMapper, functionMapper, rexBuilder);
    }

    @Test
    void hashExchangeProducesStandardScatterExchange() {
        PangrovaDistribution hashDist = PangrovaDistribution.hash(List.of(0, 2));
        PangrovaExchange exchange = buildExchange(hashDist);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(ScatterExchange.class);
        ScatterExchange scatter = (ScatterExchange) result;
        assertThat(scatter.getPartitionCount())
                .isEqualTo(PhysicalJoinConverter.UNBOUND_PARTITION_COUNT);
        assertThat(scatter.getFields()).extracting(ExchangeSubstraitConversionTest::fieldOffset)
                .containsExactly(0, 2);
    }

    @Test
    void broadcastExchangeProducesStandardBroadcastExchange() {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.BROADCAST);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(BroadcastExchange.class);
        assertThat(((BroadcastExchange) result).getPartitionCount())
                .isEqualTo(PhysicalJoinConverter.UNBOUND_PARTITION_COUNT);
    }

    @Test
    void singletonExchangeProducesOnePartitionRoundRobin() {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.SINGLETON);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(RoundRobinExchange.class);
        RoundRobinExchange roundRobin = (RoundRobinExchange) result;
        assertThat(roundRobin.getPartitionCount()).isEqualTo(1);
        assertThat(roundRobin.getExact()).isTrue();
    }

    @Test
    void roundRobinExchangeProducesStandardRoundRobin() {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.ROUND_ROBIN);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(RoundRobinExchange.class);
        RoundRobinExchange roundRobin = (RoundRobinExchange) result;
        assertThat(roundRobin.getPartitionCount())
                .isEqualTo(PhysicalJoinConverter.UNBOUND_PARTITION_COUNT);
        assertThat(roundRobin.getExact()).isFalse();
    }

    @Test
    void exchangeSerializesAsOfficialExchangeRel() throws Exception {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.hash(List.of(0)));
        byte[] bytes = new SubstraitPlanSerializer().serialize(
                new SubstraitConverter().convert(exchange));

        io.substrait.proto.Rel rel = io.substrait.proto.Plan.parseFrom(bytes)
                .getRelations(0).getRoot().getInput();
        assertThat(rel.hasExchange()).isTrue();
        assertThat(rel.getExchange().hasScatterByFields()).isTrue();
        assertThat(rel.getExchange().getScatterByFields().getFieldsCount()).isEqualTo(1);
        assertThat(rel.getExchange().hasAdvancedExtension()).isFalse();
    }

    private PangrovaExchange buildExchange(PangrovaDistribution dist) {
        RelDataType rowType = typeFactory.builder()
                .add("id", SqlTypeName.INTEGER)
                .add("name", SqlTypeName.VARCHAR, 50)
                .add("amount", SqlTypeName.DECIMAL, 10, 2)
                .build();

        // Create a minimal input with one row (Substrait VirtualTableScan requires >=1 row).
        RexBuilder rb = cluster.getRexBuilder();
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(
                ImmutableList.of(
                        rb.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.INTEGER)),
                        rb.makeLiteral("x"),
                        rb.makeExactLiteral(BigDecimal.valueOf(100), typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2))));
        LogicalValues valuesInput = LogicalValues.create(cluster, rowType, tuples);

        RelTraitSet traitSet = cluster.traitSet().replace(dist);
        return new PangrovaExchange(cluster, traitSet, valuesInput, dist);
    }

    private static int fieldOffset(io.substrait.expression.FieldReference reference) {
        return ((io.substrait.expression.FieldReference.StructField)
                reference.segments().get(0)).offset();
    }
}
