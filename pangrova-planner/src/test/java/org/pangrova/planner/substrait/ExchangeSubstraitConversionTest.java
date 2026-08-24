package org.pangrova.planner.substrait;

import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;

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
 * Substrait ExtensionSingleRel with ExchangeRelDetail metadata,
 * making physical distribution explicit in the IR.
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
    void hashExchangeProducesExtensionSingleWithDetail() {
        PangrovaDistribution hashDist = PangrovaDistribution.hash(List.of(0, 2));
        PangrovaExchange exchange = buildExchange(hashDist);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(ExtensionSingle.class);
        ExtensionSingle ext = (ExtensionSingle) result;
        assertThat(ext.getDetail()).isInstanceOf(ExchangeRelDetail.class);
        ExchangeRelDetail detail = (ExchangeRelDetail) ext.getDetail();
        assertThat(detail.distributionType()).isEqualTo("HASH");
        assertThat(detail.hashKeys()).containsExactly(0, 2);
    }

    @Test
    void broadcastExchangeProducesExtensionSingle() {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.BROADCAST);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(ExtensionSingle.class);
        ExchangeRelDetail detail = (ExchangeRelDetail) ((ExtensionSingle) result).getDetail();
        assertThat(detail.distributionType()).isEqualTo("BROADCAST");
        assertThat(detail.hashKeys()).isEmpty();
    }

    @Test
    void singletonExchangeProducesExtensionSingle() {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.SINGLETON);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(ExtensionSingle.class);
        ExchangeRelDetail detail = (ExchangeRelDetail) ((ExtensionSingle) result).getDetail();
        assertThat(detail.distributionType()).isEqualTo("SINGLETON");
    }

    @Test
    void roundRobinExchangeProducesExtensionSingle() {
        PangrovaExchange exchange = buildExchange(PangrovaDistribution.ROUND_ROBIN);

        Rel result = converter.visit(exchange);

        assertThat(result).isInstanceOf(ExtensionSingle.class);
        ExchangeRelDetail detail = (ExchangeRelDetail) ((ExtensionSingle) result).getDetail();
        assertThat(detail.distributionType()).isEqualTo("ROUND_ROBIN");
    }

    @Test
    void exchangeDetailSerializesToJson() {
        var outputType = TypeCreator.NULLABLE.struct(
                List.of(TypeCreator.NULLABLE.I32, TypeCreator.NULLABLE.STRING));
        ExchangeRelDetail detail = new ExchangeRelDetail("HASH", List.of(0), outputType);

        com.google.protobuf.Any proto = detail.toProto();
        assertThat(proto).isNotNull();
        assertThat(proto.getTypeUrl()).contains("StringValue");
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
}
