package org.pangrova.planner.cost.distribution;

import java.math.BigDecimal;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.PangrovaRelOptCost;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.HashJoinConfig;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for architecture-remediation #117: shuffle network cost was double
 * counted across {@link PangrovaHashJoin} (which charged network for HASH/BROADCAST
 * distribution traits) and the {@link PangrovaExchange} enforcer (which used a
 * hardcoded {@code NETWORK_COST_PER_BYTE = 0.01}).
 *
 * <p>After the fix, network cost is charged in exactly one place — the Exchange
 * enforcer — and its magnitude is sourced from {@link CostModelConfig#networkCostPerByte()}.
 */
class ShuffleNetworkCostSingleCountTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private VolcanoPlanner planner;
    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private CostModelConfig config;

    @BeforeEach
    void setUp() {
        config = CostModelConfig.SSD_DEFAULT;
        PangrovaCostFactory costFactory = new PangrovaCostFactory(config);
        planner = new VolcanoPlanner(costFactory, null);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);

        rexBuilder = new RexBuilder(TYPE_FACTORY);
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    @Test
    void hashJoin_withHashDistributionTrait_chargesZeroNetwork() {
        PangrovaHashJoin join = buildHashJoin(1000, 5000, PangrovaDistribution.HASH);
        RelMetadataQuery mq = cluster.getMetadataQuery();

        PangrovaRelOptCost cost = (PangrovaRelOptCost) join.computeSelfCost(planner, mq);

        assertThat(cost.getNetwork())
                .as("HASH-distributed hash join must not charge network; "
                        + "the Exchange enforcer owns shuffle cost")
                .isEqualTo(0.0);
        assertThat(cost.getMemory()).isGreaterThan(0.0);
    }

    @Test
    void hashJoin_withBroadcastDistributionTrait_chargesZeroNetwork() {
        PangrovaHashJoin join = buildHashJoin(1000, 5000, PangrovaDistribution.BROADCAST);
        RelMetadataQuery mq = cluster.getMetadataQuery();

        PangrovaRelOptCost cost = (PangrovaRelOptCost) join.computeSelfCost(planner, mq);

        assertThat(cost.getNetwork())
                .as("BROADCAST-distributed hash join must not charge network")
                .isEqualTo(0.0);
        assertThat(cost.getMemory()).isGreaterThan(0.0);
    }

    @Test
    void exchange_networkCostSourcedFromConfig_notHardcoded() {
        RelNode input = buildInput(3, 1000);
        RelTraitSet traitSet = cluster.traitSet().replace(PangrovaDistribution.HASH);
        PangrovaExchange exchange = new PangrovaExchange(cluster, traitSet, input,
                PangrovaDistribution.HASH);
        RelMetadataQuery mq = cluster.getMetadataQuery();

        double rows = mq.getRowCount(input);
        Double avgRowSize = mq.getAverageRowSize(input);
        double rowWidth = avgRowSize != null ? avgRowSize : 100.0;
        double dataBytes = rows * rowWidth;
        double expectedNetwork = dataBytes * config.networkCostPerByte();

        PangrovaRelOptCost cost = (PangrovaRelOptCost) exchange.computeSelfCost(planner, mq);

        assertThat(cost.getNetwork())
                .as("Exchange network cost must come from config().networkCostPerByte()")
                .isEqualTo(expectedNetwork);
        // Guard against regressing to the old hardcoded 0.01 per byte.
        assertThat(cost.getNetwork())
                .as("Exchange must not use the legacy hardcoded 0.01 per byte")
                .isNotEqualTo(dataBytes * 0.01);
    }

    @Test
    void sameShuffle_countedExactlyOnce_byExchangeOnly() {
        // The hash join carries HASH distribution AND its enforced child shuffle is an
        // Exchange with the same distribution. Network for the shuffle must be the
        // Exchange's cost only — the join contributes zero.
        PangrovaHashJoin join = buildHashJoin(1000, 5000, PangrovaDistribution.HASH);
        RelNode input = buildInput(3, 1000);
        RelTraitSet traitSet = cluster.traitSet().replace(PangrovaDistribution.HASH);
        PangrovaExchange exchange = new PangrovaExchange(cluster, traitSet, input,
                PangrovaDistribution.HASH);
        RelMetadataQuery mq = cluster.getMetadataQuery();

        PangrovaRelOptCost joinCost = (PangrovaRelOptCost) join.computeSelfCost(planner, mq);
        PangrovaRelOptCost exchangeCost = (PangrovaRelOptCost) exchange.computeSelfCost(planner, mq);

        double rows = mq.getRowCount(input);
        Double avgRowSize = mq.getAverageRowSize(input);
        double rowWidth = avgRowSize != null ? avgRowSize : 100.0;
        double expectedNetwork = rows * rowWidth * config.networkCostPerByte();

        double totalNetwork = joinCost.getNetwork() + exchangeCost.getNetwork();
        assertThat(totalNetwork)
                .as("Shuffle network counted exactly once, magnitude from config")
                .isEqualTo(expectedNetwork);
    }

    private PangrovaHashJoin buildHashJoin(int leftRows, int rightRows, PangrovaDistribution dist) {
        RelNode left = buildInput(3, leftRows);
        RelNode right = buildInput(3, rightRows);
        RexNode condition = buildEquiCondition(left, right);
        RelTraitSet joinTraitSet = cluster.traitSet().replace(dist);
        return new PangrovaHashJoin(new HashJoinConfig(
                cluster, joinTraitSet, left, right,
                condition, Set.of(), JoinRelType.INNER));
    }

    private RelNode buildInput(int columnCount, int rowCount) {
        RelDataTypeFactory.Builder typeBuilder = TYPE_FACTORY.builder();
        for (int i = 0; i < columnCount; i++) {
            typeBuilder.add("COL_" + i, SqlTypeName.INTEGER);
        }
        RelDataType rowType = typeBuilder.build();

        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        for (int r = 0; r < rowCount; r++) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            for (int i = 0; i < columnCount; i++) {
                row.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(r * columnCount + i)));
            }
            tuples.add(row.build());
        }
        return LogicalValues.create(cluster, rowType, tuples.build());
    }

    private RexNode buildEquiCondition(RelNode left, RelNode right) {
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(0).getType(), 0);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(0).getType(), leftFieldCount);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
    }
}
