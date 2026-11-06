package org.pangrova.planner.core.substrait;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;

/**
 * A real Calcite RelNode subclass that SubstraitConverter does not recognize.
 * Used to verify the converter correctly rejects unsupported node types.
 */
final class UnsupportedRelNode extends AbstractRelNode {

    UnsupportedRelNode() {
        super(createCluster(), RelTraitSet.createEmpty());
    }

    @Override
    protected RelDataType deriveRowType() {
        return getCluster().getTypeFactory().builder()
                .add("dummy", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
                .build();
    }

    private static RelOptCluster createCluster() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(
                org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        return RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }
}
