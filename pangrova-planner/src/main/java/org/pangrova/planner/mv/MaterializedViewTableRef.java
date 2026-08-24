package org.pangrova.planner.mv;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Minimal RelOptTable implementation representing a materialized view.
 * Used by DigestPlanRewriter to create a LogicalTableScan on the MV.
 */
final class MaterializedViewTableRef implements RelOptTable {

    private final String mvName;
    private final RelDataType rowType;

    MaterializedViewTableRef(String mvName, RelDataType rowType) {
        this.mvName = mvName;
        this.rowType = rowType;
    }

    @Override
    public List<String> getQualifiedName() {
        return List.of(mvName);
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public double getRowCount() {
        return 100.0;
    }

    @Override
    public RelOptSchema getRelOptSchema() {
        return null;
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return null;
    }

    @Override
    public List<RelCollation> getCollationList() {
        return List.of();
    }

    @Override
    public RelDistribution getDistribution() {
        return RelDistributions.ANY;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return List.of();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return List.of();
    }

    @Override
    public Expression getExpression(Class clazz) {
        return null;
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return this;
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return List.of();
    }

    @Override
    public <C> C unwrap(Class<C> clazz) {
        return null;
    }
}
