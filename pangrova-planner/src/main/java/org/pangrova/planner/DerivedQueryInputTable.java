package org.pangrova.planner;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.pangrova.planner.type.ConnectorColumnTypeMapper;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;

/** A definition-local alias becomes an ordinary scan of its exact physical identity. */
final class DerivedQueryInputTable extends AbstractTable implements TranslatableTable {
    private final ConnectorReadBinding binding;
    private final ImmutableList<String> qualifiedName;

    DerivedQueryInputTable(ConnectorReadBinding binding, List<String> catalogPath) {
        this.binding = binding;
        var name = binding.declaration().metadata().table();
        qualifiedName = ImmutableList.<String>builder().addAll(catalogPath)
                .add(name.schema(), name.table()).build();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
        return ConnectorColumnTypeMapper.rowType(binding.declaration().metadata().typedColumns(), factory);
    }

    @Override
    public Statistic getStatistic() {
        long rows = binding.statistics().rowCount();
        return rows < 0 ? Statistics.UNKNOWN : Statistics.of((double) rows, List.of());
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable alias) {
        var physical = RelOptTableImpl.create(alias.getRelOptSchema(),
                getRowType(context.getCluster().getTypeFactory()), this, qualifiedName);
        return LogicalTableScan.create(context.getCluster(), physical, List.of());
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(binding)) return type.cast(binding);
        if (type.isInstance(binding.declaration().metadata())) return type.cast(binding.declaration().metadata());
        return super.unwrap(type);
    }
}
