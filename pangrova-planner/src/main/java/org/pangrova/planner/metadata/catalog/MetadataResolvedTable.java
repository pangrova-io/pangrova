package org.pangrova.planner.metadata.catalog;

import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Represents a table with typed columns resolved from ConnectorTableMetadata.
 * Provides schema metadata (column names and types) to Calcite's planning
 * infrastructure via {@link #getRowType}.
 *
 * <p>Retains the exact immutable connector declaration used for planning. It
 * exposes no data-reading interface; execution belongs to Native Workers.</p>
 */
public final class MetadataResolvedTable extends AbstractTable {

    private final ConnectorTableMetadata metadata;
    private final Supplier<ConnectorReadBinding> selectedBinding;
    private final Supplier<ConnectorReadBinding> verifiedBinding;
    private final Supplier<TableStatistics> costingStatistics;

    /** Exposes a supplied declaration without a query-bound statistics source. */
    public MetadataResolvedTable(ConnectorTableMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.selectedBinding = null;
        this.verifiedBinding = null;
        this.costingStatistics = () -> TableStatistics.UNKNOWN;
    }

    private MetadataResolvedTable(Supplier<ConnectorReadBinding> selectedBinding,
            Supplier<ConnectorReadBinding> verifiedBinding, Supplier<TableStatistics> costingStatistics) {
        this.metadata = null;
        this.selectedBinding = Objects.requireNonNull(selectedBinding, "binding");
        this.verifiedBinding = verifiedBinding;
        this.costingStatistics = costingStatistics;
    }

    /** Uses the authenticated binding as the only declaration for this table. */
    public static MetadataResolvedTable fromReadBinding(Supplier<ConnectorReadBinding> binding) {
        return new MetadataResolvedTable(binding, binding, () -> binding.get().statistics());
    }

    static MetadataResolvedTable fromReadBinding(PlanningReadBindings bindings, QualifiedTableName table,
            PlanningReadBindings.TableDiscovery discovery) {
        return new MetadataResolvedTable(() -> bindings.selectedRead(table, discovery),
                () -> bindings.resolve(table, discovery)
                        .orElseThrow(() -> new IllegalStateException("Selected table disappeared")),
                () -> bindings.statistics(table));
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return org.pangrova.planner.type.ConnectorColumnTypeMapper.rowType(declaration().typedColumns(),
                typeFactory);
    }

    @Override
    public org.apache.calcite.schema.Statistic getStatistic() {
        long rows = selectedBinding == null ? -1 : selectedBinding.get().statistics().rowCount();
        return rows < 0 ? org.apache.calcite.schema.Statistics.UNKNOWN
                : org.apache.calcite.schema.Statistics.of((double) rows, java.util.List.of());
    }

    /** Work units observed by the compiler for its already selected snapshot. */
    public long selectedSourceWorkUnits() {
        return selectedBinding == null ? -1 : selectedBinding.get().statistics().sourceWorkUnits();
    }

    /** Returns the immutable planning selection. Execution validates it before enumeration. */
    public java.util.Optional<ConnectorReadBinding> selectedReadBinding() {
        return selectedBinding == null ? java.util.Optional.empty() : java.util.Optional.of(selectedBinding.get());
    }

    /** Provides version-matched costing estimates for a direct column of the selected source. */
    public TableStatistics.ColumnStatistics selectedColumnStatistics(String column) {
        return costingStatistics.get().columnStats().getOrDefault(column, TableStatistics.ColumnStatistics.UNKNOWN);
    }

    private ConnectorTableMetadata declaration() {
        return selectedBinding == null ? metadata : selectedBinding.get().declaration().metadata();
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (!type.isAssignableFrom(ConnectorTableMetadata.class)
                && !type.isAssignableFrom(ConnectorReadBinding.class)) {
            return super.unwrap(type);
        }
        ConnectorReadBinding selected = verifiedBinding == null ? null : verifiedBinding.get();
        var declaration = selected == null ? metadata : selected.declaration().metadata();
        if (type.isInstance(declaration)) return type.cast(declaration);
        if (type.isInstance(selected)) return type.cast(selected);
        return super.unwrap(type);
    }
}
