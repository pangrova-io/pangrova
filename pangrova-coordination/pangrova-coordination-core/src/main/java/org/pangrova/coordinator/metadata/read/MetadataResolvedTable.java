package org.pangrova.coordinator.metadata.read;

import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Objects;

/**
 * Represents a table with typed columns resolved from ConnectorTableMetadata.
 * Provides schema metadata (column names and types) to Calcite's planning
 * infrastructure via {@link #getRowType}.
 *
 * <p>Retains the exact immutable connector declaration used for planning. It
 * exposes no data-reading interface; execution belongs to Native Workers.</p>
 */
final class MetadataResolvedTable extends AbstractTable {

    private final ConnectorTableMetadata metadata;
    private final java.util.function.Supplier<org.pangrova.connector.api.metadata.table.ConnectorReadBinding> binding;

    MetadataResolvedTable(ConnectorTableMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.binding = null;
    }

    MetadataResolvedTable(ConnectorTableMetadata metadata,
            java.util.function.Supplier<org.pangrova.connector.api.metadata.table.ConnectorReadBinding> binding) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.binding = Objects.requireNonNull(binding, "binding");
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return org.pangrova.planner.core.type.ConnectorColumnTypeMapper.rowType(declaration().typedColumns(),
                typeFactory);
    }

    @Override
    public org.apache.calcite.schema.Statistic getStatistic() {
        if (binding == null) return org.apache.calcite.schema.Statistics.UNKNOWN;
        long rowCount = binding.get().statistics().rowCount();
        return rowCount < 0 ? org.apache.calcite.schema.Statistics.UNKNOWN
                : org.apache.calcite.schema.Statistics.of((double) rowCount, java.util.List.of());
    }

    private ConnectorTableMetadata declaration() {
        return binding == null ? metadata : binding.get().declaration().metadata();
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(declaration())) return type.cast(declaration());
        if (binding != null && type.isInstance(binding.get())) return type.cast(binding.get());
        return super.unwrap(type);
    }
}
