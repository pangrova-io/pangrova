package org.pangrova.coordinator.metadata;

import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
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

    MetadataResolvedTable(ConnectorTableMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return org.pangrova.planner.type.ConnectorColumnTypeMapper.rowType(metadata.typedColumns(), typeFactory);
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        return type.isInstance(metadata) ? type.cast(metadata) : super.unwrap(type);
    }
}
