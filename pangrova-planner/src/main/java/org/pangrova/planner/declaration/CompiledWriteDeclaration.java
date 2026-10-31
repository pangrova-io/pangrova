package org.pangrova.planner.declaration;

import org.pangrova.planner.PlanTable;

import org.apache.calcite.rel.core.TableModify;
import org.pangrova.planner.type.ConnectorColumnTypeMapper;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.coordination.WriteDeclarationSnapshot;

import java.util.Objects;
import java.util.Optional;

/** Captures only the connector metadata retained by this compiled mutation target. */
@Internal
public final class CompiledWriteDeclaration {
    private CompiledWriteDeclaration() {}

    /** Returns unknown for an unbacked planner table; never fetches newer metadata as evidence. */
    public static Optional<WriteDeclarationSnapshot> capture(TableModify modify) {
        return metadata(modify).map(metadata -> {
            var definitions = GeneratedColumnMetadataCodec.decode(metadata.properties());
            var names = PlanTable.require(modify).getQualifiedName();
            var table = new QualifiedTableName(names.get(0), names.get(1), names.get(2));
            return WriteDeclarationSnapshot.capture(table, metadata, definitions.toByteArray(),
                    definitions.getDefinitionsCount() != 0);
        });
    }

    /** Returns the same frozen connector declaration used by mutation expression planning. */
    public static Optional<ConnectorTableMetadata> metadata(TableModify modify) {
        Objects.requireNonNull(modify, "modify");
        var table = PlanTable.require(modify);
        var metadata = table.unwrap(ConnectorTableMetadata.class);
        if (metadata == null) return Optional.empty();
        var names = table.getQualifiedName();
        if (names.size() != 3 || !metadata.columns().equals(
                metadata.typedColumns().stream().map(ConnectorTableMetadata.Column::name).toList())
                || !table.getRowType().equals(
                ConnectorColumnTypeMapper.rowType(metadata.typedColumns(), modify.getCluster().getTypeFactory()))) {
            throw new IllegalArgumentException("Compiled write target lacks exact connector schema identity");
        }
        if (!metadata.table().schema().equals(names.get(1))
                || !metadata.table().table().equals(names.get(2))) {
            throw new IllegalArgumentException("Compiled write target differs from actual connector schema identity");
        }
        return Optional.of(metadata);
    }
}
