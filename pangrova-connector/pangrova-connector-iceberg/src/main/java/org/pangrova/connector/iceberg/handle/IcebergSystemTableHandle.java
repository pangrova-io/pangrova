package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import org.pangrova.types.MapType;
import org.pangrova.types.primitive.VarcharType;
import org.pangrova.types.primitive.temporal.TimestampType;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

/** Immutable, read-only reference to one Iceberg metadata-table version. */
public record IcebergSystemTableHandle(
        IcebergTableHandle baseTable,
        Kind kind,
        String metadataLocation,
        long metadataLength) implements ConnectorTableHandle {

    /** Metadata tables supported by the Native JSON reader. */
    public enum Kind {
        SNAPSHOTS("snapshots"),
        HISTORY("history"),
        REFS("refs");

        private final String suffix;

        Kind(String suffix) {
            this.suffix = suffix;
        }

        public String suffix() {
            return suffix;
        }

        /** Resolves a supported suffix without accepting a base-table fallback. */
        public static Optional<Kind> fromSuffix(String suffix) {
            if (suffix == null) {
                return Optional.empty();
            }
            String normalized = suffix.toLowerCase(Locale.ROOT);
            for (Kind kind : values()) {
                if (kind.suffix.equals(normalized)) {
                    return Optional.of(kind);
                }
            }
            return Optional.empty();
        }
    }

    public IcebergSystemTableHandle {
        Objects.requireNonNull(baseTable, "baseTable");
        Objects.requireNonNull(kind, "kind");
        if (metadataLocation == null || metadataLocation.isBlank()) {
            throw new IllegalArgumentException("metadataLocation must be nonblank");
        }
        if (metadataLength <= 0) {
            throw new IllegalArgumentException("metadataLength must be positive");
        }
    }

    public String tableName() {
        return baseTable.tableName() + "$" + kind.suffix();
    }

    /** Exact public metadata schema, shared by catalog validation and split read. */
    public List<ConnectorTableMetadata.Column> typedColumns() {
        return switch (kind) {
            case SNAPSHOTS -> List.of(
                    new ConnectorTableMetadata.Column("committed_at", PangrovaType.TIMESTAMP_TZ,
                            OptionalInt.of(1), TimestampType.WITH_TIMEZONE),
                    new ConnectorTableMetadata.Column("snapshot_id", PangrovaType.BIGINT, OptionalInt.of(2)),
                    new ConnectorTableMetadata.Column("parent_id", PangrovaType.BIGINT, OptionalInt.of(3)),
                    new ConnectorTableMetadata.Column("operation", PangrovaType.VARCHAR, OptionalInt.of(4)),
                    new ConnectorTableMetadata.Column("manifest_list", PangrovaType.VARCHAR, OptionalInt.of(5)),
                    new ConnectorTableMetadata.Column("summary", PangrovaType.MAP, OptionalInt.of(6),
                            new MapType(VarcharType.UNBOUNDED, VarcharType.UNBOUNDED)));
            case REFS -> List.of(
                    new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("type", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("snapshot_id", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("max_reference_age_in_ms", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("min_snapshots_to_keep", PangrovaType.INTEGER),
                    new ConnectorTableMetadata.Column("max_snapshot_age_in_ms", PangrovaType.BIGINT));
            case HISTORY -> List.of(
                    new ConnectorTableMetadata.Column("made_current_at", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("snapshot_id", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("parent_id", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("is_current_ancestor", PangrovaType.BOOLEAN));
        };
    }
}
