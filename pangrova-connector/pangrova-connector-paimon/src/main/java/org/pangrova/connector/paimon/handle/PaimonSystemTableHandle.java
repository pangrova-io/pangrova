package org.pangrova.connector.paimon.handle;

import org.pangrova.common.crypto.Sha256Digests;

import java.util.List;
import java.util.Objects;

import org.pangrova.connector.paimon.enums.PaimonSystemTableKind;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

/** A read-only Paimon metadata relation, bound to exact source objects before scheduling. */
public record PaimonSystemTableHandle(
        PaimonTableHandle baseTable,
        PaimonSystemTableKind kind,
        List<MetadataObject> objects,
        String sourceIdentity) implements ConnectorTableHandle {

    /** One immutable Paimon snapshot or tag JSON object. */
    public record MetadataObject(long snapshotId, String tagName, long schemaId,
            String uri, long length, String sha256) {
        public MetadataObject {
            if (!validIdentity(snapshotId, tagName, schemaId)
                    || uri == null || uri.isBlank() || length <= 0
                    || !Sha256Digests.isSha256Hex(sha256)) {
                throw new IllegalArgumentException("Metadata object requires kind identity, URI, length and SHA-256");
            }
        }

        private static boolean validIdentity(long snapshotId, String tagName, long schemaId) {
            if (snapshotId > 0) {
                return tagName == null && schemaId == -1;
            }
            if (tagName != null) {
                return snapshotId == 0 && !tagName.isBlank() && schemaId == -1;
            }
            return snapshotId == 0 && schemaId >= 0;
        }
    }

    public PaimonSystemTableHandle {
        Objects.requireNonNull(baseTable, "baseTable");
        Objects.requireNonNull(kind, "kind");
        objects = List.copyOf(objects);
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        if (sourceIdentity.isBlank() && !objects.isEmpty()) {
            throw new IllegalArgumentException("Unbound metadata relation cannot carry objects");
        }
        for (MetadataObject object : objects) {
            if ((kind == PaimonSystemTableKind.SNAPSHOTS && object.snapshotId() <= 0)
                    || (kind == PaimonSystemTableKind.TAGS && object.tagName() == null)
                    || ((kind == PaimonSystemTableKind.OPTIONS || kind == PaimonSystemTableKind.SCHEMAS)
                            && object.schemaId() < 0)) {
                throw new IllegalArgumentException("Paimon metadata object does not match relation kind");
            }
        }
    }

    /** Selector produced during relation lookup, before the authorized read is bound. */
    public static PaimonSystemTableHandle selector(PaimonTableHandle baseTable, PaimonSystemTableKind kind) {
        return new PaimonSystemTableHandle(baseTable, kind, List.of(), "");
    }

    public boolean bound() {
        return !sourceIdentity.isBlank();
    }

    public String tableName() {
        return baseTable.tableName() + "$" + kind.suffix();
    }

    /** Paimon 1.4.1 system-table columns, independent of base-table columns. */
    public List<ConnectorTableMetadata.Column> typedColumns() {
        if (kind == PaimonSystemTableKind.SCHEMAS) {
            return List.of(
                    new ConnectorTableMetadata.Column("schema_id", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("fields", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("partition_keys", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("primary_keys", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("options", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("comment", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("update_time", PangrovaType.TIMESTAMP));
        }
        if (kind == PaimonSystemTableKind.OPTIONS) {
            return List.of(
                    new ConnectorTableMetadata.Column("key", PangrovaType.VARCHAR),
                    new ConnectorTableMetadata.Column("value", PangrovaType.VARCHAR));
        }
        if (kind == PaimonSystemTableKind.TAGS) {
            return List.of(
                new ConnectorTableMetadata.Column("tag_name", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("snapshot_id", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("schema_id", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("commit_time", PangrovaType.TIMESTAMP),
                new ConnectorTableMetadata.Column("record_count", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("create_time", PangrovaType.TIMESTAMP),
                new ConnectorTableMetadata.Column("time_retained", PangrovaType.VARCHAR));
        }
        return List.of(
                new ConnectorTableMetadata.Column("snapshot_id", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("schema_id", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("commit_user", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("commit_identifier", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("commit_kind", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("commit_time", PangrovaType.TIMESTAMP),
                new ConnectorTableMetadata.Column("base_manifest_list", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("delta_manifest_list", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("changelog_manifest_list", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("total_record_count", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("delta_record_count", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("changelog_record_count", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("watermark", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("next_row_id", PangrovaType.BIGINT));
    }
}
