package org.pangrova.connector.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergPartitionTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.connector.api.metadata.table.ConnectorDerivedQuery;

/** Declares SDK partition statistics as Native relational work over frozen metadata scans. */
final class IcebergPartitionReadBindings {
    private IcebergPartitionReadBindings() {}

    static IcebergPartitionTableHandle select(IcebergCatalogTables tables, IcebergTableHandle base, Table live) {
        Table frozen = IcebergFileReadBindings.frozen(live);
        var metadata = ((HasTableOperations) frozen).operations().current();
        var entries = IcebergFileReadBindings.selectEntries(base, frozen);
        String location = metadata.metadataFileLocation();
        var snapshots = new IcebergSystemTableHandle(base, IcebergSystemTableHandle.Kind.SNAPSHOTS,
                location, frozen.io().newInputFile(location).getLength());
        var inputs = List.of(
                new ConnectorDerivedQuery.Input("pg_entries", IcebergReadBindings.bind(tables, entries)),
                new ConnectorDerivedQuery.Input("pg_snapshots", IcebergReadBindings.bind(tables, snapshots)));
        var partition = Partitioning.partitionType(frozen);
        var schema = MetadataTableUtils.createMetadataTableInstance(frozen, MetadataTableType.PARTITIONS).schema();
        return new IcebergPartitionTableHandle(base, IcebergTypeMapper.toPangrovaColumns(schema),
                new ConnectorDerivedQuery(query(entries, partition), inputs));
    }

    private static String query(IcebergFileTableHandle entries, Types.StructType partition) {
        boolean partitioned = !partition.fields().isEmpty();
        String source = "SELECT e.data_file AS f, s.snapshot_id AS retained_id, s.committed_at, 0 AS bucket "
                + "FROM pg_entries e LEFT JOIN pg_snapshots s ON e.snapshot_id = s.snapshot_id WHERE e.status <> 2";
        var aggregates = new ArrayList<String>();
        if (partitioned) aggregates.add("ANY_VALUE(l.f.\"partition\") AS \"partition\"");
        aggregates.add("MAX_BY(ROW(\"spec_id\" => l.f.spec_id, \"snapshot_id\" => l.retained_id), "
                + "l.committed_at) AS chosen");
        aggregates.add("MAX(l.committed_at) AS last_updated_at");
        aggregates.add(sum(0, "record_count", "record_count"));
        aggregates.add(count(0, "file_count"));
        aggregates.add(sum(0, "file_size_in_bytes", "total_data_file_size_in_bytes"));
        aggregates.add(sum(1, "record_count", "position_delete_record_count"));
        aggregates.add(count(1, "position_delete_file_count"));
        aggregates.add(sum(2, "record_count", "equality_delete_record_count"));
        aggregates.add(count(2, "equality_delete_file_count"));
        String grouping = partitioned ? grouping(entries, partition) : "l.bucket";
        String aggregate = "SELECT " + String.join(", ", aggregates) + " FROM (" + source
                + ") l GROUP BY " + grouping;
        String prefix = partitioned ? "g.\"partition\", COALESCE(g.chosen.\"spec_id\", 0) AS spec_id, " : "";
        return "SELECT " + prefix + "g.record_count, g.file_count, g.total_data_file_size_in_bytes, "
                + "g.position_delete_record_count, g.position_delete_file_count, "
                + "g.equality_delete_record_count, g.equality_delete_file_count, "
                + "g.last_updated_at, g.chosen.\"snapshot_id\" AS last_updated_snapshot_id FROM (" + aggregate + ") g";
    }

    private static String grouping(IcebergFileTableHandle entries, Types.StructType partition) {
        String presence = new java.util.TreeMap<>(entries.manifestSelection().partitionSummarySpecs())
                .entrySet().stream().map(spec -> " WHEN " + spec.getKey() + " THEN "
                        + spec.getValue().stream().filter(field -> !field.name().isEmpty()).count())
                .collect(Collectors.joining("", "CASE l.f.spec_id", " END"));
        var keys = new ArrayList<String>();
        keys.add(presence);
        for (var field : partition.fields()) {
            String value = "l.f.\"partition\"." + quote(field.name());
            String key = switch (field.type().typeId()) {
                case FLOAT -> "TO_IEEE754_32(" + value + ")";
                case DOUBLE -> "TO_IEEE754_64(" + value + ")";
                default -> value;
            };
            keys.add(key);
        }
        return String.join(", ", keys);
    }

    private static String sum(int content, String field, String name) {
        return "SUM(CASE WHEN l.f.content = " + content + " THEN l.f." + field + " ELSE 0 END) AS " + name;
    }

    private static String count(int content, String name) {
        return "CAST(SUM(CASE WHEN l.f.content = " + content + " THEN 1 ELSE 0 END) AS INTEGER) AS " + name;
    }

    private static String quote(String name) { return "\"" + name.replace("\"", "\"\"") + "\""; }
}
