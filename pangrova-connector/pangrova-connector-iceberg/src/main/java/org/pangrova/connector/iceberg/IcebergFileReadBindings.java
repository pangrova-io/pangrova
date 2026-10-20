package org.pangrova.connector.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.pangrova.connector.iceberg.handle.IcebergFileTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergManifestTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;

/** Pins the child manifests behind Iceberg's exact metadata-table schema. */
final class IcebergFileReadBindings {
    private static final Set<String> NATIVE_COLUMNS = Set.of(
            "content", "file_path", "file_format", "spec_id",
            "record_count", "file_size_in_bytes", "column_sizes", "split_offsets",
            "equality_ids", "sort_order_id", "partition");

    private IcebergFileReadBindings() {}

    static IcebergFileTableHandle select(IcebergTableHandle base, Table table) {
        table = frozen(table);
        IcebergManifestTableHandle list = IcebergManifestReadBindings.select(base, table);
        var schema = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES).schema();
        var columns = IcebergTypeMapper.toPangrovaColumns(schema).stream()
                .filter(column -> NATIVE_COLUMNS.contains(column.name()))
                .toList();
        int expected = NATIVE_COLUMNS.size() - (schema.findField("partition") == null ? 1 : 0);
        if (columns.size() != expected) {
            throw new UnsupportedOperationException(
                    "Iceberg $files Native schema differs from the supported typed projection");
        }
        return new IcebergFileTableHandle(list, childObjects(list, table), columns);
    }

    static IcebergFileTableHandle selectEntries(IcebergTableHandle base, Table table) {
        table = frozen(table);
        IcebergManifestTableHandle list = IcebergManifestReadBindings.select(base, table);
        var schema = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ENTRIES).schema();
        var projection = new ArrayList<>(List.of(
                "status", "snapshot_id", "sequence_number", "file_sequence_number"));
        for (String name : NATIVE_COLUMNS) {
            String path = "data_file." + name;
            if (schema.findField(path) != null) projection.add(path);
        }
        projection.add("data_file.first_row_id");
        var columns = IcebergTypeMapper.toPangrovaColumns(schema.select(projection));
        return new IcebergFileTableHandle(list, childObjects(list, table), columns, true);
    }

    static Table frozen(Table table) {
        if (!(table instanceof HasTableOperations operations)) {
            throw new IllegalStateException("Iceberg file reads require table operations");
        }
        return new BaseTable(new StaticTableOperations(operations.operations().current(), table.io()), table.name());
    }

    static void validate(IcebergFileTableHandle bound, Table table) {
        IcebergManifestReadBindings.validate(bound.manifestSelection(), table);
        if (!bound.manifests().equals(childObjects(bound.manifestSelection(), table))) {
            throw new IllegalStateException("Selected Iceberg child manifests changed or expired");
        }
    }

    private static List<IcebergFileTableHandle.ManifestObject> childObjects(
            IcebergManifestTableHandle list, Table table) {
        if (list.empty()) {
            return List.of();
        }
        Snapshot snapshot = table.snapshot(list.snapshotId().orElseThrow());
        if (snapshot == null) {
            throw new IllegalStateException("Selected Iceberg snapshot expired");
        }
        List<IcebergFileTableHandle.ManifestObject> children = new ArrayList<>();
        for (ManifestFile manifest : snapshot.allManifests(table.io())) {
            InputFile input = table.io().newInputFile(manifest.path());
            long length = input.getLength();
            if (length <= 0 || length != manifest.length()) {
                throw new IllegalStateException("Selected Iceberg child manifest length changed");
            }
            children.add(new IcebergFileTableHandle.ManifestObject(
                    manifest.path(), length, IcebergManifestReadBindings.digest(input, length),
                    manifest.partitionSpecId(), manifest.content().id()));
        }
        IcebergManifestReadBindings.validate(list, table);
        return List.copyOf(children);
    }
}
