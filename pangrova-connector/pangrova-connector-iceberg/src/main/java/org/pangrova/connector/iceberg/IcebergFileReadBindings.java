package org.pangrova.connector.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Snapshot;
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
            "record_count", "file_size_in_bytes");

    private IcebergFileReadBindings() {}

    static IcebergFileTableHandle select(IcebergTableHandle base, Table table) {
        IcebergManifestTableHandle list = IcebergManifestReadBindings.select(base, table);
        var columns = IcebergTypeMapper.toPangrovaColumns(
                MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES)
                        .schema()).stream()
                .filter(column -> NATIVE_COLUMNS.contains(column.name()))
                .toList();
        if (columns.size() != NATIVE_COLUMNS.size()) {
            throw new UnsupportedOperationException(
                    "Iceberg $files Native schema differs from the supported scalar projection");
        }
        return new IcebergFileTableHandle(list, childObjects(list, table), columns);
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
