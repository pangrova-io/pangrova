package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;

import java.util.List;

/** Builds one SDK overwrite from the immutable allocation scope, including an empty static result. */
final class IcebergOverwriteCommit {
    private IcebergOverwriteCommit() {}

    static SnapshotUpdate<?> build(Table table, IcebergOutputTableHandle handle, List<DataFile> files) {
        if (!handle.staticPartitions().isEmpty()) {
            return scoped(table, handle, files);
        }
        if (files.isEmpty()) {
            var overwrite = table.newOverwrite().overwriteByRowFilter(Expressions.alwaysTrue());
            if (handle.baseSnapshotId() != IcebergOutputTableHandle.NO_SNAPSHOT) {
                overwrite.validateFromSnapshot(handle.baseSnapshotId());
                overwrite.validateNoConflictingData();
                overwrite.validateNoConflictingDeletes();
            }
            return overwrite;
        }
        var overwrite = table.newReplacePartitions();
        if (handle.baseSnapshotId() != IcebergOutputTableHandle.NO_SNAPSHOT) {
            overwrite.validateFromSnapshot(handle.baseSnapshotId());
            overwrite.validateNoConflictingData();
            overwrite.validateNoConflictingDeletes();
        }
        files.forEach(overwrite::addFile);
        return overwrite;
    }

    private static SnapshotUpdate<?> scoped(Table table, IcebergOutputTableHandle handle, List<DataFile> files) {
        var schema = SchemaParser.fromJson(handle.schemaJson());
        var spec = PartitionSpecParser.fromJson(schema, handle.partitionSpecJson());
        if (!schema.sameSchema(table.schema()) || !spec.equals(table.spec())) {
            throw new IllegalStateException(
                    "Iceberg static overwrite schema or partition spec changed since allocation");
        }
        var filter = IcebergStaticOverwriteScope.filter(schema, spec, handle.staticPartitions());
        var overwrite = table.newOverwrite().overwriteByRowFilter(filter).conflictDetectionFilter(filter);
        // The SDK's added-file check requires at least one added file to determine its partition spec.
        if (!files.isEmpty()) {
            overwrite.validateAddedFilesMatchOverwriteFilter();
        }
        if (handle.baseSnapshotId() != IcebergOutputTableHandle.NO_SNAPSHOT) {
            overwrite.validateFromSnapshot(handle.baseSnapshotId());
            overwrite.validateNoConflictingData();
            overwrite.validateNoConflictingDeletes();
        }
        files.forEach(overwrite::addFile);
        return overwrite;
    }
}
