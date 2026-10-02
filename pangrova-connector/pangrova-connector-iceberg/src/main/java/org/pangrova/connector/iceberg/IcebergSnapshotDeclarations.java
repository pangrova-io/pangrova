package org.pangrova.connector.iceberg;

import org.apache.iceberg.SnapshotUpdate;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;

import java.util.Map;

/** Snapshot-local declaration evidence uses the existing codec, including its explicit empty set. */
final class IcebergSnapshotDeclarations {
    private IcebergSnapshotDeclarations() {}

    static void apply(SnapshotUpdate<?> update, IcebergOutputTableHandle handle) {
        handle.declarationProperties().forEach(update::set);
    }

    static Map<String, String> properties(Map<String, String> source) {
        return IcebergDeclarationProperties.capture(source);
    }
}
