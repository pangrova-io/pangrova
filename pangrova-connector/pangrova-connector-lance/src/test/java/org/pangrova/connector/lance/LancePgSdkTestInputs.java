package org.pangrova.connector.lance;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.protocol.v2.LanceWriteOperation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Actual scalar SDK inputs shared by source catalog, allocation and EMPTY snapshot regression fixtures. */
final class LancePgSdkTestInputs {
    static final Schema SCHEMA = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
    private LancePgSdkTestInputs() {}

    static Prepared write(LanceManagedSourceFixture fixture, Path directory, String uri, String writeId,
            LanceWriteOperation operation, long base, int... values) throws Exception {
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        var input = stream(directory, writeId, values);
        var allocation = sdk.allocate(input, uri, writeId, operation, base, 0, SCHEMA);
        var fragment = sdk.write(input, allocation);
        var pages = sdk.prepare(allocation, List.of(fragment));
        return new Prepared(allocation, sdk, pages);
    }
    static Path stream(Path directory, String name, int... values) throws Exception {
        var path = directory.resolve(name + ".arrow");
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(SCHEMA, allocator);
             var output = Files.newOutputStream(path); var writer = new ArrowStreamWriter(root, null, output)) {
            root.allocateNew();
            var vector = (IntVector) root.getVector(0);
            for (int index = 0; index < values.length; index++) vector.setSafe(index, values[index]);
            root.setRowCount(values.length); writer.start(); writer.writeBatch(); writer.end();
        }
        return path;
    }
    record Prepared(LanceManagedSourceFixture.Allocation allocation, LanceNativeSdkFixture sdk,
                    List<org.pangrova.protocol.v2.PrepareDistributedWriteResult> pages) {
        org.pangrova.protocol.v2.AcceptedSourceSnapshotRef accept() { return sdk.accept(allocation, pages); }
    }
}
