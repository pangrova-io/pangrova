package org.pangrova.connector.paimon;

import java.nio.file.Path;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.paimon.write.PaimonPhysicalWriteAllocations;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.protocol.write.PhysicalFileWriteBindingFactory;
import org.pangrova.protocol.write.NativeWriteBinder;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.connector.api.metadata.NativePhysicalWriteAllocation;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual SDK path and immutable source-file allocation, never a synthesized successful write. */
class PaimonPhysicalAllocationIntegrationTest {
    @TempDir Path directory;

    @Test
    void allocationDoesNotCreateAnObjectAndRestoresActualSdkSequenceFromTheBoundSnapshot() throws Exception {
        try (var catalog = catalog()) {
            var table = table(catalog);
            var empty = PaimonPhysicalWriteAllocations.append(table, -1);
            assertThat(empty.baseSnapshotId()).isEqualTo(-1);
            assertThat(empty.files().get(0).restoredMaxSequence()).isEqualTo(-1);
            assertThat(table.fileIO().exists(new org.apache.paimon.fs.Path(
                    empty.files().get(0).location().opaqueLocation()))).isFalse();
            write(table, 7, 8);
            long snapshot = table.snapshotManager().latestSnapshotId();
            var files = table.newReadBuilder().newScan().plan().splits().stream()
                    .flatMap(split -> ((DataSplit) split).dataFiles().stream()).toList();
            var allocation = PaimonPhysicalWriteAllocations.append(table, snapshot);
            assertThat(allocation.files().get(0).restoredMaxSequence())
                    .isEqualTo(DataFileMeta.getMaxSequenceNumber(files));
            assertThat(allocation.schema().schemaId()).isEqualTo(table.schema().id());
            assertThat(allocation.policy().bucketKeyFields()).containsExactly(table.schema().fields().get(0).id());
            assertThat(allocation.files().get(0).location().fileName()).endsWith(".lance");
            assertThat(table.fileIO().exists(new org.apache.paimon.fs.Path(
                    allocation.files().get(0).location().opaqueLocation()))).isFalse();
            assertPersistedAllocation(table, allocation);
            write(table, 9, 10);
            var pinned = PaimonPhysicalWriteAllocations.append(table, snapshot);
            assertThat(pinned.files().get(0).restoredMaxSequence())
                    .isEqualTo(allocation.files().get(0).restoredMaxSequence());
            assertThat(pinned.files().get(0).restoredFileSetSha256())
                    .isEqualTo(allocation.files().get(0).restoredFileSetSha256());
            assertThat(pinned.baseSnapshotId()).isEqualTo(snapshot);
        }
    }

    /** Tests only Java allocation/handle/template authority, not the still-unsupported LIST→Native VECTOR boundary. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void propertyVectorJavaAllocationAndTemplateRetainDimensionWithoutClaimingNativeExecution(
            boolean doubled) throws Exception {
        try (var catalog = catalog()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "allocation");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("embedding", DataTypes.ARRAY(doubled ? DataTypes.DOUBLE() : DataTypes.FLOAT()))
                    .option("pangrova.vector.dimension.embedding", "7")
                    .option("bucket", "1").option("bucket-key", "id")
                    .option("lance.native.encoder-cache-bytes", "65536")
                    .option("file.format", "lance").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            var allocation = PaimonPhysicalWriteAllocations.append(table, -1);
            var column = allocation.schema().columns().get(1);
            assertThat(column.fieldId()).isEqualTo(table.schema().fields().get(1).id());
            assertThat(column.logicalType()).isEqualTo(new org.pangrova.types.VectorType(7,
                    doubled ? org.pangrova.types.enums.VectorElementType.DOUBLE
                            : org.pangrova.types.enums.VectorElementType.FLOAT).getTypeSignature());
            assertThat(column.nullable()).isTrue();
            assertThat(column.declaredType().children()).singleElement()
                    .satisfies(child -> assertThat(child.nullable()).isTrue());
            var template = PhysicalFileWriteBindingFactory.template(allocation);
            assertThat(template.getColumns(1).getLogicalType()).isEqualTo(
                    org.pangrova.protocol.type.VectorLogicalType.create(7, doubled
                            ? org.pangrova.protocol.type.VectorLogicalType.FLOAT64
                            : org.pangrova.protocol.type.VectorLogicalType.FLOAT32, true, true));
            assertThat(table.fileIO().exists(new org.apache.paimon.fs.Path(
                    allocation.files().get(0).location().opaqueLocation()))).isFalse();
            assertPersistedAllocation(table, allocation);
        }
    }

    private static void assertPersistedAllocation(FileStoreTable table, NativePhysicalWriteAllocation allocation) {
        var output = PaimonTableHandleFactory.outputFromTable("db", "allocation", table)
                .withBaseSnapshotId(allocation.baseSnapshotId()).withPhysicalAllocation(allocation);
        var codec = new PaimonOutputHandleCodec();
        byte[] persisted = codec.serialize(output);
        var restored = (PaimonOutputTableHandle) codec.deserialize(codec.codecVersion(), persisted);
        assertThat(codec.serialize(restored)).isEqualTo(persisted);
        var template = PhysicalFileWriteBindingFactory.template(restored.nativePhysicalWriteAllocation().orElseThrow());
        assertThat(template).isEqualTo(PhysicalFileWriteBindingFactory.template(allocation));
        var attempt = TaskAttemptId.newBuilder().setQueryOrJobId("physical-allocation")
                .setTaskId("write-task").setPlanEpoch(1).setControlEpoch(2).setFencingToken(3)
                .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId("coordinator")
                        .setOwnerIncarnation("incarnation").setFencingToken(4)).build();
        var bound = NativeWriteBinder.bindPhysical(template, attempt);
        var file = bound.getFiles(0);
        assertThat(file.getTaskAttempt()).isEqualTo(attempt);
        assertThat(file.getOpaqueLocation()).startsWith(template.getAllocationPrefix() + "/")
                .endsWith("/" + file.getFileName());
        assertThat(file.getFileName()).startsWith(template.getFiles(0).getFileName().replace(".lance", ".attempt-"));
        assertThat(file.getRestoredMaxSequence()).isEqualTo(template.getFiles(0).getRestoredMaxSequence());
        assertThat(file.getRestoredFileSetChecksum()).isEqualTo(template.getFiles(0).getRestoredFileSetChecksum());
        assertThat(file.getAllocationChecksum()).isNotEqualTo(template.getFiles(0).getAllocationChecksum());
        assertThat(NativeWriteBinder.bindPhysical(template, attempt.toBuilder().setAttemptId(1).build())
                .getFiles(0).getOpaqueLocation()).isNotEqualTo(file.getOpaqueLocation());
    }

    private Catalog catalog() {
        Options options = new Options();
        options.setString("warehouse", directory.toString());
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    private static FileStoreTable table(Catalog catalog) throws Exception {
        catalog.createDatabase("db", false);
        var id = Identifier.create("db", "allocation");
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "1").option("bucket-key", "id")
                .option("lance.native.encoder-cache-bytes", "65536")
                .option("file.format", "lance").build(), false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private static void write(FileStoreTable table, long first, long second) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(GenericRow.of(first));
            writer.write(GenericRow.of(second));
            commit.commit(writer.prepareCommit());
        }
    }
}
