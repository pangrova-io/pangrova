package org.pangrova.connector.paimon;

import org.pangrova.testfixtures.tableformat.PaimonCommitFixture;
import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies native DELETE survivor fragments use full, atomic replacement semantics. */
class PaimonDistributedDeleteCommitTest extends PaimonDeleteFixture {

    private static final StoredRow DELETED = new StoredRow(1, "east", "one", "delete");
    private static final StoredRow SURVIVOR = new StoredRow(2, "west", "two", "keep");
    private static final StoredRow OTHER_SURVIVOR = new StoredRow(3, "north", "one", "also-keep");
    private static final List<String> COLUMNS = List.of("id", "region", "day", "payload");

    @Test
    void ambiguousSnapshotPublicationIsNotRetriedByTheConnector() throws Exception {
        // Exhaust the SDK's own retry budget immediately to test the connector boundary.
        createTable(false, true, Map.of("commit.max-retries", "0"));
        writeRows(DELETED);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new PaimonSplitRegistry());
        var output = (org.pangrova.connector.paimon.handle.PaimonOutputTableHandle)
                metadata.beginOverwrite(SESSION, handle(metadata));
        List<byte[]> fragments = List.of(prepare(output, SURVIVOR));
        var context = PaimonCommitFixture.context(metadata, SESSION, output, fragments);
        var original = (org.apache.paimon.table.FileStoreTable) freshTable();
        var publications = new java.util.concurrent.atomic.AtomicInteger();
        var notifications = new java.util.concurrent.atomic.AtomicInteger();
        var faultyIo = new org.apache.paimon.fs.local.LocalFileIO() {
            @Override
            public boolean rename(org.apache.paimon.fs.Path source, org.apache.paimon.fs.Path destination)
                    throws java.io.IOException {
                if (destination.getName().startsWith("snapshot-")) {
                    publications.incrementAndGet();
                    throw new java.io.IOException("Snapshot publication response lost");
                }
                return super.rename(source, destination);
            }
        };
        var table = new org.apache.paimon.table.AppendOnlyFileStoreTable(faultyIo,
                original.location(), original.schema(), org.apache.paimon.table.CatalogEnvironment.empty());

        var prepared = PaimonDistributedWriteFinisher.deserialize(fragments, output);
        assertThatThrownBy(() -> PaimonDistributedWriteFinisher.finish(table, output, prepared, context,
                () -> table.snapshotManager().latestSnapshotOfUser(context.commitFingerprint()),
                (schema, name, added, removed, snapshot, event, fingerprint) -> notifications.incrementAndGet()))
                .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
                .hasStackTraceContaining("Exception occurs when committing snapshot");
        assertThat(publications).hasValue(1);
        assertThat(notifications).hasValue(0);
        assertThat(readRows()).containsExactly(DELETED);
        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, output, fragments, context))
                .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
                .hasMessageContaining("previously started without a confirmed snapshot");
        assertThat(readRows()).containsExactly(DELETED);
    }

    @ParameterizedTest(name = "survivors: partitioned={0}, appendOnly={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void survivorCommitRemovesPartitionsWithoutOutput(boolean partitioned, boolean appendOnly)
            throws Exception {
        createTable(partitioned, appendOnly);
        writeRows(DELETED, SURVIVOR, OTHER_SURVIVOR);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var output = metadata.beginOverwrite(SESSION, handle(metadata));
        byte[] firstWorker = prepare(output, SURVIVOR);
        byte[] secondWorker = prepare(output, OTHER_SURVIVOR);
        byte[] emptyWorker = prepare(output);

        // Workers only stage files. The old table remains visible until the single commit.
        assertThat(readRows()).containsExactlyInAnyOrder(DELETED, SURVIVOR, OTHER_SURVIVOR);
        PaimonCommitFixture.finish(metadata, SESSION, output,
                List.of(firstWorker, secondWorker, emptyWorker));

        assertThat(readRows()).containsExactlyInAnyOrder(SURVIVOR, OTHER_SURVIVOR);
        assertThat(freshTable().options()).doesNotContainEntry("dynamic-partition-overwrite", "false");
        writeRows(DELETED);
        assertThat(readRows()).containsExactlyInAnyOrder(DELETED, SURVIVOR, OTHER_SURVIVOR);
    }

    @ParameterizedTest(name = "zero survivors: partitioned={0}, appendOnly={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void emptySurvivorFragmentsRemoveAllRows(boolean partitioned, boolean appendOnly) throws Exception {
        createTable(partitioned, appendOnly);
        writeRows(DELETED, SURVIVOR);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var output = metadata.beginOverwrite(SESSION, handle(metadata));
        byte[] emptyWorker = prepare(output);
        assertThat(emptyWorker).isNotEmpty();
        assertThat(readRows()).containsExactlyInAnyOrder(DELETED, SURVIVOR);

        PaimonCommitFixture.finish(metadata, SESSION, output, List.of(emptyWorker));

        assertThat(readRows()).isEmpty();
        var repeatOutput = metadata.beginOverwrite(SESSION, handle(metadata));
        PaimonCommitFixture.finish(metadata, SESSION, repeatOutput,
                List.of(prepare(repeatOutput)));
        assertThat(readRows()).isEmpty();
        writeRows(SURVIVOR);
        assertThat(readRows()).containsExactly(SURVIVOR);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyStaticPartitionCommitKeepsUnselectedPartitions(boolean appendOnly) throws Exception {
        createTable(true, appendOnly);
        writeRows(DELETED, SURVIVOR);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var output = metadata.beginOverwrite(SESSION, handle(metadata), Map.of("region", "east"));

        PaimonCommitFixture.finish(metadata, SESSION, output, List.of(prepare(output)));

        assertThat(readRows()).containsExactly(SURVIVOR);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void appendCommitDoesNotAcquireOverwriteSemantics(boolean partitioned) throws Exception {
        createTable(partitioned, false);
        writeRows(DELETED);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var output = metadata.beginInsert(SESSION, handle(metadata));
        ConnectorPageSink sink = provider().createPageSink(ConnectorTransactionHandle.empty(), SESSION, output);
        byte[] fragment = finish(sink, SURVIVOR);

        PaimonCommitFixture.finish(metadata, SESSION, output, List.of(fragment));

        assertThat(readRows()).containsExactlyInAnyOrder(DELETED, SURVIVOR);
    }

    @Test
    void stagedSdkDataFilesUseTheAllocationFromTheDispatchedHandle() throws Exception {
        createTable(false, false, Map.of("data-file.prefix", "custom-data-"));
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new PaimonSplitRegistry());
        var first = (org.pangrova.connector.paimon.handle.PaimonOutputTableHandle)
                metadata.beginInsert(SESSION, handle(metadata));
        var second = (org.pangrova.connector.paimon.handle.PaimonOutputTableHandle)
                metadata.beginInsert(SESSION, handle(metadata));
        var codec = new org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec();
        var restored = (org.pangrova.connector.paimon.handle.PaimonOutputTableHandle)
                codec.deserialize(codec.codecVersion(), codec.serialize(first));
        assertThat(restored.stagedFileNames()).isEqualTo(first.stagedFileNames());
        assertThat(second.stagedFileNames().allocationId()).isNotEqualTo(first.stagedFileNames().allocationId());
        assertThat(first.stagedFileNames().dataPrefix()).startsWith("custom-data-");
        assertThat(second.stagedFileNames().dataPrefix()).isNotEqualTo(first.stagedFileNames().dataPrefix());
        assertThatThrownBy(() -> codec.deserialize(4, codec.serialize(first)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("version");

        byte[] receipt = finish(provider().createPageSink(ConnectorTransactionHandle.empty(), SESSION, restored),
                SURVIVOR);
        var prepared = org.pangrova.connector.paimon.write.PaimonSdkWriteFragment.decode(receipt, restored);
        assertThat(prepared.messages()).singleElement().satisfies(message -> {
            var increment = ((org.apache.paimon.table.sink.CommitMessageImpl) message).newFilesIncrement();
            assertThat(increment.newFiles()).singleElement().satisfies(file -> {
                assertThat(file.fileName()).startsWith(first.stagedFileNames().dataPrefix());
                assertThat(file.fileName()).doesNotStartWith(second.stagedFileNames().dataPrefix());
            });
        });
        assertThat(prepared.objects()).hasSize(1);
        var object = prepared.objects().get(0);
        var path = java.nio.file.Path.of(java.net.URI.create(object.uri()));
        assertThat(path).isRegularFile();
        assertThat(java.nio.file.Files.size(path)).isEqualTo(object.sizeBytes());
        assertThat(path.getFileName().toString()).startsWith(first.stagedFileNames().dataPrefix());
        assertThatThrownBy(() -> org.pangrova.connector.paimon.write.PaimonSdkWriteFragment.decode(receipt, second))
                .hasMessageContaining("staged file allocation");
        byte[] changedSize = receipt.clone();
        changedSize[changedSize.length - 1] ^= 1;
        assertThatThrownBy(() -> org.pangrova.connector.paimon.write.PaimonSdkWriteFragment.decode(changedSize, restored))
                .hasMessageContaining("differs from its SDK commit metadata");
        assertThat(snapshotId()).isEqualTo(-1L);
    }

    @Test
    void observedStagingReplaysDeletionOnlyInTheOriginalLocalStorageNamespace() throws Exception {
        createTable(false, false);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new PaimonSplitRegistry());
        var handle = (org.pangrova.connector.paimon.handle.PaimonOutputTableHandle)
                metadata.beginInsert(SESSION, handle(metadata));
        var first = finish(provider().createPageSink(ConnectorTransactionHandle.empty(), SESSION, handle), SURVIVOR);
        var second = finish(provider().createPageSink(ConnectorTransactionHandle.empty(), SESSION, handle), SURVIVOR);
        var prior = org.pangrova.connector.paimon.write.PaimonSdkWriteFragment.decode(first, handle);
        var current = org.pangrova.connector.paimon.write.PaimonSdkWriteFragment.decode(second, handle);
        var deleted = java.nio.file.Path.of(java.net.URI.create(prior.objects().get(0).uri()));
        var remaining = java.nio.file.Path.of(java.net.URI.create(current.objects().get(0).uri()));
        assertThat(deleted).isNotEqualTo(remaining);
        java.nio.file.Files.delete(deleted);
        var fragments = List.of(first, second);
        var context = PaimonCommitFixture.context(metadata, SESSION, handle, fragments);
        boolean authoritative = !current.localAuthority().isEmpty();

        assertThat(metadata.reclaimObservedStaging(SESSION, handle, fragments, context)).isEqualTo(authoritative);
        assertThat(java.nio.file.Files.exists(remaining)).isEqualTo(!authoritative);
        assertThat(metadata.reclaimObservedStaging(SESSION, handle, fragments, context)).isEqualTo(authoritative);
        assertThat(snapshotId()).isEqualTo(-1L);
    }

    @Test
    void durableCommitReplayDoesNotPublishDuplicateRowsOrSnapshots() throws Exception {
        createTable(false, false);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var output = metadata.beginInsert(SESSION, handle(metadata));
        ConnectorPageSink sink = provider().createPageSink(
                ConnectorTransactionHandle.empty(), SESSION, output);
        List<byte[]> fragments = List.of(finish(sink, SURVIVOR));
        var context = PaimonCommitFixture.context(metadata, SESSION, output, fragments);

        metadata.finishDistributedWrite(SESSION, output, fragments, context);
        long publishedSnapshot = snapshotId();
        var binding = org.pangrova.protocol.v2.WriteOutputBinding.parseFrom(context.writeOutputBinding());
        var handle = binding.getConnector().getOutputHandle();
        var recovered = new org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec().deserialize(
                handle.getCodecVersion(), handle.getPayload().toByteArray());
        assertThat(recovered).isEqualTo(output);
        metadata.finishDistributedWrite(SESSION, recovered, fragments, context);

        assertThat(snapshotId()).isEqualTo(publishedSnapshot);
        assertThat(readRows()).containsExactly(SURVIVOR);
    }

    @Test
    void unjournaledPartitionOverwriteFailsClosed() throws Exception {
        createTable(true, false);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var output = metadata.beginOverwrite(
                SESSION, handle(metadata), Map.of("region", "east"));
        assertThat(output.writeMode()).isEqualTo("OVERWRITE_PARTITIONED");
        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, output, List.of(prepare(output))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectsMixedShardOperationsAndAllocationsBeforeCatalogCommit(boolean sameAllocation) throws Exception {
        createTable(false, false);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var insert = metadata.beginInsert(SESSION, handle(metadata));
        var overwrite = sameAllocation ? insert : metadata.beginOverwrite(SESSION, handle(metadata));
        ConnectorPageSink insertSink = provider().createPageSink(
                ConnectorTransactionHandle.empty(), SESSION, insert);
        byte[] insertFragment = finish(insertSink, SURVIVOR);
        byte[] overwriteFragment = prepare(overwrite, OTHER_SURVIVOR);
        List<byte[]> fragments = List.of(insertFragment, overwriteFragment);
        var context = PaimonCommitFixture.context(metadata, SESSION, insert, fragments);

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, insert, fragments, context))
                .hasStackTraceContaining(sameAllocation
                        ? "Mixed Paimon write operations in one commit" : "differs from its staged file allocation");
        assertThat(readRows()).isEmpty();
        assertThat(snapshotId()).isEqualTo(-1L);
    }

    private org.pangrova.connector.api.metadata.table.ConnectorTableHandle handle(PaimonMetadata metadata) {
        return metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME));
    }

    private byte[] prepare(ConnectorOutputTableHandle output, StoredRow... survivors) {
        ConnectorPageSink sink = provider().createOverwritePageSink(
                ConnectorTransactionHandle.empty(), SESSION, output);
        return finish(sink, survivors);
    }

    private PaimonPageSinkProvider provider() {
        return new PaimonPageSinkProvider(catalog, warehouse.resolve("spill").toString());
    }

    private static byte[] finish(ConnectorPageSink sink, StoredRow... rows) {
        try {
            sink.appendPage(page(rows));
            return sink.finishWithMetadata();
        } catch (RuntimeException | Error failure) {
            sink.abort();
            throw failure;
        } finally {
            sink.close();
        }
    }

    private static Page page(StoredRow... rows) {
        List<TypedRow> values = Arrays.stream(rows).map(row -> TypedRow.builder()
                .putLong("id", row.id()).putString("region", row.region())
                .putString("day", row.day()).putString("payload", row.payload()).build()).toList();
        return new Page(COLUMNS, values);
    }
}
