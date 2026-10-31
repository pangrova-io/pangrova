package org.pangrova.connector.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.pangrova.protocol.v2.AcceptedSourceSnapshotRef;
import org.pangrova.protocol.v2.DatasetScanPartitionsRequest;
import org.pangrova.protocol.v2.DatasetScanPartitionsResult;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.WritePreparationOutcome;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Real fixed Rust SDK through the same formal FFI used by Worker; PostgreSQL remains the sole source authority. */
final class LanceNativeSdkFixture {
    private final LanceManagedSourceFixture catalog;
    private final Path directory;
    private final Path executable;
    private final java.util.concurrent.atomic.AtomicInteger invocation = new java.util.concurrent.atomic.AtomicInteger();

    LanceNativeSdkFixture(LanceManagedSourceFixture catalog, Path directory) {
        this.catalog = catalog;
        this.directory = directory;
        String path = System.getProperty("pangrova.lance.fixture.tool");
        if (path == null || !Files.isExecutable(Path.of(path))) {
            throw new IllegalStateException("Build the matching pangrova_lance_fixture_tool and set pangrova.lance.fixture.tool");
        }
        executable = Path.of(path);
    }

    LanceManagedSourceFixture.Allocation allocate(Path stream, String uri, String writeId,
            LanceWriteOperation operation, long version, int partition, org.apache.arrow.vector.types.pojo.Schema schema)
            throws Exception {
        var hash = path("schema.sha256");
        run("fingerprint", stream.toString(), hash.toString());
        return catalog.allocate(uri, writeId, operation, version, Files.readAllBytes(hash), schema.serializeAsMessage(), partition);
    }

    byte[] write(Path stream, LanceManagedSourceFixture.Allocation allocation) throws Exception {
        var storage = save("storage.pb", storage(allocation.output().table().datasetUri()).toByteArray());
        var attempt = save("attempt.pb", allocation.attempt().toByteArray());
        var binding = save("output.pb", allocation.binding().toByteArray());
        var fragment = path("fragment.pb");
        run("write", storage.toString(), attempt.toString(), binding.toString(), allocation.attemptPrefix(),
                stream.toString(), fragment.toString());
        return Files.readAllBytes(fragment);
    }

    List<PrepareDistributedWriteResult> prepare(LanceManagedSourceFixture.Allocation allocation, List<byte[]> fragments)
            throws Exception {
        return prepare(catalog.reserve(allocation.executionId(), fragments));
    }

    List<PrepareDistributedWriteResult> prepare(PrepareDistributedWriteRequest first) throws Exception {
        List<PrepareDistributedWriteResult> pages = new ArrayList<>();
        var request = first.toBuilder().clearInvocationId().build();
        for (int page = 0; page < org.pangrova.connector.spi.metadata.SourceObjectPage.MAX_PAGES; page++) {
            var result = preparePage(request);
            if (result.getOutcome() != WritePreparationOutcome.WRITE_PREPARATION_OUTCOME_PREPARED
                    || !result.hasPublication() || !result.hasObjectPage()) {
                throw new IllegalStateException("Actual fixed SDK preparation is UNKNOWN");
            }
            pages.add(result);
            if (result.getObjectPage().getIsLast()) return List.copyOf(pages);
            var objects = result.getObjectPage().getObjectsList();
            request = request.toBuilder().clearInvocationId().setObjectPageIndex(page + 1)
                    .setAfterObjectPath(objects.get(objects.size() - 1).getRelativePath())
                    .setDeadlineEpochMs(Instant.now().plusSeconds(30).toEpochMilli()).build();
        }
        throw new IllegalStateException("Actual SDK object membership exceeds its page bound");
    }

    PrepareDistributedWriteResult preparePage(PrepareDistributedWriteRequest request) throws Exception {
        try (var actor = new LanceSdkPreparationActor()) {
            if (!request.getInvocationId().isEmpty()) {
                // The coordinator has already issued this invocation durably. The
                // worker-side fixture must execute that exact request, not issue
                // a second invocation from its own fixture catalog.
                LanceSdkPreparationActor.requireListeningActor(actor.socket(), actor.nonce(),
                        request.toBuilder().clearInvocationId().build());
                return actor.prepare(request, this::prepareIssued);
            }
            var issued = catalog.issuePreparation(request, actor.socket(), actor.nonce());
            return actor.prepare(issued, this::prepareIssued);
        }
    }

    private PrepareDistributedWriteResult prepareIssued(PrepareDistributedWriteRequest request) throws Exception {
        var storage = save("storage.pb", storage(request.getOutputBinding().getLance().getDatasetUri()).toByteArray());
        var input = save("prepare.pb", request.toByteArray());
        var output = path("prepared.pb");
        run("prepare", storage.toString(), input.toString(), output.toString());
        return PrepareDistributedWriteResult.parseFrom(Files.readAllBytes(output));
    }

    org.pangrova.protocol.v2.CleanupSourceObjectsResult cleanupPage(
            org.pangrova.protocol.v2.CleanupSourceObjectsRequest request) throws Exception {
        var input = save("cleanup.pb", request.toByteArray());
        var storage = save("storage.pb", storage(request.getDatasetUri()).toByteArray());
        var output = path("deleted.pb"); run("cleanup", storage.toString(), input.toString(), output.toString());
        return org.pangrova.protocol.v2.CleanupSourceObjectsResult.parseFrom(Files.readAllBytes(output));
    }

    void cleanup(String execution) throws Exception {
        for (int page = 0; page < org.pangrova.connector.spi.metadata.SourceObjectPage.MAX_PAGES; page++) {
            var request = catalog.cleanupRequest(execution);
            if (request.isEmpty()) return;
            catalog.completeCleanup(execution, cleanupPage(request.orElseThrow()));
        }
        throw new IllegalStateException("Actual source cleanup exceeded the admitted page bound");
    }

    AcceptedSourceSnapshotRef accept(LanceManagedSourceFixture.Allocation allocation, List<PrepareDistributedWriteResult> pages) {
        AcceptedSourceSnapshotRef accepted = null;
        for (var page : pages) accepted = catalog.register(allocation.executionId(), page);
        if (accepted == null) throw new IllegalStateException("Actual PG did not accept complete SDK membership");
        var stored = catalog.reference(accepted.getDatasetUri(), accepted.getVersion());
        if (!stored.equals(accepted)) throw new IllegalStateException("Actual accepted source differs from publication receipt");
        return accepted;
    }

    List<List<Object>> read(AcceptedSourceSnapshotRef reference, BufferAllocator allocator) throws Exception {
        var actual = catalog.reference(reference.getDatasetUri(), reference.getVersion());
        if (!actual.equals(reference)) throw new IllegalArgumentException("Read fixture requires actual PG accepted source");
        var storage = save("storage.pb", storage(reference.getDatasetUri()).toByteArray());
        List<List<Object>> rows = new ArrayList<>();
        var request = DatasetScanPartitionsRequest.newBuilder().setCatalog(catalog.catalog())
                .setSecurity(SecurityContext.newBuilder().setTenantId(LanceManagedSourceFixture.TENANT)
                        .setPrincipal(LanceManagedSourceFixture.PRINCIPAL))
                .setDatasetUri(reference.getDatasetUri()).setVersion(reference.getVersion()).setAcceptedSnapshot(reference)
                .setPhysicalSchemaSha256(reference.getPhysicalSchemaSha256()).setPageSize(1)
                .setDeadlineEpochMs(Instant.now().plusSeconds(30).toEpochMilli()).build();
        for (;;) {
            var input = save("scan.pb", request.toByteArray());
            var output = path("partitions.pb");
            run("scan", storage.toString(), input.toString(), output.toString());
            var page = DatasetScanPartitionsResult.parseFrom(Files.readAllBytes(output));
            if (!page.getAcceptedSnapshot().equals(reference)) throw new IllegalStateException("SDK scan lost its accepted source");
            for (var partition : page.getPartitionsList()) {
                var detail = save("split.pb", FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_LANCE)
                        .setScanPartition(partition).build().toByteArray());
                var stream = path("rows.arrow");
                run("read", storage.toString(), reference.getDatasetUri(), Long.toString(reference.getVersion()),
                        detail.toString(), stream.toString());
                try (var reader = new ArrowStreamReader(Files.newInputStream(stream), allocator)) {
                    while (reader.loadNextBatch()) {
                        var root = reader.getVectorSchemaRoot();
                        for (int row = 0; row < root.getRowCount(); row++) {
                            List<Object> values = new ArrayList<>();
                            for (var vector : root.getFieldVectors()) values.add(vector.getObject(row));
                            rows.add(java.util.Collections.unmodifiableList(values));
                        }
                    }
                }
            }
            if (!page.getHasMore()) return List.copyOf(rows);
            request = request.toBuilder().setHasCursor(true).setAfterPartitionId(page.getNextPartitionId())
                    .setDeadlineEpochMs(Instant.now().plusSeconds(30).toEpochMilli()).build();
        }
    }

    DatasetScanPartitionsResult scanPage(AcceptedSourceSnapshotRef reference, java.util.OptionalLong after, int pageSize)
            throws Exception {
        if (!catalog.reference(reference.getDatasetUri(), reference.getVersion()).equals(reference)) {
            throw new IllegalArgumentException("Scan fixture requires actual PG accepted source");
        }
        var request = DatasetScanPartitionsRequest.newBuilder().setCatalog(catalog.catalog())
                .setSecurity(SecurityContext.newBuilder().setTenantId(LanceManagedSourceFixture.TENANT)
                        .setPrincipal(LanceManagedSourceFixture.PRINCIPAL))
                .setDatasetUri(reference.getDatasetUri()).setVersion(reference.getVersion()).setAcceptedSnapshot(reference)
                .setPhysicalSchemaSha256(reference.getPhysicalSchemaSha256()).setPageSize(pageSize)
                .setHasCursor(after.isPresent()).setDeadlineEpochMs(Instant.now().plusSeconds(30).toEpochMilli());
        after.ifPresent(request::setAfterPartitionId);
        var store = save("storage.pb", storage(reference.getDatasetUri()).toByteArray());
        var input = save("scan.pb", request.build().toByteArray());
        var output = path("scan-result.pb");
        run("scan", store.toString(), input.toString(), output.toString());
        return DatasetScanPartitionsResult.parseFrom(Files.readAllBytes(output));
    }

    private ObjectStoreExecutionBinding storage(String uri) {
        return ObjectStoreExecutionBinding.newBuilder().setCatalog(catalog.catalog()).setTenantId(LanceManagedSourceFixture.TENANT)
                .setWarehouseUri(Path.of(uri).getParent().toString()).build();
    }
    private Path path(String suffix) { return directory.resolve("sdk_" + invocation.getAndIncrement() + "_" + suffix); }
    private Path save(String suffix, byte[] bytes) throws IOException {
        var path = path(suffix);
        Files.write(path, bytes);
        return path;
    }
    private void run(String... arguments) throws Exception {
        var command = new ArrayList<String>();
        command.add(executable.toString());
        command.addAll(List.of(arguments));
        var log = path("process.log");
        var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            throw new IllegalStateException("Actual fixed SDK fixture command timed out: " + Files.readString(log));
        }
        if (process.exitValue() != 0) throw new IllegalStateException("Actual fixed SDK fixture rejected command: " + Files.readString(log));
    }
}
