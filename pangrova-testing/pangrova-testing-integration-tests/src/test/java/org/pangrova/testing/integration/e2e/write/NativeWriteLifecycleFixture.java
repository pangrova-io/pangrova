package org.pangrova.testing.integration.e2e.write;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.iceberg.TableProperties;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.coordinator.query.execution.ConnectorWriteBindingFactory;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.WriteTaskOptions;
import org.pangrova.worker.nativebridge.jni.write.NativeWriteExecutor;
import org.pangrova.worker.nativebridge.jni.write.catalog.WorkerCatalogRuntime;
import org.pangrova.worker.nativebridge.jni.write.WriteDescriptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SDK allocation, authenticated deployment HTTP, codec and owned files; not SQL admission. */
public final class NativeWriteLifecycleFixture implements AutoCloseable {
    private final CatalogHandle catalog;
    private final ConnectorSession session;
    private final TaskRequest authenticatedRequest;
    private final WorkerCatalogRuntime runtime;
    private final NativeWriteExecutor executor;
    private final Set<String> tasks = ConcurrentHashMap.newKeySet();

    NativeWriteLifecycleFixture(CatalogHandle catalog, ConnectorSession session,
            TaskRequest authenticatedRequest, String coordinatorUrl, String internalSecret) {
        this.catalog = catalog;
        this.session = session;
        this.authenticatedRequest = java.util.Objects.requireNonNull(authenticatedRequest);
        runtime = WorkerCatalogRuntime.discover(coordinatorUrl, internalSecret,
                Thread.currentThread().getContextClassLoader());
        executor = new NativeWriteExecutor(runtime);
    }

    public NativeWriteExecutor executor() { return executor; }

    public int cachedCatalogRevisionCount() { return runtime.cachedCatalogRevisionCount(); }

    public OpenSession open(String table) throws IOException {
        return open(table, WriteDescriptor.DmlOperation.INSERT, Map.of());
    }

    public OpenSession open(String table, WriteDescriptor.DmlOperation operation,
            Map<String, String> staticPartitions) throws IOException {
        String taskId = "lifecycle-" + UUID.randomUUID();
        var metadata = catalog.getMetadata(session);
        var target = metadata.getTableHandle(session, new SchemaTableName("default", table));
        var allocated = operation == WriteDescriptor.DmlOperation.OVERWRITE
                ? metadata.beginOverwrite(session, target, staticPartitions)
                : metadata.beginInsert(session, target);
        var binding = ConnectorWriteBindingFactory.create(catalog, allocated);
        var request = TaskRequest.newBuilder().setProtocol(authenticatedRequest.getProtocol())
                .setSecurity(authenticatedRequest.getSecurity()).setTaskKind(TaskKind.TASK_KIND_WRITE)
                .setAttempt(authenticatedRequest.getAttempt().toBuilder().setTaskId(taskId)
                        .setQueryOrJobId(taskId))
                .setExecutionOptions(TaskExecutionOptions.newBuilder().setWrite(WriteTaskOptions.newBuilder()
                        .setTargetTable(session.getCatalog().orElseThrow() + ".default." + table)
                        .setWriteMode(operation.name()).setOutputBinding(binding))).build();
        executor.bindTask(taskId, request);
        tasks.add(taskId);
        var bound = runtime.resolve(taskId);
        assertThat(bound.outputHandle()).isEqualTo(allocated);
        assertThat(bound.writeColumnNames()).containsExactlyElementsOf(
                binding.getConnector().getOutputHandle().getWriteColumnNamesList());
        Map<String, String> properties = new HashMap<>(Map.of("taskId", taskId));
        staticPartitions.forEach((key, value) -> properties.put("staticPartition." + key, value));
        var columns = metadata.getTableMetadata(session, target).typedColumns().stream()
                .map(column -> new WriteDescriptor.ColumnSchema(column.name(),
                        java.util.Objects.requireNonNull(column.parameterizedType(), "actual declared type")
                                .getDisplayName())).toList();
        var descriptor = new WriteDescriptor(operation, columns, properties);
        Path tableLocation = location(((IcebergTableHandle) target).tableLocation());
        Set<Path> existingFiles = Set.copyOf(files(tableLocation));
        long handle = executor.beginWrite(descriptor);
        return new OpenSession(taskId, handle, descriptor, request, (IcebergOutputTableHandle) allocated,
                tableLocation, existingFiles);
    }

    public Path appendAndFindOwnedFile(OpenSession opened, long value) throws IOException {
        append(opened.handle(), value);
        return findOwnedFile(opened);
    }

    /** Finds a file naturally created by the real writer without finishing or closing the sink. */
    public Path findOwnedFile(OpenSession opened) throws IOException {
        var added = files(opened).stream().filter(path -> !opened.existingFiles().contains(path)).toList();
        assertThat(added).as("one actual owned Parquet file from the real encoder").hasSize(1);
        return added.get(0);
    }

    /** Real Iceberg table properties that flush each record through the ordinary SDK writer. */
    public static String naturalFlushTableOptions() {
        return " WITH ('" + TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES + "'='1', '"
                + TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT + "'='1', '"
                + TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT + "'='1')";
    }

    public void append(long handle, long value) throws IOException {
        byte[] bytes = batch(value);
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes).flip();
        executor.appendDirectBatch(handle, direct, 1, 1);
    }

    public static List<Path> files(OpenSession opened) throws IOException {
        return files(opened.tableLocation());
    }

    private static List<Path> files(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .filter(Files::isRegularFile).toList();
        }
    }

    private static byte[] batch(long value) throws IOException {
        try (var allocator = new RootAllocator(1024 * 1024)) {
            var vector = new BigIntVector("id", allocator);
            try (var root = VectorSchemaRoot.of(vector);
                    var output = new ByteArrayOutputStream();
                    var writer = new ArrowStreamWriter(root, null, output)) {
                vector.allocateNew(1);
                vector.set(0, value);
                vector.setValueCount(1);
                root.setRowCount(1);
                writer.start();
                writer.writeBatch();
                writer.end();
                return output.toByteArray();
            }
        }
    }

    private static Path location(String location) {
        URI uri = URI.create(location);
        return uri.getScheme() == null ? Path.of(location) : Path.of(uri);
    }

    @Override
    public void close() {
        executor.closeAllSessions();
        tasks.forEach(executor::releaseTask);
        executor.close();
    }

    public record OpenSession(String taskId, long handle, WriteDescriptor descriptor, TaskRequest request,
                              IcebergOutputTableHandle outputHandle, Path tableLocation,
                              Set<Path> existingFiles) {}

    /** Actual Hadoop delete failure at the exact writer-owned path, restored before cleanup retry. */
    public static final class DeletionBarrier implements AutoCloseable {
        private final Path file;
        private final Path retained;
        private final Path blocker;

        public DeletionBarrier(Path file) throws IOException {
            this.file = file;
            retained = file.resolveSibling(file.getFileName() + ".retained");
            Files.move(file, retained);
            Files.createDirectory(file);
            blocker = Files.writeString(file.resolve("actual-delete-barrier"), "occupied");
        }

        /** Returns the original owned file retained while its actual deletion is blocked. */
        public Path retainedFile() {
            return retained;
        }

        @Override
        public void close() throws IOException {
            Files.delete(blocker);
            Files.delete(file);
            Files.move(retained, file);
        }
    }
}
