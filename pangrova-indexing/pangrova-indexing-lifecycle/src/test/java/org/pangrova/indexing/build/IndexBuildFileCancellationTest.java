package org.pangrova.indexing.build;

import org.pangrova.indexing.enums.IndexBuildOutcome;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.strategies.hash.provider.HashStrategyProvider;
import org.pangrova.indexing.strategies.hash.serializer.HashSerializer;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.config.BuildConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.RuntimeConfig;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.storage.IndexStorageManager;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class IndexBuildFileCancellationTest {
    @TempDir Path root;

    enum Point { NONE, READ, ROW_ITERATION, WRITE_COMPLETE, MAPPING_CALLBACK }

    @ParameterizedTest
    @EnumSource(Point.class)
    void partialBuildNeverCompletesSuccessfullyOrContinuesAfterCancellation(Point point) throws Exception {
        var serializer = new IndexSerializer(TypeSerializerRegistry.builder()
                .register(IndexType.HASH, new HashSerializer()).build());
        var config = new IndexingConfig(root.toString(), true,
                new RuntimeConfig(new BuildConfig(1, 1, 1), null, null), null);
        var fs = new RecordingFileSystem();
        var reads = new AtomicInteger();
        var rowsRead = new AtomicInteger();
        List<BuildFileMapping> mappings = new ArrayList<>();
        try (var builder = new IndexBuildCoordinator(new IndexBuildContext(config,
                new IndexEngineRegistry(List.of(new HashStrategyProvider())), new IndexStorageManager(),
                serializer), null)) {
            fs.afterWrite = () -> { if (point == Point.WRITE_COMPLETE) builder.cancelBuild("index"); };
            ColumnValueReader reader = new ColumnValueReader() {
                @Override
                public Iterator<List<Comparable<?>>> readColumns(String file, List<String> columns) {
                    reads.incrementAndGet();
                    if (point == Point.READ) builder.cancelBuild("index");
                    var rows = List.<List<Comparable<?>>>of(List.of(1L), List.of(2L)).iterator();
                    return new Iterator<>() {
                        @Override public boolean hasNext() { return rows.hasNext(); }
                        @Override public List<Comparable<?>> next() {
                            rowsRead.incrementAndGet();
                            if (point == Point.ROW_ITERATION) builder.cancelBuild("index");
                            return rows.next();
                        }
                    };
                }
                @Override public void close() { throw new AssertionError("Borrowed reader was closed"); }
            };
            Instant now = Instant.now();
            var descriptor = new IndexDescriptor("index", "idx", "cat", "sch", "tbl", List.of("col"),
                    IndexType.HASH, Map.of(), IndexState.BUILDING, null, 0, "", "", now, now, "tenant");
            var result = builder.submitBuildTask(descriptor,
                    List.of(new DataFileInfo("first", 2, 10, "test"), new DataFileInfo("second", 2, 10, "test")),
                    fs, reader, batch -> {
                        mappings.addAll(batch);
                        if (point == Point.MAPPING_CALLBACK) builder.cancelBuild("index");
                    }).get(5, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(point == Point.NONE
                    ? IndexBuildOutcome.COMPLETED : IndexBuildOutcome.CANCELLED);
            assertThat(result.isFullySuccessful()).isEqualTo(point == Point.NONE);
            assertThat(reads.get()).isEqualTo(point == Point.NONE ? 2 : 1);
            int expectedFiles = switch (point) {
                case NONE -> 2;
                case READ, ROW_ITERATION -> 0;
                default -> 1;
            };
            assertThat(fs.files).hasSize(expectedFiles);
            assertThat(mappings).hasSize(point == Point.NONE ? 2 : point == Point.MAPPING_CALLBACK ? 1 : 0);
            if (point == Point.READ) assertThat(rowsRead.get()).isZero();
            if (point == Point.ROW_ITERATION) assertThat(rowsRead.get()).isEqualTo(1);
            for (Path file : fs.files) assertThat(serializer.deserialize(Files.readAllBytes(file))).isPresent();
        }
    }

    private static final class RecordingFileSystem implements FileSystemAccessor {
        private final List<Path> files = new ArrayList<>();
        private Runnable afterWrite = () -> {};
        @Override public InputStream openInputStream(String path) throws IOException {
            return Files.newInputStream(Path.of(path));
        }
        @Override public OutputStream openOutputStream(String path) throws IOException {
            Path file = Path.of(path);
            Files.createDirectories(file.getParent());
            return new FilterOutputStream(Files.newOutputStream(file)) {
                @Override public void close() throws IOException {
                    super.close();
                    files.add(file);
                    afterWrite.run();
                }
            };
        }
        @Override public void deleteFile(String path) throws IOException { Files.delete(Path.of(path)); }
        @Override public boolean exists(String path) { return Files.exists(Path.of(path)); }
        @Override public long fileSize(String path) throws IOException { return Files.size(Path.of(path)); }
        @Override public List<String> listFiles(String path) throws IOException {
            try (var files = Files.list(Path.of(path))) { return files.map(Path::toString).toList(); }
        }
        @Override public void close() { throw new AssertionError("Borrowed filesystem was closed"); }
    }
}
