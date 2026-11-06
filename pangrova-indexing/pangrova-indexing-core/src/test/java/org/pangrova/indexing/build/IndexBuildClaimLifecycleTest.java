package org.pangrova.indexing.build;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.spi.index.ColumnValueReader;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.lifecycle.IndexState;
import org.pangrova.indexing.builtin.hash.provider.HashStrategyProvider;
import org.pangrova.indexing.builtin.hash.serializer.HashSerializer;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.config.BuildConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.RuntimeConfig;
import org.pangrova.indexing.registry.IndexEngineRegistry;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.indexing.metrics.IndexBuildMetrics;
import org.pangrova.observability.spi.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Exercises ownership failure at the real scalar builder, codec, file and mapping boundary. */
class IndexBuildClaimLifecycleTest {
    @TempDir Path root;

    @Test
    void eachSubmissionUsesANewOwnerAndReleasesTheExactAcquiredOwner() throws Exception {
        var claims = new Claims();
        try (var builder = builder(claims)) {
            for (int i = 0; i < 2; i++) {
                assertThat(builder.submitBuildTask(descriptor(), List.of(), null, null)
                        .get(5, TimeUnit.SECONDS).isFullySuccessful()).isTrue();
            }
        }
        assertThat(claims.acquired).hasSize(2).doesNotHaveDuplicates();
        assertThat(claims.released).containsExactlyElementsOf(claims.acquired);
    }

    @Test
    void rejectedClaimDoesNotReleaseAnotherOwnersWork() throws Exception {
        var claims = new Claims();
        claims.acceptClaim = false;
        try (var builder = builder(claims)) {
            assertThat(builder.submitBuildTask(descriptor(), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS).outcome()).isEqualTo(IndexBuildResult.Outcome.CLAIM_FAILED);
        }
        assertThat(claims.released).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedReleaseCannotReportASuccessfulBuild(boolean backendFailure) {
        var claims = new Claims();
        claims.acceptRelease = false;
        claims.releaseFailure = backendFailure ? new IllegalStateException("release connection failed") : null;
        try (var builder = builder(claims)) {
            var failure = catchThrowableOfType(() -> builder.submitBuildTask(descriptor(), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS), ExecutionException.class);
            assertThat(failure.getCause()).isInstanceOf(IllegalStateException.class);
            if (backendFailure) assertThat(failure.getCause()).isSameAs(claims.releaseFailure);
        }
        assertThat(claims.released).containsExactlyElementsOf(claims.acquired);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lostOrFailedHeartbeatStopsBeforeMappingAndNextFile(boolean backendFailure) {
        var claims = new Claims();
        claims.acceptHeartbeat = false;
        claims.heartbeatFailure = backendFailure ? new IllegalStateException("heartbeat connection failed") : null;
        var reader = new Rows();
        List<BuildFileMapping> mappings = new ArrayList<>();
        try (var builder = builder(claims)) {
            var failure = buildFailure(builder, reader, mappings::addAll);
            assertThat(failure.getCause()).isInstanceOf(IllegalStateException.class);
            if (backendFailure) assertThat(failure.getCause()).isSameAs(claims.heartbeatFailure);
        }
        assertThat(reader.reads).isEqualTo(1);
        assertThat(mappings).isEmpty();
        assertThat(claims.renewed).containsExactlyElementsOf(claims.acquired);
        assertThat(claims.released).containsExactlyElementsOf(claims.acquired);
    }

    @Test
    void cleanupFailureIsSuppressedOnOriginalMappingFailure() {
        var claims = new Claims();
        claims.releaseFailure = new IllegalStateException("cleanup failed");
        var mappingFailure = new IllegalArgumentException("mapping transaction failed");
        try (var builder = builder(claims)) {
            var failure = buildFailure(builder, new Rows(), batch -> { throw mappingFailure; });
            assertThat(failure.getCause()).isSameAs(mappingFailure);
            assertThat(mappingFailure.getSuppressed()).containsExactly(claims.releaseFailure);
        }
    }

    @Test
    void successfulOwnedBuildProducesBothMappings() throws Exception {
        var claims = new Claims();
        var reader = new Rows();
        List<BuildFileMapping> mappings = new ArrayList<>();
        try (var builder = builder(claims)) {
            assertThat(builder.submitBuildTask(descriptor(), files(), new LocalFiles(), reader, mappings::addAll)
                    .get(5, TimeUnit.SECONDS).isFullySuccessful()).isTrue();
        }
        assertThat(mappings).hasSize(2);
        assertThat(reader.reads).isEqualTo(2);
        assertThat(claims.renewed).containsOnly(claims.acquired.get(0));
        assertThat(claims.released).containsExactlyElementsOf(claims.acquired);
    }

    private ExecutionException buildFailure(IndexBuildCoordinator builder, Rows reader,
            Consumer<List<BuildFileMapping>> mappings) {
        return catchThrowableOfType(() -> builder.submitBuildTask(descriptor(), files(), new LocalFiles(),
                reader, mappings).get(5, TimeUnit.SECONDS), ExecutionException.class);
    }

    @Test
    void failedReleaseRecordsOneFailureDurationWithoutPrematureCompletion() {
        var claims = new Claims();
        claims.acceptRelease = false;
        var metrics = new RecordingMetrics();
        try (var builder = builder(claims, new IndexBuildMetrics(metrics))) {
            var failure = catchThrowableOfType(() -> builder.submitBuildTask(descriptor(), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS), ExecutionException.class);
            assertThat(failure.getCause()).hasMessageContaining("claim lost");
        }
        assertThat(metrics.failures.get()).isEqualTo(1);
        assertThat(metrics.durations.get()).isEqualTo(1);
    }

    private IndexBuildCoordinator builder(Claims claims) {
        return builder(claims, null);
    }

    private IndexBuildCoordinator builder(Claims claims, IndexBuildMetrics metrics) {
        var config = new IndexingConfig(root.toString(), true,
                new RuntimeConfig(new BuildConfig(1, 1, 1), null, null), null);
        var serializer = new IndexSerializer(TypeSerializerRegistry.builder()
                .register(IndexType.HASH, new HashSerializer()).build());
        return new IndexBuildCoordinator(new IndexBuildContext(config,
                new IndexEngineRegistry(List.of(new HashStrategyProvider())), new IndexStorageManager(),
                serializer, claims, null, null, null), metrics);
    }

    private static IndexDescriptor descriptor() {
        Instant now = Instant.now();
        return new IndexDescriptor("index", "idx", "cat", "sch", "tbl", List.of("col"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 0, "", "", now, now, "tenant");
    }

    private static List<DataFileInfo> files() {
        return List.of(new DataFileInfo("first", 2, 10, "test"), new DataFileInfo("second", 2, 10, "test"));
    }

    private static final class RecordingMetrics implements MetricsProvider {
        private final AtomicInteger durations = new AtomicInteger();
        private final AtomicLong failures = new AtomicLong();
        @Override public LongCounter createCounter(String name, String description, String unit) {
            return value -> { if (name.endsWith("failures_total")) failures.addAndGet(value); };
        }
        @Override public DoubleHistogram createHistogram(
                String name, String description, String unit, List<Double> boundaries) {
            return value -> durations.incrementAndGet();
        }
        @Override public ObservableDoubleGauge createGauge(
                String name, String description, String unit, Supplier<Double> supplier) {
            return () -> {};
        }
        @Override public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            throw new AssertionError("Builder does not use an up-down counter");
        }
    }

    private static final class Claims implements IndexBuildClaimRepository {
        private final List<String> acquired = new ArrayList<>();
        private final List<String> renewed = new ArrayList<>();
        private final List<String> released = new ArrayList<>();
        private boolean acceptClaim = true;
        private boolean acceptHeartbeat = true;
        private boolean acceptRelease = true;
        private RuntimeException heartbeatFailure;
        private RuntimeException releaseFailure;

        @Override public boolean tryClaim(String index, int generation, String owner, Instant now) {
            acquired.add(owner);
            return acceptClaim;
        }
        @Override public boolean heartbeat(String index, int generation, String owner, Instant now) {
            renewed.add(owner);
            if (heartbeatFailure != null) throw heartbeatFailure;
            return acceptHeartbeat;
        }
        @Override public boolean release(String index, int generation, String owner) {
            released.add(owner);
            if (releaseFailure != null) throw releaseFailure;
            return acceptRelease;
        }
        @Override public int expireBefore(Instant cutoff) {
            throw new AssertionError("Builder does not own recovery");
        }
    }

    private static final class Rows implements ColumnValueReader {
        private int reads;
        @Override public Iterator<List<Comparable<?>>> readColumns(String file, List<String> columns) {
            reads++;
            return List.<List<Comparable<?>>>of(List.of(1L), List.of(2L)).iterator();
        }
        @Override public void close() { throw new AssertionError("Borrowed reader was closed"); }
    }

    private static final class LocalFiles implements FileSystemAccessor {
        @Override public InputStream openInputStream(String path) throws IOException {
            return Files.newInputStream(Path.of(path));
        }
        @Override public OutputStream openOutputStream(String path) throws IOException {
            Path file = Path.of(path);
            Files.createDirectories(file.getParent());
            return Files.newOutputStream(file);
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
