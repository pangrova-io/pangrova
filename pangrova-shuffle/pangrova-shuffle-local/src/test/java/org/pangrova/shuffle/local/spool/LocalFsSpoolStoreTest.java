package org.pangrova.shuffle.local.spool;

import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.pangrova.shuffle.api.enums.ShuffleDurability;
import org.pangrova.shuffle.api.spool.SpoolReadSelection;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;
import org.pangrova.shuffle.api.spool.SpoolStoreFactory;
import org.pangrova.shuffle.api.spool.SpoolWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.*;

class LocalFsSpoolStoreTest {
    @TempDir Path directory;
    private LocalFsSpoolStore store;

    @BeforeEach void open() { store = new LocalFsSpoolStore(directory); }
    @AfterEach void close() { store.close(); }

    @Test
    void preservesEveryEncodedBatchBoundaryAndProducerOrder() {
        write(7, 1, "a", "bb", "ccc");
        write(2, 9, "before");
        assertThat(read(Map.of(7, 1L, 2, 9L))).containsExactly("before", "a", "bb", "ccc");
    }

    @Test
    void selectedAttemptWinsEvenWhenALargerAttemptHasFinished() {
        write(0, 1, "selected");
        write(0, Long.MAX_VALUE, "loser");
        assertThat(read(Map.of(0, 1L))).containsExactly("selected");
        assertThat(read(Map.of(0, Long.MAX_VALUE))).containsExactly("loser");
    }

    @Test
    void attemptIdentifiersAreNotTruncatedToIntegers() {
        long attempt = (long) Integer.MAX_VALUE + 123;
        write(0, attempt, "large");
        write(0, 122, "other");
        assertThat(read(Map.of(0, attempt))).containsExactly("large");
    }

    @Test
    void selectionControlsProducerRangeWithoutScanningOtherAttempts() {
        for (int producer = 0; producer < 4; producer++) write(producer, 0, "p" + producer);
        assertThat(read(Map.of(1, 0L, 2, 0L))).containsExactly("p1", "p2");
    }

    @Test
    void absentSelectedAttemptIsRejectedInsteadOfSubstitutingOrReturningEmpty() {
        write(0, 1, "wrong-attempt");
        assertThatThrownBy(() -> store.read(selection(Map.of(0, 0L))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("absent or incomplete");
        assertThatThrownBy(() -> store.read(selection(Map.of(0, 1L, 1, 0L))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anEmptyCompletedSegmentIsDistinctFromAMissingSegment() {
        write(0, 0);
        assertThat(read(Map.of(0, 0L))).isEmpty();
        assertThatThrownBy(() -> store.read(selection(Map.of(1, 0L)))).isInstanceOf(IllegalStateException.class);
        assertThat(read(Map.of())).isEmpty();
    }

    @Test
    void uncompletedWriterIsNotReadableAndAbortRemovesOnlyItsTemporaryFile() throws Exception {
        try (var writer = store.open("q", 0, 0, 0, 0)) {
            writer.write(bytes("pending"));
            assertThatThrownBy(() -> store.read(selection(Map.of(0, 0L)))).isInstanceOf(IllegalStateException.class);
        }
        try (var paths = Files.list(directory.resolve("q/s0/p0"))) {
            assertThat(paths.toList()).isEmpty();
        }
    }

    @Test
    void duplicateWriterAndCompletedAttemptCannotBeOverwritten() {
        try (var first = store.open("q", 0, 0, 0, 0)) {
            first.write(bytes("original"));
            assertThatThrownBy(() -> store.open("q", 0, 0, 0, 0)).isInstanceOf(IllegalStateException.class);
            first.commit();
            first.commit();
            assertThatThrownBy(() -> first.write(bytes("late"))).isInstanceOf(IllegalStateException.class);
        }
        assertThatThrownBy(() -> store.open("q", 0, 0, 0, 0)).isInstanceOf(IllegalStateException.class);
        assertThat(read(Map.of(0, 0L))).containsExactly("original");
    }

    @Test
    void concurrentOpenHasExactlyOneOwner() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var call = (java.util.concurrent.Callable<Object>) () -> {
                start.await();
                try { return store.open("q", 0, 0, 0, 0); }
                catch (IllegalStateException conflict) { return conflict; }
            };
            var first = pool.submit(call);
            var second = pool.submit(call);
            start.countDown();
            var results = List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
            assertThat(results.stream().filter(SpoolWriter.class::isInstance).count()).isEqualTo(1);
            assertThat(results.stream().filter(IllegalStateException.class::isInstance).count()).isEqualTo(1);
            for (var result : results) if (result instanceof SpoolWriter writer) writer.close();
        } finally { pool.shutdownNow(); }
    }

    @Test
    void cleanupCannotRaceAnActiveReaderIncludingBeforeFirstReadAndAfterEof() {
        write(0, 0, "retained");
        try (var reader = store.read(selection(Map.of(0, 0L)))) {
            assertThatThrownBy(() -> store.delete(new String("q"))).isInstanceOf(IllegalStateException.class);
            assertThat(reader.next()).isNotNull();
            assertThat(reader.hasNext()).isFalse();
            assertThatThrownBy(() -> store.delete("q")).isInstanceOf(IllegalStateException.class);
        }
        store.delete("q");
        store.delete("q");
        assertThat(directory.resolve("q")).doesNotExist();
    }

    @Test
    void cleanupCannotRaceAWriterEvenAfterItsCommitUntilClose() {
        try (var writer = store.open("q", 0, 0, 0, 0)) {
            assertThatThrownBy(() -> store.delete("q")).isInstanceOf(IllegalStateException.class);
            writer.commit();
            assertThatThrownBy(() -> store.delete("q")).isInstanceOf(IllegalStateException.class);
        }
        store.delete("q");
    }

    @Test
    void bufferIsConsumedAndCallerMutationAfterReturnCannotChangeStoredBytes() {
        byte[] input = "xpayloadz".getBytes(StandardCharsets.UTF_8);
        ByteBuffer slice = ByteBuffer.wrap(input, 1, 7);
        try (var writer = store.open("q", 0, 0, 0, 0)) {
            writer.write(slice);
            assertThat(slice.remaining()).isZero();
            java.util.Arrays.fill(input, (byte) 0);
            writer.commit();
        }
        assertThat(read(Map.of(0, 0L))).containsExactly("payload");
    }

    @Test
    void directBuffersAreSupported() {
        var batch = ByteBuffer.allocateDirect(6).put("direct".getBytes(StandardCharsets.UTF_8)).flip();
        try (var writer = store.open("q", 0, 0, 0, 0)) { writer.write(batch); writer.commit(); }
        assertThat(read(Map.of(0, 0L))).containsExactly("direct");
    }

    @Test
    void corruptPayloadCannotPassTheBatchChecksum() throws Exception {
        write(0, 0, "verified");
        byte[] data = Files.readAllBytes(segment(0, 0));
        data[8 + 4 + 32] ^= 1;
        Files.write(segment(0, 0), data);
        try (var reader = store.read(selection(Map.of(0, 0L)))) {
            assertThatThrownBy(reader::next).isInstanceOf(UncheckedIOException.class);
            assertThatThrownBy(reader::hasNext).isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void truncationAtEveryByteIncludingBatchBoundaryIsNotReportedAsEof() throws Exception {
        write(0, 0, "body");
        byte[] complete = Files.readAllBytes(segment(0, 0));
        for (int size = 0; size < complete.length; size++) {
            Files.write(segment(0, 0), java.util.Arrays.copyOf(complete, size));
            assertThatThrownBy(() -> read(Map.of(0, 0L))).as("truncated length %s", size)
                    .isInstanceOf(UncheckedIOException.class);
        }
        Files.write(segment(0, 0), complete);
        assertThat(read(Map.of(0, 0L))).containsExactly("body");
    }

    @Test
    void unsupportedVersionAndOversizedFrameAreRejectedBeforeAllocation() throws Exception {
        write(0, 0, "body");
        byte[] original = Files.readAllBytes(segment(0, 0));
        byte[] badVersion = original.clone();
        ByteBuffer.wrap(badVersion).putInt(4, 999);
        Files.write(segment(0, 0), badVersion);
        assertThatThrownBy(() -> read(Map.of(0, 0L))).isInstanceOf(UncheckedIOException.class);
        byte[] oversized = original.clone();
        ByteBuffer.wrap(oversized).putInt(8, Integer.MAX_VALUE);
        Files.write(segment(0, 0), oversized);
        assertThatThrownBy(() -> read(Map.of(0, 0L))).isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void trailerCountAndTrailingBytesAreVerified() throws Exception {
        write(0, 0, "body");
        byte[] original = Files.readAllBytes(segment(0, 0));
        byte[] wrongCount = original.clone();
        ByteBuffer.wrap(wrongCount).putLong(wrongCount.length - 16, 2);
        Files.write(segment(0, 0), wrongCount);
        assertThatThrownBy(() -> read(Map.of(0, 0L))).isInstanceOf(UncheckedIOException.class);
        Files.write(segment(0, 0), java.util.Arrays.copyOf(original, original.length + 1));
        assertThatThrownBy(() -> read(Map.of(0, 0L))).isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void storeCloseCancelsStorageResourcesAndReleasesDirectoryOwnership() {
        write(0, 0, "complete");
        var reader = store.read(selection(Map.of(0, 0L)));
        var writer = store.open("q", 0, 0, 1, 0);
        writer.write(bytes("pending"));
        store.close();
        assertThatThrownBy(reader::next).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(writer::commit).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.open("q", 0, 0, 2, 0)).isInstanceOf(IllegalStateException.class);
        try (var reopened = new LocalFsSpoolStore(directory);
             var restored = reopened.read(selection(Map.of(0, 0L)))) {
            assertThat(text(restored.next())).isEqualTo("complete");
            assertThat(restored.hasNext()).isFalse();
            assertThatThrownBy(() -> reopened.read(selection(Map.of(1, 0L)))).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void directoryOwnershipAlsoExcludesAnotherJvmAndIsReleasedOnClose() throws Exception {
        assertThat(probeDirectory()).isEqualTo(23);
        store.close();
        assertThat(probeDirectory()).isZero();
    }

    private int probeDirectory() throws Exception {
        var paths = new ArrayList<String>();
        for (Class<?> type : List.of(LocalSpoolDirectoryOwnerProbe.class, LocalFsSpoolStore.class,
                SpoolStore.class, org.pangrova.common.util.Preconditions.class)) {
            paths.add(Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString());
        }
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", String.join(java.io.File.pathSeparator, paths),
                LocalSpoolDirectoryOwnerProbe.class.getName(), directory.toString()).redirectErrorStream(true).start();
        try {
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            assertThat(output).isIn("BUSY", "OWNED");
            return process.exitValue();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void secondStoreCannotOwnTheSameDirectory() {
        assertThatThrownBy(() -> new LocalFsSpoolStore(directory)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void commitFailureDoesNotOverwriteOrDeleteACompletedPath() throws Exception {
        try (var writer = store.open("q", 0, 0, 0, 0)) {
            writer.write(bytes("candidate"));
            Files.writeString(segment(0, 0), "already-present");
            assertThatThrownBy(writer::commit).isInstanceOf(UncheckedIOException.class);
            assertThatThrownBy(writer::commit).isInstanceOf(IllegalStateException.class);
        }
        assertThat(Files.readString(segment(0, 0))).isEqualTo("already-present");
    }

    @Test
    void invalidIdentitiesAndEmptyBatchesAreRejected() {
        for (String query : List.of("", ".", "..", "../secret", "foo/bar", "foo\\bar")) {
            assertThatThrownBy(() -> store.open(query, 0, 0, 0, 0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.delete(query)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> store.open(null, 0, 0, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.open("q", -1, 0, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.open("q", 0, -1, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.open("q", 0, 0, -1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.open("q", 0, 0, 0, -1)).isInstanceOf(IllegalArgumentException.class);
        try (var writer = store.open("q", 0, 0, 0, 0)) {
            assertThatThrownBy(() -> writer.write(ByteBuffer.allocate(0))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> writer.write(ByteBuffer.allocate(LocalSpoolFormat.MAX_BATCH_BYTES + 1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void symlinkQueryDirectoryCannotEscapeTheOwnedRoot(@TempDir Path foreign) throws Exception {
        Files.createSymbolicLink(directory.resolve("q"), foreign);
        assertThatThrownBy(() -> store.open("q", 0, 0, 0, 0)).isInstanceOf(UncheckedIOException.class);
        assertThatThrownBy(() -> store.delete("q")).isInstanceOf(IllegalStateException.class);
        try (var paths = Files.list(foreign)) { assertThat(paths.toList()).isEmpty(); }
    }

    @Test
    void serviceLoaderDiscoversOnlyTheDeclaredLocalFactoryAndOwnsItsResources() {
        var factories = ServiceLoader.load(SpoolStoreFactory.class).stream().map(ServiceLoader.Provider::get).toList();
        assertThat(factories).hasSize(1);
        assertThat(factories.get(0).name()).isEqualTo("local-fs");
        try (SpoolStore created = factories.get(0).create(new SpoolStoreContext("local-fs",
                Map.of("spool.local-fs.base-dir", directory.resolve("another").toString())))) {
            assertThat(created.durability()).isEqualTo(ShuffleDurability.LOCAL_SPILL);
            try (var writer = created.open("factory-query", 0, 0, 0, 2)) {
                writer.write(ByteBuffer.wrap(new byte[] {4, 5}));
                writer.commit();
            }
            try (var reader = created.read(new SpoolReadSelection("factory-query", 0, 0, Map.of(0, 2L)))) {
                assertThatThrownBy(() -> created.delete("factory-query")).isInstanceOf(IllegalStateException.class);
                assertThat(reader.next().remaining()).isEqualTo(2);
            }
            created.delete("factory-query");
            assertThat(directory.resolve("another/factory-query")).doesNotExist();
        }
    }

    @Test
    void factoryRejectsWrongBackendAndBlankDirectoryBeforeCreatingResources() {
        var factory = new LocalFsSpoolStoreFactory();
        var unused = directory.resolve("unused");
        assertThatThrownBy(() -> factory.create(new SpoolStoreContext("different",
                Map.of("spool.local-fs.base-dir", unused.toString())))).isInstanceOf(IllegalArgumentException.class);
        assertThat(unused).doesNotExist();
        assertThatThrownBy(() -> factory.create(new SpoolStoreContext("local-fs",
                Map.of("spool.local-fs.base-dir", " ")))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validQueryIdentityWithPunctuationIsAccepted() {
        try (var writer = store.open("query-123_abc.456", 0, 0, 0, 0)) { writer.commit(); }
        try (var reader = store.read(new SpoolReadSelection("query-123_abc.456", 0, 0, Map.of(0, 0L)))) {
            assertThat(reader.hasNext()).isFalse();
        }
    }

    @Test
    void selectedDirectoryCannotRedirectALazyReader(@TempDir Path foreign) throws Exception {
        write(0, 0, "selected");
        try (var other = new LocalFsSpoolStore(foreign);
             var writer = other.open("q", 0, 0, 0, 0)) {
            writer.write(bytes("foreign"));
            writer.commit();
        }
        try (var reader = store.read(selection(Map.of(0, 0L)))) {
            Path selected = directory.resolve("q/s0/p0");
            Files.move(selected, directory.resolve("detached"));
            Files.createSymbolicLink(selected, foreign.resolve("q/s0/p0"));
            assertThat(text(reader.next())).isEqualTo("selected");
        }
    }

    private void write(int producer, long attempt, String... batches) {
        try (var writer = store.open("q", 0, 0, producer, attempt)) {
            for (String batch : batches) writer.write(bytes(batch));
            writer.commit();
        }
    }
    private List<String> read(Map<Integer, Long> attempts) {
        var result = new ArrayList<String>();
        try (var reader = store.read(selection(attempts))) {
            while (reader.hasNext()) result.add(text(reader.next()));
        }
        return result;
    }
    private SpoolReadSelection selection(Map<Integer, Long> attempts) { return new SpoolReadSelection("q", 0, 0, attempts); }
    private Path segment(int producer, long attempt) { return directory.resolve("q/s0/p0/m" + producer + "-a" + attempt + ".spool"); }
    private static ByteBuffer bytes(String text) { return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)); }
    private static String text(ByteBuffer data) { return StandardCharsets.UTF_8.decode(data).toString(); }
}
