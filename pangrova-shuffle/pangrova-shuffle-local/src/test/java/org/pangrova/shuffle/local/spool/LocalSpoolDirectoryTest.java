package org.pangrova.shuffle.local.spool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.shuffle.api.spool.SpoolReadSelection;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalSpoolDirectoryTest {
    @TempDir Path directory;

    @Test
    void allSelectedSegmentsArePinnedBeforeTheFirstBatch() throws Exception {
        try (var store = new LocalFsSpoolStore(directory)) {
            write(store, 0, "first");
            write(store, 1, "second");
            try (var reader = store.read(selection(Map.of(0, 0L, 1, 0L)))) {
                Path second = segment(1);
                Files.move(second, directory.resolve("detached.spool"));
                Files.copy(segment(0), second);
                assertThat(text(reader.next())).isEqualTo("first");
                assertThat(text(reader.next())).isEqualTo("second");
                assertThat(reader.hasNext()).isFalse();
            }
        }
    }

    @Test
    void replacedPartitionCannotReceivePublicationOrLoseForeignTemporaryData() throws Exception {
        try (var store = new LocalFsSpoolStore(directory)) {
            var writer = store.open("q", 0, 0, 0, 0);
            writer.write(bytes("candidate"));
            Path partition = directory.resolve("q/s0/p0");
            Path temporary;
            try (var files = Files.list(partition)) {
                temporary = files.filter(p -> p.toString().endsWith(".tmp")).findFirst().orElseThrow();
            }
            Path detached = directory.resolve("detached");
            Files.move(partition, detached);
            Files.createDirectory(partition);
            Path foreign = partition.resolve(temporary.getFileName());
            Files.writeString(foreign, "foreign");
            try {
                assertThatThrownBy(writer::commit).isInstanceOf(UncheckedIOException.class);
                assertThatThrownBy(writer::close).isInstanceOf(UncheckedIOException.class);
                assertThat(segment(0)).doesNotExist();
                assertThat(Files.readString(foreign)).isEqualTo("foreign");
                assertThat(detached.resolve(temporary.getFileName())).exists();
            } finally {
                Files.delete(foreign);
                Files.delete(partition);
                Files.move(detached, partition);
                writer.close();
            }
            try (var files = Files.list(partition)) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }

    @Test
    void replacedRootIsRejectedBeforeCreatingOrDeletingData() throws Exception {
        Path root = directory.resolve("root");
        Path detached = directory.resolve("detached");
        try (var store = new LocalFsSpoolStore(root)) {
            Files.move(root, detached);
            Files.createDirectory(root);
            Files.createDirectories(root.resolve("q"));
            Files.writeString(root.resolve("q/foreign"), "retained");
            try {
                assertThatThrownBy(() -> store.open("q", 0, 0, 0, 0)).isInstanceOf(UncheckedIOException.class);
                assertThatThrownBy(() -> store.read(selection(Map.of()))).isInstanceOf(UncheckedIOException.class);
                assertThatThrownBy(() -> store.delete("q")).isInstanceOf(IllegalStateException.class);
                assertThat(Files.readString(root.resolve("q/foreign"))).isEqualTo("retained");
                assertThat(root.resolve("q/s0")).doesNotExist();
            } finally {
                Files.delete(root.resolve("q/foreign"));
                Files.delete(root.resolve("q"));
                Files.delete(root);
                Files.move(detached, root);
            }
        }
    }

    @Test
    void symlinkCompletedSegmentCannotRedirectASelection() throws Exception {
        try (var store = new LocalFsSpoolStore(directory, 1)) {
            write(store, 0, "selected");
            Path selected = segment(0);
            Path detached = directory.resolve("detached.spool");
            Files.move(selected, detached);
            Files.createSymbolicLink(selected, detached);
            assertThatThrownBy(() -> store.read(selection(Map.of(0, 0L)))).isInstanceOf(IllegalStateException.class);
            Files.delete(selected);
            Files.move(detached, selected);
            try (var reader = store.read(selection(Map.of(0, 0L)))) {
                assertThat(text(reader.next())).isEqualTo("selected");
            }
        }
    }

    @Test
    void budgetIsSharedByWritersAndPinnedReadersAndReleasedOnlyOnClose() {
        try (var store = new LocalFsSpoolStore(directory, 2)) {
            write(store, 0, "selected");
            try (var writer = store.open("q", 0, 0, 1, 0)) {
                var reader = store.read(selection(Map.of(0, 0L)));
                try {
                    assertThat(text(reader.next())).isEqualTo("selected");
                    assertThat(reader.hasNext()).isFalse();
                    assertThatThrownBy(() -> store.open("new", 0, 0, 0, 0))
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("budget");
                    assertThat(directory.resolve("new")).doesNotExist();
                } finally {
                    reader.close();
                    reader.close();
                }
                try (var another = store.open("q", 0, 0, 2, 0)) {
                    another.commit();
                }
                writer.commit();
            }
        }
    }

    @Test
    void partiallyPinnedFailureReturnsTheWholeReservation() {
        try (var store = new LocalFsSpoolStore(directory, 2)) {
            write(store, 0, "selected");
            for (int i = 0; i < 20; i++) {
                assertThatThrownBy(() -> store.read(selection(Map.of(0, 0L, 1, 0L))))
                        .isInstanceOf(IllegalStateException.class);
            }
            try (var reader = store.read(selection(Map.of(0, 0L)));
                 var writer = store.open("q", 0, 0, 1, 0)) {
                assertThat(text(reader.next())).isEqualTo("selected");
                writer.commit();
            }
        }
    }

    @Test
    void oversizedSelectionIsRejectedWithoutDroppingProducers() {
        try (var store = new LocalFsSpoolStore(directory, 1)) {
            write(store, 0, "first");
            write(store, 1, "second");
            assertThatThrownBy(() -> store.read(selection(Map.of(0, 0L, 1, 0L))))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("budget");
            try (var reader = store.read(selection(Map.of(1, 0L)))) {
                assertThat(text(reader.next())).isEqualTo("second");
            }
        }
    }

    @Test
    void factoryValidatesAndAppliesItsFileBudget() {
        var factory = new LocalFsSpoolStoreFactory();
        Path root = directory.resolve("configured");
        for (String invalid : new String[]{"0", "-1", "invalid"}) {
            assertThatThrownBy(() -> factory.create(new SpoolStoreContext("local-fs", Map.of(
                    "spool.local-fs.base-dir", root.toString(), "spool.local-fs.max-open-files", invalid))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(root).doesNotExist();
        }
        try (var store = factory.create(new SpoolStoreContext("local-fs", Map.of(
                "spool.local-fs.base-dir", root.toString(), "spool.local-fs.max-open-files", "1")));
             var writer = store.open("q", 0, 0, 0, 0)) {
            assertThatThrownBy(() -> store.open("q", 0, 0, 1, 0))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("budget");
            writer.commit();
        }
    }

    @Test
    void pathOwnerRejectsOutsideOrNonNormalizedParents() throws Exception {
        var paths = new LocalSpoolPaths(directory);
        assertThatThrownBy(() -> paths.requireParents(directory.resolve("../outside")))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> paths.requireParents(Path.of("relative"))).isInstanceOf(IOException.class);
    }

    private Path segment(int producer) {
        return directory.resolve("q/s0/p0/m" + producer + "-a0.spool");
    }

    private static void write(LocalFsSpoolStore store, int producer, String value) {
        try (var writer = store.open("q", 0, 0, producer, 0)) {
            writer.write(bytes(value));
            writer.commit();
        }
    }

    private static SpoolReadSelection selection(Map<Integer, Long> attempts) {
        return new SpoolReadSelection("q", 0, 0, attempts);
    }

    private static ByteBuffer bytes(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(ByteBuffer value) {
        return StandardCharsets.UTF_8.decode(value).toString();
    }
}
