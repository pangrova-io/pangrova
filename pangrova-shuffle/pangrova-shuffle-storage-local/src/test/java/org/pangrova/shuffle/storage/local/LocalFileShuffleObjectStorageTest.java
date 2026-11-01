package org.pangrova.shuffle.storage.local;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalFileShuffleObjectStorageTest {

    @TempDir
    Path tempDir;

    @Test
    void storesAndReadsBackImmutableObject() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            byte[] data = "batch-bytes".getBytes(StandardCharsets.UTF_8);
            objects.putImmutable("q1/batch-1", () -> new ByteArrayInputStream(data),
                    data.length, Sha256.digest(data));
            assertThat(objects.get("q1/batch-1")).isEqualTo(data);
        }
    }

    @Test
    void rePutWithIdenticalContentIsIdempotent() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            byte[] data = "batch-bytes".getBytes(StandardCharsets.UTF_8);
            objects.putImmutable("q1/batch-1", () -> new ByteArrayInputStream(data),
                    data.length, Sha256.digest(data));
            objects.putImmutable("q1/batch-1", () -> new ByteArrayInputStream(data),
                    data.length, Sha256.digest(data));
            assertThat(objects.get("q1/batch-1")).isEqualTo(data);
        }
    }

    @Test
    void conflictingContentFailsFast() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            byte[] data = "batch-bytes".getBytes(StandardCharsets.UTF_8);
            objects.putImmutable("q1/batch-1", () -> new ByteArrayInputStream(data),
                    data.length, Sha256.digest(data));
            byte[] other = "other-bytes".getBytes(StandardCharsets.UTF_8);
            assertThatThrownBy(() -> objects.putImmutable("q1/batch-1",
                    () -> new ByteArrayInputStream(other), other.length, Sha256.digest(other)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsContentThatDoesNotMatchDeclaredChecksum() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            byte[] data = "batch-bytes".getBytes(StandardCharsets.UTF_8);
            byte[] wrong = Sha256.digest("different".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> objects.putImmutable("q1/batch-1",
                    () -> new ByteArrayInputStream(data), data.length, wrong))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> objects.get("q1/batch-1"))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void verifyStoredChecksLengthAndChecksum() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            byte[] data = "batch-bytes".getBytes(StandardCharsets.UTF_8);
            objects.putImmutable("q1/batch-1", () -> new ByteArrayInputStream(data),
                    data.length, Sha256.digest(data));
            objects.verifyStored("q1/batch-1", data.length, Sha256.digest(data));
            assertThatThrownBy(() -> objects.verifyStored("q1/batch-1", data.length + 1,
                    Sha256.digest(data))).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> objects.verifyStored("q1/missing", 0,
                    Sha256.digest(data))).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void listPrefixReturnsKeysInLexicographicOrder() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            for (int i = 0; i < 3; i++) {
                byte[] data = ("batch-" + i).getBytes(StandardCharsets.UTF_8);
                objects.putImmutable("q1/batch-" + i, () -> new ByteArrayInputStream(data),
                        data.length, Sha256.digest(data));
            }
            assertThat(objects.listPrefix("q1/"))
                    .containsExactly("q1/batch-0", "q1/batch-1", "q1/batch-2");
            assertThat(objects.listPrefix("q2/")).isEmpty();
        }
    }

    @Test
    void deletePrefixPagePagesThroughKeys() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            for (int i = 0; i < 5; i++) {
                byte[] data = ("batch-" + i).getBytes(StandardCharsets.UTF_8);
                objects.putImmutable("q1/batch-" + i, () -> new ByteArrayInputStream(data),
                        data.length, Sha256.digest(data));
            }
            int pages = 0;
            String after = null;
            do {
                after = objects.deletePrefixPage("q1/", after, 2);
                pages++;
            } while (after != null);
            assertThat(pages).isEqualTo(3);
            assertThatThrownBy(() -> objects.get("q1/batch-0"))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void rejectsPathTraversalKeys() {
        try (LocalFileShuffleObjectStorage objects =
                     new LocalFileShuffleObjectStorage(tempDir, "shuffle")) {
            byte[] data = "batch".getBytes(StandardCharsets.UTF_8);
            for (String bad : List.of("../escape", "a/../../b", "/absolute", "a\\b")) {
                assertThatThrownBy(() -> objects.putImmutable(bad,
                        () -> new ByteArrayInputStream(data), data.length, Sha256.digest(data)))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }
}
