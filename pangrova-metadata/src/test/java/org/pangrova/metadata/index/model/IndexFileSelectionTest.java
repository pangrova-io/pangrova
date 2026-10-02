package org.pangrova.metadata.index.model;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexFileSelectionTest {

    @Test
    void matchesExactGenerationAndOpaqueSnapshot() {
        IndexFileSelection selection = new IndexFileSelection(7, "snapshot-A9");

        assertThat(selection.matches(mapping(7, "object-created-at-A1"))).isTrue();
        assertThat(selection.matches(mapping(6, "object-created-at-A1"))).isFalse();
        IndexArtifactFormat format = new IndexArtifactFormat(
                "format", 1, 0, "b".repeat(64), "writer");
        IndexGenerationManifest active = new IndexGenerationManifest(
                "idx", "idx", 7, "snapshot-A9", "VECTOR", format,
                1, 1, 10, "c".repeat(64), 8, Instant.EPOCH);
        IndexGenerationManifest wrongSnapshot = new IndexGenerationManifest(
                "idx", "idx", 7, "snapshot-A10", "VECTOR", format,
                1, 1, 10, "c".repeat(64), 8, Instant.EPOCH);
        assertThat(selection.matches(active)).isTrue();
        assertThat(selection.matches(wrongSnapshot)).isFalse();
    }

    @Test
    void validatesStrongArtifactIdentity() {
        assertThatThrownBy(() -> new IndexFileMapping(
                "idx", "data", "artifact", 1, 10, "crc32",
                1, "snapshot", 0, Instant.EPOCH, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
    }

    private static IndexFileMapping mapping(int generation, String snapshot) {
        return new IndexFileMapping(
                "idx", "data", "artifact", 1, 10, "a".repeat(64),
                generation, snapshot, 0, Instant.EPOCH, 0);
    }
}
