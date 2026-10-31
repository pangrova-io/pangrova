package org.pangrova.indexing.core.build;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BuildFileMappingTest {

    @Test
    void acceptsSha256ArtifactDigest() {
        BuildFileMapping mapping = new BuildFileMapping(
                "idx", "data", "artifact", 2, 128, "a".repeat(64));

        assertThat(mapping.checksum()).hasSize(64);
    }

    @Test
    void rejectsWeakArtifactDigest() {
        assertThatThrownBy(() -> new BuildFileMapping(
                "idx", "data", "artifact", 2, 128, "1234"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
    }
}
