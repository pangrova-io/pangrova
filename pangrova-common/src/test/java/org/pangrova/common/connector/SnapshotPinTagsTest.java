package org.pangrova.common.connector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class SnapshotPinTagsTest {
    @Test
    void persistentNamesUseTheFullUtf8IdentityAndOpaqueSnapshot() {
        String prefix = "pangrova_index_5f39c6d9f5fd069a7d6a2a74e5d0a8d66d10bfb08cd1854110a4a0faad8f2bc1";
        assertThat(SnapshotPinTags.indexPrefix("索引/a")).isEqualTo(prefix);
        assertThat(SnapshotPinTags.forSnapshot("索引/a", 7, "attempt-1", "snapshot/opaque"))
                .isEqualTo(prefix + "_v2_7_617474656d70742d31_736e617073686f742f6f7061717565");
        assertThat(SnapshotPinTags.indexPrefix(" 索引/a")).isNotEqualTo(prefix);
        assertThat(SnapshotPinTags.decode(
                SnapshotPinTags.forSnapshot("索引/a", 7, "attempt-1", "snapshot/opaque"), "索引/a"))
                .isEqualTo(new SnapshotPinTags.DecodedSnapshotPin(7, "attempt-1", "snapshot/opaque"));
    }

    @Test
    void rejectsAnAbsentIndexBeforeNamingAnySnapshot() {
        for (String id : new String[] {null, "", "  "}) {
            assertThatIllegalArgumentException().isThrownBy(() -> SnapshotPinTags.forSnapshot(id, 1, "attempt", "snapshot"))
                    .withMessage("Index ID is required for snapshot pinning");
        }
    }
}
