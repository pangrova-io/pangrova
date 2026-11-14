package org.pangrova.common.connector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class SnapshotPinTagsTest {
    @Test
    void persistentNamesUseTheFullUtf8IdentityAndDecimalSnapshot() {
        String prefix = "pangrova_index_5f39c6d9f5fd069a7d6a2a74e5d0a8d66d10bfb08cd1854110a4a0faad8f2bc1";
        assertThat(SnapshotPinTags.indexPrefix("索引/a")).isEqualTo(prefix);
        assertThat(SnapshotPinTags.forSnapshot("索引/a", Long.MAX_VALUE))
                .isEqualTo(prefix + "_9223372036854775807");
        assertThat(SnapshotPinTags.indexPrefix(" 索引/a")).isNotEqualTo(prefix);
    }

    @Test
    void rejectsAnAbsentIndexBeforeNamingAnySnapshot() {
        for (String id : new String[] {null, "", "  "}) {
            assertThatIllegalArgumentException().isThrownBy(() -> SnapshotPinTags.forSnapshot(id, 1))
                    .withMessage("Index ID is required for snapshot pinning");
        }
    }
}
