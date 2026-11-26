package org.pangrova.common.connector;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class SnapshotPinTagsTest {
    @Test
    void persistentNamesUseTheFullUtf8IdentityAndOpaqueSnapshot() {
        String prefix = "pangrova_index_5f39c6d9f5fd069a7d6a2a74e5d0a8d66d10bfb08cd1854110a4a0faad8f2bc1";
        assertThat(SnapshotPinTags.indexPrefix("索引/a")).isEqualTo(prefix);
        assertThat(SnapshotPinTags.forSnapshot("索引/a", 7, "attempt-1", "snapshot/opaque"))
                .isEqualTo(prefix + "_v3_7_617474656d70742d31_736e617073686f742f6f7061717565");
        assertThat(SnapshotPinTags.indexPrefix(" 索引/a")).isNotEqualTo(prefix);
        assertThat(SnapshotPinTags.decode(
                SnapshotPinTags.forSnapshot("索引/a", 7, "attempt-1", "snapshot/opaque"), "索引/a"))
                .isEqualTo(new SnapshotPinTags.DecodedSnapshotPin(7, "attempt-1", "snapshot/opaque"));
    }

    @Test
    void readsBothHistoricalV2EncodingsWithoutChangingTheirIdentity() {
        String prefix = SnapshotPinTags.indexPrefix("index-1");
        String oldBase64 = prefix + "_v2_7_YXR0ZW1wdC0x_NDI=";
        String interimHex = prefix + "_v2_7_617474656d70742d31_3432";
        String current = prefix + "_v3_7_617474656d70742d31_3432";
        var expected = new SnapshotPinTags.DecodedSnapshotPin(7, "attempt-1", "42");
        for (String name : List.of(oldBase64, interimHex, current)) {
            assertThat(SnapshotPinTags.decode(name, "index-1")).isEqualTo(expected);
            assertThat(SnapshotPinTags.decodeAny(name)).isEqualTo(expected);
            assertThat(SnapshotPinTags.legacySnapshotToken(name, "index-1")).isNull();
            assertThat(SnapshotPinTags.decode(name, "index-2")).isNull();
        }
        assertThat(SnapshotPinTags.matchingNames("index-1", 7, "attempt-1", "42",
                name -> List.of(oldBase64, interimHex, current).contains(name)))
                .containsExactly(current, oldBase64, interimHex);
    }

    @Test
    void rejectsAnAbsentIndexBeforeNamingAnySnapshot() {
        for (String id : new String[] {null, "", "  "}) {
            assertThatIllegalArgumentException().isThrownBy(() -> SnapshotPinTags.forSnapshot(id, 1, "attempt", "snapshot"))
                    .withMessage("Index ID is required for snapshot pinning");
        }
    }
}
