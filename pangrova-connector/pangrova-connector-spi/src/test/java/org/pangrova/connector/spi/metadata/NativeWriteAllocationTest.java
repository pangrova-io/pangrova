package org.pangrova.connector.spi.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeWriteAllocationTest {
    @Test
    void copiesSchemaFenceAndRequiresExactVersionAndColumns() {
        byte[] fence = new byte[32];
        var allocation = allocation(1, "staging/write-1", List.of("id"), fence);
        fence[0] = 7;
        allocation.schemaFenceSha256()[1] = 7;
        assertThat(allocation.schemaFenceSha256()).containsOnly((byte) 0);
        assertThatThrownBy(() -> allocation(0, "staging/write-1", List.of("id"), fence))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> allocation(1, "../write-1", List.of("id"), fence))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> allocation(1, "staging/write-1", List.of("id", "id"), fence))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> allocation(1, "staging/write-1", List.of(" "), fence))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void operationAndVersionAreOneExactTransactionContract() {
        for (var operation : NativeWriteAllocation.Operation.values()) {
            for (long version : List.of(-1L, 0L, 1L, Long.MAX_VALUE)) {
                boolean valid = version >= 0 && (operation == NativeWriteAllocation.Operation.CREATE) == (version == 0);
                if (valid) {
                    assertThat(new NativeWriteAllocation("s3://warehouse/table.lance", operation, version, new byte[32],
                            "write-1", "staging/write-1", "generation-1", List.of("id")).readVersion()).isEqualTo(version);
                } else {
                    assertThatThrownBy(() -> new NativeWriteAllocation("s3://warehouse/table.lance", operation,
                            version, new byte[32], "write-1", "staging/write-1", "generation-1", List.of("id")))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("version");
                }
            }
        }
        assertThatThrownBy(() -> new NativeWriteAllocation("s3://warehouse/table.lance", null, 1, new byte[32],
                "write-1", "staging/write-1", "generation-1", List.of("id")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("operation");
    }

    @Test
    void schemaFenceRejectsNullAndWrongDigestLengths() {
        assertThatThrownBy(() -> allocation(1, "staging", List.of("id"), null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("schemaFenceSha256");
        for (int length : List.of(0, 31, 33)) {
            assertThatThrownBy(() -> allocation(1, "staging", List.of("id"), new byte[length]))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("32 bytes");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", ".", "..", "./staging", "stage/../escape", "stage/./child",
            "/absolute", "stage//child", "stage/", "stage/%2e%2e", "stage\\child", "stage?token", "stage#fragment"})
    void stagingPrefixCannotEscapeTheAllocatedDataset(String prefix) {
        assertThatThrownBy(() -> allocation(1, prefix, List.of("id"), new byte[32]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("stagingPrefix");
    }

    @Test
    void prefixLengthBoundaryAndCanonicalSegmentsArePreserved() {
        String maximum = "a".repeat(2048);
        assertThat(allocation(1, maximum, List.of("id"), new byte[32]).stagingPrefix()).isEqualTo(maximum);
        assertThat(allocation(1, "stage-a_b.c=1/.hidden", List.of("id"), new byte[32]).stagingPrefix())
                .isEqualTo("stage-a_b.c=1/.hidden");
        assertThatThrownBy(() -> allocation(1, maximum + "a", List.of("id"), new byte[32]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("normalized");
        assertThatThrownBy(() -> allocation(1, null, List.of("id"), new byte[32]))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("stagingPrefix");
    }

    @Test
    void pathCharactersRemainExactlyAsciiAndLongInvalidPrefixesAreRejected() {
        String allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_=-";
        for (char value = 0; value < 256; value++) {
            String prefix = String.valueOf(value);
            if (allowed.indexOf(value) >= 0) {
                assertThat(allocation(1, prefix, List.of("id"), new byte[32]).stagingPrefix())
                        .isEqualTo(prefix);
            } else {
                assertThatThrownBy(() -> allocation(1, prefix, List.of("id"), new byte[32]))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
        for (String prefix : List.of("a/".repeat(1023) + "a!", "a".repeat(2047) + "!",
                "stage/\u4e2d", "stage/\ud83d\ude80")) {
            assertThatThrownBy(() -> allocation(1, prefix, List.of("id"), new byte[32]))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("stagingPrefix");
        }
    }

    @Test
    void columnsAreDefensivelyCopiedAndNeverMissingBlankOrDuplicated() {
        var columns = new ArrayList<>(List.of("id", "value"));
        var allocation = allocation(1, "staging", columns, new byte[32]);
        columns.clear();
        assertThat(allocation.writeColumnNames()).containsExactly("id", "value");
        assertThatThrownBy(() -> allocation.writeColumnNames().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> allocation(1, "staging", null, new byte[32])).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> allocation(1, "staging", Arrays.asList("id", null), new byte[32]))
                .isInstanceOf(NullPointerException.class);
        for (List<String> invalid : List.of(List.<String>of(), List.of("id", "id"), List.of(" "))) {
            assertThatThrownBy(() -> allocation(1, "staging", invalid, new byte[32]))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nonempty, unique and nonblank");
        }
    }

    @Test
    void datasetWriteAndGenerationIdentitiesRejectMissingValues() {
        for (String invalid : Arrays.asList(null, "", " ")) {
            assertThatThrownBy(() -> new NativeWriteAllocation(invalid, NativeWriteAllocation.Operation.APPEND,
                    1, new byte[32], "write", "stage", "generation", List.of("id")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("datasetUri");
            assertThatThrownBy(() -> new NativeWriteAllocation("dataset", NativeWriteAllocation.Operation.APPEND,
                    1, new byte[32], invalid, "stage", "generation", List.of("id")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("writeId");
            assertThatThrownBy(() -> new NativeWriteAllocation("dataset", NativeWriteAllocation.Operation.APPEND,
                    1, new byte[32], "write", "stage", invalid, List.of("id")))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("generation");
        }
    }

    private static NativeWriteAllocation allocation(long version, String prefix, List<String> columns, byte[] fence) {
        return new NativeWriteAllocation("s3://warehouse/table.lance", NativeWriteAllocation.Operation.APPEND,
                version, fence, "write-1", prefix, "generation-1", columns);
    }
}
