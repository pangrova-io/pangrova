package org.pangrova.batch.api.page;

import org.pangrova.batch.api.enums.BatchFormat;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativePageOwnershipTest {
    @Test
    void closeRejectsNewPayloadReadsButDoesNotInvalidateCallerSnapshotsOrMetadata() {
        NativePage page = new NativePage(new byte[]{1, 2, 3}, 2, 1, BatchFormat.CUSTOM_BINARY);
        byte[] snapshot = page.serializedBatch();

        page.close();
        page.close();

        assertThat(page.isClosed()).isTrue();
        assertThatThrownBy(page::serializedBatch).isInstanceOf(IllegalStateException.class);
        assertThat(snapshot).containsExactly(1, 2, 3);
        assertThat(page.serializedSizeInBytes()).isEqualTo(3);
        assertThat(page.getRowCount()).isEqualTo(2);
        assertThat(page.getColumnCount()).isEqualTo(1);
        assertThat(page.format()).isEqualTo(BatchFormat.CUSTOM_BINARY);
    }

    @Test
    void emptyPayloadHasZeroEncodedSize() {
        try (NativePage page = new NativePage(new byte[0], 0, 0, BatchFormat.CUSTOM_BINARY)) {
            assertThat(page.serializedBatch()).isEmpty();
            assertThat(page.serializedSizeInBytes()).isZero();
            assertThat(page.isEmpty()).isTrue();
        }
    }
}
