package org.pangrova.testing.testkit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual owned startup tar buffer, not a simulated container or business collaborator. */
class StartupArchiveTest {
    @Test
    void rejectsBothWriteOverloadsBeforeTheyCanExceedTheActualLimit() {
        var archive = new RuntimeUserContainer.StartupArchive();
        try {
            byte[] maximum = new byte[RuntimeUserContainer.MAX_STARTUP_ARCHIVE_BYTES];
            archive.write(maximum, 0, maximum.length);
            assertThat(archive.size()).isEqualTo(maximum.length);
            assertThatThrownBy(() -> archive.write(1)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> archive.write(new byte[] {1}, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(archive.size()).isEqualTo(maximum.length);
        } finally {
            archive.erase();
        }
    }

    @Test
    void erasesTheExactBufferStillHeldByItsInputView() throws Exception {
        var archive = new RuntimeUserContainer.StartupArchive();
        try {
            archive.write(new byte[] {3, 4, 5}, 0, 3);
            try (var view = archive.openInput()) {
                archive.erase();
                assertThat(view.readAllBytes()).containsExactly((byte) 0, (byte) 0, (byte) 0);
                assertThat(archive.size()).isZero();
            }
        } finally {
            archive.erase();
        }
    }
}
