package org.pangrova.worker.nativebridge.jni.write;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteCommitResultTest {
    @Test
    void preservesPublishedCommitFragmentsAcrossCallerMutations() {
        byte[] fragments = {1, 2, 3};
        var paths = new ArrayList<>(List.of("data-1.parquet"));
        var result = new WriteCommitResult(7, 11, paths, fragments);
        fragments[0] = 9;
        paths.clear();
        byte[] returned = result.commitMetadata();
        returned[1] = 9;
        assertThat(result.commitMetadata()).containsExactly((byte) 1, (byte) 2, (byte) 3);
        assertThat(result.filePathsCreated()).containsExactly("data-1.parquet");
        assertThatThrownBy(() -> result.filePathsCreated().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.rowsWritten()).isEqualTo(7);
        assertThat(result.bytesWritten()).isEqualTo(11);
    }

    @Test
    void absentFragmentsRemainAnEmptyCallerOwnedArray() {
        var result = new WriteCommitResult(0, 0, null, null);
        assertThat(result.commitMetadata()).isEmpty();
        assertThat(result.commitMetadata()).isNotSameAs(result.commitMetadata());
        assertThat(result.filePathsCreated()).isEmpty();
    }
}
