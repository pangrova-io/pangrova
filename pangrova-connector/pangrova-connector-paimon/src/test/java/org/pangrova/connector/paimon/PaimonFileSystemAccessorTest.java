package org.pangrova.connector.paimon;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.paimon.fs.PaimonFileSystemAccessor;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonFileSystemAccessorTest {
    @Test
    void failedDeletionIsObservableAndAbsentFileIsIdempotent(@TempDir java.nio.file.Path root)
            throws IOException {
        var file = Files.write(root.resolve("index"), new byte[] {1});
        var accessor = new PaimonFileSystemAccessor(new LocalFileIO() {
            @Override
            public boolean delete(Path path, boolean recursive) {
                return false;
            }
        });
        assertThatThrownBy(() -> accessor.deleteFile(file.toString()))
                .isInstanceOf(IOException.class).hasMessageContaining("could not delete");
        assertThat(file).exists();
        var local = new PaimonFileSystemAccessor(new LocalFileIO());
        local.deleteFile(file.toString());
        local.deleteFile(file.toString());
        assertThat(file).doesNotExist();
    }
}
