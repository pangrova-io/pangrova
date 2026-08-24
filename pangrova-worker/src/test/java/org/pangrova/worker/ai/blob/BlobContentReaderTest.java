package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies BlobContentReader: read path, size verification, streaming,
 * and error handling.
 */
class BlobContentReaderTest {

    private final InMemoryFileSystemAccessor storage = new InMemoryFileSystemAccessor();

    @Test
    void readReturnsStoredContent() {
        byte[] content = "hello blob content".getBytes(StandardCharsets.UTF_8);
        String uri = "s3://bucket/table/blob/col/abc-123.txt";
        storage.openOutputStreamAndWrite(uri, content);

        BlobContentReader reader = new BlobContentReader(storage);
        BlobReference ref = new BlobReference(uri, "text/plain", content.length, "a".repeat(64));

        byte[] result = reader.read(ref);

        assertThat(result).isEqualTo(content);
    }

    @Test
    void readDetectsSizeMismatch() {
        byte[] content = "short".getBytes(StandardCharsets.UTF_8);
        String uri = "s3://bucket/table/blob/col/abc.txt";
        storage.openOutputStreamAndWrite(uri, content);

        BlobContentReader reader = new BlobContentReader(storage);
        BlobReference ref = new BlobReference(uri, "text/plain", 999, "b".repeat(64));

        assertThatThrownBy(() -> reader.read(ref))
                .isInstanceOf(PangrovaException.class)
                .satisfies(e -> {
                    PangrovaException ae = (PangrovaException) e;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_READ_ERROR);
                    assertThat(ae.getMessage()).contains("size mismatch");
                    assertThat(ae.getMessage()).contains("999");
                    assertThat(ae.getMessage()).contains(uri);
                });
    }

    @Test
    void readWrapsStorageFailure() {
        storage.setFailOnRead(true);
        BlobContentReader reader = new BlobContentReader(storage);
        BlobReference ref = new BlobReference("s3://b/t/blob/c/x.bin", "application/octet-stream", 10, "c".repeat(64));

        assertThatThrownBy(() -> reader.read(ref))
                .isInstanceOf(PangrovaException.class)
                .satisfies(e -> {
                    PangrovaException ae = (PangrovaException) e;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_READ_ERROR);
                    assertThat(ae.getMessage()).contains("s3://b/t/blob/c/x.bin");
                });
    }

    @Test
    void openStreamReturnsInputStream() throws IOException {
        byte[] content = "streaming content".getBytes(StandardCharsets.UTF_8);
        String uri = "s3://bucket/table/blob/col/stream.txt";
        storage.openOutputStreamAndWrite(uri, content);

        BlobContentReader reader = new BlobContentReader(storage);
        BlobReference ref = new BlobReference(uri, "text/plain", content.length, "d".repeat(64));

        try (InputStream stream = reader.openStream(ref)) {
            byte[] result = stream.readAllBytes();
            assertThat(result).isEqualTo(content);
        }
    }

    @Test
    void openStreamWrapsStorageFailure() {
        storage.setFailOnRead(true);
        BlobContentReader reader = new BlobContentReader(storage);
        BlobReference ref = new BlobReference("s3://b/t/blob/c/x.bin", "application/octet-stream", 10, "e".repeat(64));

        assertThatThrownBy(() -> reader.openStream(ref))
                .isInstanceOf(PangrovaException.class)
                .satisfies(e -> {
                    PangrovaException ae = (PangrovaException) e;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_READ_ERROR);
                    assertThat(ae.getMessage()).contains("s3://b/t/blob/c/x.bin");
                });
    }

    @Test
    void constructorRejectsNullStorage() {
        assertThatThrownBy(() -> new BlobContentReader(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readRejectsNullReference() {
        BlobContentReader reader = new BlobContentReader(storage);

        assertThatThrownBy(() -> reader.read(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void openStreamRejectsNullReference() {
        BlobContentReader reader = new BlobContentReader(storage);

        assertThatThrownBy(() -> reader.openStream(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void roundTripWithWriter() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        BlobContentReader reader = new BlobContentReader(storage);

        byte[] original = "round trip content for blob storage".getBytes(StandardCharsets.UTF_8);
        BlobReference ref = writer.write("s3://bucket/table", "doc_col", original, "text/plain");

        byte[] retrieved = reader.read(ref);

        assertThat(retrieved).isEqualTo(original);
    }
}
