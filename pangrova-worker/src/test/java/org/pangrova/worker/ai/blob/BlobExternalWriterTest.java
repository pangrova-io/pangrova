package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies BlobExternalWriter: write path, SHA-256 checksum, max size enforcement,
 * extension resolution, and error handling.
 */
class BlobExternalWriterTest {

    private final InMemoryFileSystemAccessor storage = new InMemoryFileSystemAccessor();

    @Test
    void writeStoresContentAndReturnsBlobReference() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = "hello blob".getBytes(StandardCharsets.UTF_8);

        BlobReference ref = writer.write("s3://bucket/table", "image_col", content, "text/plain");

        assertThat(ref.uri()).startsWith("s3://bucket/table/blob/image_col/");
        assertThat(ref.uri()).endsWith(".txt");
        assertThat(ref.mimeType()).isEqualTo("text/plain");
        assertThat(ref.size()).isEqualTo(content.length);
        assertThat(ref.checksum()).isNotBlank();
        assertThat(ref.checksum()).hasSize(64); // SHA-256 hex = 64 chars

        // Verify content was actually written to storage
        assertThat(storage.readAll(ref.uri())).isEqualTo(content);
    }

    @Test
    void writeSha256ChecksumIsCorrect() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

        BlobReference ref = writer.write("s3://bucket/t", "col", content, "text/plain");

        // Independently compute expected SHA-256
        String expected = BlobExternalWriter.computeSha256(content);
        assertThat(ref.checksum()).isEqualTo(expected);
    }

    @Test
    void writeEnforcesMaxSize() {
        long maxSize = 100;
        BlobExternalWriter writer = new BlobExternalWriter(storage, maxSize);
        byte[] content = new byte[101];

        assertThatThrownBy(() -> writer.write("s3://bucket/t", "col", content, "application/octet-stream"))
                .isInstanceOf(PangrovaException.class)
                .satisfies(e -> {
                    PangrovaException ae = (PangrovaException) e;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.AI_BLOB_SIZE_EXCEEDED);
                    assertThat(ae.getMessage()).contains("101");
                    assertThat(ae.getMessage()).contains("100");
                });

        // Verify nothing was written
        assertThat(storage.fileCount()).isZero();
    }

    @Test
    void writeAtExactMaxSizeSucceeds() {
        long maxSize = 100;
        BlobExternalWriter writer = new BlobExternalWriter(storage, maxSize);
        byte[] content = new byte[100];

        BlobReference ref = writer.write("s3://bucket/t", "col", content, "application/octet-stream");

        assertThat(ref.size()).isEqualTo(100);
    }

    @Test
    void writeHandlesTrailingSlashInTableLocation() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = "data".getBytes(StandardCharsets.UTF_8);

        BlobReference ref = writer.write("s3://bucket/table/", "col", content, "image/jpeg");

        assertThat(ref.uri()).startsWith("s3://bucket/table/blob/col/");
        assertThat(ref.uri()).doesNotContain("//blob");
    }

    @Test
    void writeResolvesJpegExtension() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = new byte[]{(byte) 0xFF, (byte) 0xD8};

        BlobReference ref = writer.write("s3://b/t", "img", content, "image/jpeg");

        assertThat(ref.uri()).endsWith(".jpg");
    }

    @Test
    void writeResolvesPngExtension() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = new byte[]{(byte) 0x89, 0x50};

        BlobReference ref = writer.write("s3://b/t", "img", content, "image/png");

        assertThat(ref.uri()).endsWith(".png");
    }

    @Test
    void writeResolvesUnknownMimeTypeToSubtype() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = "data".getBytes(StandardCharsets.UTF_8);

        BlobReference ref = writer.write("s3://b/t", "doc", content, "application/pdf");

        assertThat(ref.uri()).endsWith(".pdf");
    }

    @Test
    void writeWrapsStorageFailure() {
        InMemoryFileSystemAccessor failingStorage = new InMemoryFileSystemAccessor();
        failingStorage.setFailOnWrite(true);
        BlobExternalWriter writer = new BlobExternalWriter(failingStorage);
        byte[] content = "data".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> writer.write("s3://b/t", "col", content, "text/plain"))
                .isInstanceOf(PangrovaException.class)
                .satisfies(e -> {
                    PangrovaException ae = (PangrovaException) e;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_WRITE_ERROR);
                    assertThat(ae.getMessage()).contains("col");
                });
    }

    @Test
    void constructorRejectsNullStorage() {
        assertThatThrownBy(() -> new BlobExternalWriter(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsNonPositiveMaxSize() {
        assertThatThrownBy(() -> new BlobExternalWriter(storage, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BlobExternalWriter(storage, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writeRejectsNullOrBlankArguments() {
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        byte[] content = "x".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> writer.write(null, "col", content, "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> writer.write("  ", "col", content, "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> writer.write("s3://b/t", null, content, "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> writer.write("s3://b/t", "col", null, "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> writer.write("s3://b/t", "col", new byte[0], "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> writer.write("s3://b/t", "col", content, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resolveExtensionHandlesEdgeCases() {
        assertThat(BlobExternalWriter.resolveExtension("image/jpeg")).isEqualTo("jpg");
        assertThat(BlobExternalWriter.resolveExtension("text/plain")).isEqualTo("txt");
        assertThat(BlobExternalWriter.resolveExtension("image/svg+xml")).isEqualTo("svg");
        assertThat(BlobExternalWriter.resolveExtension("audio/x-wav")).isEqualTo("wav");
        assertThat(BlobExternalWriter.resolveExtension("audio/mpeg")).isEqualTo("mp3");
        assertThat(BlobExternalWriter.resolveExtension("application/pdf")).isEqualTo("pdf");
        assertThat(BlobExternalWriter.resolveExtension("invalid")).isEqualTo("bin");
        assertThat(BlobExternalWriter.resolveExtension("type/")).isEqualTo("bin");
    }
}
