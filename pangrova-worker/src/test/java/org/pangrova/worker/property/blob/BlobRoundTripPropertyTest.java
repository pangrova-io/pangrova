package org.pangrova.worker.property.blob;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.worker.ai.blob.BlobContentReader;
import org.pangrova.worker.ai.blob.BlobExternalWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for BLOB round-trip integrity (P10).
 *
 * <p>Validates that writing a blob via {@link BlobExternalWriter} and reading it back
 * via {@link BlobContentReader} returns identical bytes, verified by SHA-256 checksum.</p>
 *
 * <p><b>Validates: Requirements P10</b></p>
 */
class BlobRoundTripPropertyTest {

    private static final String TABLE_LOCATION = "s3://bucket/warehouse/db/table1";
    private static final String COLUMN = "image";

    @Provide
    Arbitrary<byte[]> blobContent() {
        return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(4096);
    }

    @Provide
    Arbitrary<String> mimeTypes() {
        return Arbitraries.of(
                "image/jpeg", "image/png", "application/pdf",
                "text/plain", "audio/mpeg", "application/octet-stream");
    }

    /**
     * P10: For any non-empty byte array written as a blob, reading it back via
     * the BlobReference returns identical content (byte-for-byte equality).
     */
    @Property(tries = 100)
    void writeAndReadReturnsIdenticalContent(
            @ForAll("blobContent") byte[] content,
            @ForAll("mimeTypes") String mimeType) {

        InMemoryStorage storage = new InMemoryStorage();
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        BlobContentReader reader = new BlobContentReader(storage);

        BlobReference ref = writer.write(TABLE_LOCATION, COLUMN, content, mimeType);
        byte[] readBack = reader.read(ref);

        assertThat(readBack).isEqualTo(content);
    }

    /**
     * P10: The SHA-256 checksum stored in the BlobReference matches the
     * independently computed checksum of the original content.
     */
    @Property(tries = 100)
    void checksumMatchesContentHash(
            @ForAll("blobContent") byte[] content,
            @ForAll("mimeTypes") String mimeType) {

        InMemoryStorage storage = new InMemoryStorage();
        BlobExternalWriter writer = new BlobExternalWriter(storage);

        BlobReference ref = writer.write(TABLE_LOCATION, COLUMN, content, mimeType);

        String expectedChecksum = computeSha256(content);
        assertThat(ref.checksum()).isEqualTo(expectedChecksum);
    }

    /**
     * P10: The size recorded in the BlobReference matches the actual content length.
     */
    @Property(tries = 100)
    void referenceSizeMatchesContentLength(
            @ForAll("blobContent") byte[] content,
            @ForAll("mimeTypes") String mimeType) {

        InMemoryStorage storage = new InMemoryStorage();
        BlobExternalWriter writer = new BlobExternalWriter(storage);

        BlobReference ref = writer.write(TABLE_LOCATION, COLUMN, content, mimeType);

        assertThat(ref.size()).isEqualTo(content.length);
    }

    /**
     * P10: Multiple distinct blobs written to the same table/column produce
     * distinct URIs and each round-trips independently.
     */
    @Property(tries = 50)
    void multipleBlobsRoundTripIndependently(
            @ForAll("blobContent") byte[] contentA,
            @ForAll("blobContent") byte[] contentB,
            @ForAll("mimeTypes") String mimeType) {

        InMemoryStorage storage = new InMemoryStorage();
        BlobExternalWriter writer = new BlobExternalWriter(storage);
        BlobContentReader reader = new BlobContentReader(storage);

        BlobReference refA = writer.write(TABLE_LOCATION, COLUMN, contentA, mimeType);
        BlobReference refB = writer.write(TABLE_LOCATION, COLUMN, contentB, mimeType);

        // Distinct URIs (UUID-based)
        assertThat(refA.uri()).isNotEqualTo(refB.uri());

        // Each reads back its own content
        assertThat(reader.read(refA)).isEqualTo(contentA);
        assertThat(reader.read(refB)).isEqualTo(contentB);
    }

    // ── Helpers ──

    private static String computeSha256(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * In-memory storage for property tests. Thread-safe, no I/O failures.
     */
    private static final class InMemoryStorage implements FileSystemAccessor {
        private final ConcurrentMap<String, byte[]> files = new ConcurrentHashMap<>();

        @Override
        public InputStream openInputStream(String path) throws IOException {
            byte[] data = files.get(path);
            if (data == null) {
                throw new IOException("File not found: " + path);
            }
            return new ByteArrayInputStream(data);
        }

        @Override
        public OutputStream openOutputStream(String path) {
            return new CapturingOutputStream(path);
        }

        @Override
        public void deleteFile(String path) {
            files.remove(path);
        }

        @Override
        public boolean exists(String path) {
            return files.containsKey(path);
        }

        @Override
        public long fileSize(String path) throws IOException {
            byte[] data = files.get(path);
            if (data == null) {
                throw new IOException("File not found: " + path);
            }
            return data.length;
        }

        @Override
        public List<String> listFiles(String directoryPath) {
            return files.keySet().stream()
                    .filter(p -> p.startsWith(directoryPath))
                    .toList();
        }

        @Override
        public void close() {
            files.clear();
        }

        private final class CapturingOutputStream extends ByteArrayOutputStream {
            private final String path;

            CapturingOutputStream(String path) {
                this.path = path;
            }

            @Override
            public void close() throws IOException {
                super.close();
                files.put(path, toByteArray());
            }
        }
    }
}
