package org.pangrova.storage.oss.blob;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;

import org.pangrova.storage.api.blob.BlobObjectId;

/** Private upload snapshot; SHA-256 binds identity, MD5 is only the OSS Content-MD5 transport checksum. */
record OssPublicationDraft(Path path, String contentMd5) implements AutoCloseable {
    private static final int BUFFER_SIZE = 8192;

    static OssPublicationDraft copy(BlobObjectId object, Path source) throws IOException {
        Path draft = Files.createTempFile("pangrova-oss-publish-", ".draft");
        try {
            MessageDigest sha256 = digest("SHA-256");
            MessageDigest md5 = digest("MD5");
            long count = 0;
            try (var input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS);
                 var output = Files.newOutputStream(draft, LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                for (int read; (read = input.read(buffer)) != -1;) {
                    if (read > object.size() - count) throw new IOException("OSS staging exceeds object size");
                    output.write(buffer, 0, read);
                    sha256.update(buffer, 0, read);
                    md5.update(buffer, 0, read);
                    count += read;
                }
            }
            if (count != object.size() || !HexFormat.of().formatHex(sha256.digest()).equals(object.checksum())) {
                throw new IOException("OSS staging does not match object size/SHA-256");
            }
            return new OssPublicationDraft(draft, Base64.getEncoder().encodeToString(md5.digest()));
        } catch (IOException | RuntimeException | Error failure) {
            try {
                Files.delete(draft);
            } catch (IOException | RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    static MessageDigest digest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("Required JDK digest is unavailable", failure);
        }
    }

    @Override
    public void close() throws IOException {
        Files.delete(path);
    }
}
