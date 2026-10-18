package org.pangrova.worker.copy;

import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.worker.ai.blob.S3BlobStorage;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Worker-local staging of one exact S3 object version into content-addressed storage.
 * This component is intentionally not connected to SQL until durable profile resolution,
 * file-read authorization, and the COPY commit claim are available.
 */
final class S3VersionedCopyStager {

    private static final int BUFFER_BYTES = 64 * 1024;

    private final S3Client source;
    private final S3BlobStorage managedStorage;
    private final Path stagingRoot;
    private final long maxSourceBytes;

    S3VersionedCopyStager(S3Client source, S3BlobStorage managedStorage,
                          Path stagingRoot, long maxSourceBytes) {
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.managedStorage = Objects.requireNonNull(managedStorage, "managedStorage must not be null");
        this.stagingRoot = Objects.requireNonNull(stagingRoot, "stagingRoot must not be null")
                .toAbsolutePath().normalize();
        if (maxSourceBytes <= 0) {
            throw new IllegalArgumentException("maxSourceBytes must be positive");
        }
        this.maxSourceBytes = maxSourceBytes;
    }

    StagedSource stage(StorageProfile profile, String sourceUrl, String tenantId) throws IOException {
        Objects.requireNonNull(profile, "profile must not be null");
        if (profile.type() != StorageType.S3 || !profile.matchesUrl(sourceUrl)) {
            throw new AccessDeniedException("S3 COPY source is outside the bound storage profile");
        }
        if (tenantId == null || tenantId.isBlank()) {
            throw new AccessDeniedException("S3 COPY staging requires an authenticated tenant");
        }
        URI uri = URI.create(sourceUrl);
        String bucket = uri.getRawAuthority();
        String key = uri.getRawPath().substring(1);
        String scope = HexFormat.of().formatHex(
                sha256().digest(("copy\0" + tenantId).getBytes(StandardCharsets.UTF_8)));
        String canonicalSourceUrl = "s3://" + bucket + "/" + key;
        try {
            HeadObjectResponse head = source.headObject(HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            String version = head.versionId();
            if (version == null || version.isBlank() || "null".equals(version)) {
                throw new IOException("S3 COPY source must have a non-null object version");
            }
            Long expectedSize = head.contentLength();
            if (expectedSize == null || expectedSize < 0 || expectedSize > maxSourceBytes) {
                throw new IOException("S3 COPY source size is unavailable or exceeds the staging limit");
            }
            Files.createDirectories(stagingRoot);
            Path draft = Files.createTempFile(stagingRoot, "copy-", ".part");
            try {
                String checksum = downloadExactVersion(bucket, key, version, expectedSize, draft);
                managedStorage.publish(scope, checksum, expectedSize, draft);
                verifyManagedObject(scope, checksum, expectedSize);
                return new StagedSource(profile.name(), canonicalSourceUrl, version,
                        managedStorage.objectUri(scope, checksum), scope, checksum, expectedSize);
            } finally {
                Files.deleteIfExists(draft);
            }
        } catch (SdkException failure) {
            throw new IOException("Failed to stage exact S3 COPY source version", failure);
        }
    }

    /** A durable proof retries from managed storage without reading the original S3 source. */
    StagedSource resume(StorageProfile profile, String sourceUrl, String tenantId,
                        StagedSource staged) throws IOException {
        Objects.requireNonNull(staged, "staged source must not be null");
        requireRetrySource(profile, sourceUrl, tenantId);
        String scope = HexFormat.of().formatHex(
                sha256().digest(("copy\0" + tenantId).getBytes(StandardCharsets.UTF_8)));
        requireRetryProof(profile, sourceUrl, staged, scope);
        verifyManagedObject(scope, staged.contentSha256(), staged.contentSize());
        return staged;
    }

    /** Materializes only verified managed bytes into a task-private local scan file. */
    VerifiedLocalCopySource materialize(StorageProfile profile, String sourceUrl, String tenantId,
                                        StagedSource staged) throws IOException {
        requireRetrySource(profile, sourceUrl, tenantId);
        String scope = HexFormat.of().formatHex(
                sha256().digest(("copy\0" + tenantId).getBytes(StandardCharsets.UTF_8)));
        requireRetryProof(profile, sourceUrl, staged, scope);
        Files.createDirectories(stagingRoot);
        if (Files.isSymbolicLink(stagingRoot)) {
            throw new AccessDeniedException("S3 COPY staging root must not be a symbolic link");
        }
        Path privateDirectory = Files.createTempDirectory(stagingRoot, "copy-scan-",
                PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rwx------")));
        Path local = privateDirectory.resolve("source.parquet");
        try {
            MessageDigest digest = sha256();
            try (InputStream input = managedStorage.open(scope, staged.contentSha256(),
                    staged.contentSize());
                    FileChannel channel = FileChannel.open(local, StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE);
                    OutputStream output = Channels.newOutputStream(channel)) {
                long bytes = transfer(input, output, digest, staged.contentSize());
                if (bytes != staged.contentSize()
                        || !staged.contentSha256().equals(HexFormat.of().formatHex(digest.digest()))) {
                    throw new IOException("S3 COPY managed scan bytes failed SHA-256/size verification");
                }
                output.flush();
                channel.force(true);
            }
            return new VerifiedLocalCopySource(local, stagingRoot);
        } catch (IOException | RuntimeException failure) {
            try {
                Files.deleteIfExists(local);
                Files.deleteIfExists(privateDirectory);
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static void requireRetrySource(StorageProfile profile, String sourceUrl, String tenantId) {
        if (profile == null || profile.type() != StorageType.S3 || !profile.matchesUrl(sourceUrl)
                || tenantId == null || tenantId.isBlank()) {
            throw new AccessDeniedException("S3 COPY retry is outside the bound storage profile");
        }
    }

    private void requireRetryProof(StorageProfile profile, String sourceUrl,
                                   StagedSource staged, String scope) {
        if (!profile.name().equals(staged.profileName()) || !sourceUrl.equals(staged.sourceUrl())
                || !scope.equals(staged.tenantScope()) || staged.sourceVersionId() == null
                || staged.sourceVersionId().isBlank() || "null".equals(staged.sourceVersionId())
                || staged.contentSha256() == null || !staged.contentSha256().matches("[0-9a-f]{64}")
                || staged.contentSize() < 0 || staged.contentSize() > maxSourceBytes
                || !managedStorage.objectUri(scope, staged.contentSha256())
                        .equals(staged.managedObjectUri())) {
            throw new AccessDeniedException("S3 COPY staged proof does not match the retry scope");
        }
    }

    private String downloadExactVersion(String bucket, String key, String version,
                                        long expectedSize, Path draft) throws IOException {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket).key(key).versionId(version).build();
        try (ResponseInputStream<GetObjectResponse> input = source.getObject(request)) {
            if (!version.equals(input.response().versionId())
                    || input.response().contentLength() == null
                    || input.response().contentLength() != expectedSize) {
                throw new IOException("S3 COPY GET response differs from the pinned HEAD version");
            }
            MessageDigest digest = sha256();
            try (FileChannel channel = FileChannel.open(draft,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    var output = Channels.newOutputStream(channel)) {
                long size = transfer(input, output, digest, expectedSize);
                if (size != expectedSize) {
                    throw new IOException("S3 COPY source ended before its declared length");
                }
                output.flush();
                channel.force(true);
            }
            return HexFormat.of().formatHex(digest.digest());
        }
    }

    private void verifyManagedObject(String scope, String checksum, long expectedSize)
            throws IOException {
        MessageDigest digest = sha256();
        try (InputStream input = managedStorage.open(scope, checksum, expectedSize)) {
            long size = transfer(input, OutputStream.nullOutputStream(), digest, expectedSize);
            if (size != expectedSize
                    || !checksum.equals(HexFormat.of().formatHex(digest.digest()))) {
                throw new IOException("S3 COPY managed staging object failed byte integrity verification");
            }
        }
    }

    private static long transfer(InputStream input, OutputStream output,
                                 MessageDigest digest, long limit) throws IOException {
        byte[] buffer = new byte[BUFFER_BYTES];
        long size = 0;
        int read;
        while ((read = input.read(buffer)) != -1) {
            if (read == 0) {
                continue;
            }
            try {
                size = Math.addExact(size, read);
            } catch (ArithmeticException overflow) {
                throw new IOException("S3 COPY content size overflow", overflow);
            }
            if (size > limit) {
                throw new IOException("S3 COPY content exceeds the pinned object size");
            }
            digest.update(buffer, 0, read);
            output.write(buffer, 0, read);
        }
        return size;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    /** Exact source and verified managed-object identity; contains no credentials or file bytes. */
    record StagedSource(String profileName, String sourceUrl, String sourceVersionId,
                        String managedObjectUri, String tenantScope,
                        String contentSha256, long contentSize) {}
}
