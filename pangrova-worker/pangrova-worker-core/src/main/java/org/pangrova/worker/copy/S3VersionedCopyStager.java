package org.pangrova.worker.copy;

import org.pangrova.filesystem.api.blob.BlobObjectId;

import org.pangrova.security.api.error.AccessDeniedException;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.blob.ManagedBlobStorage;
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
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Worker-local staging of one exact S3 object version into content-addressed storage.
 * Source version checks remain in this S3 adapter; admitted managed publication and reads
 * use the storage API supplied by process assembly.
 */
final class S3VersionedCopyStager {

    private static final int BUFFER_BYTES = 64 * 1024;
    private static final Duration MAX_STAGE_WALL_TIME = Duration.ofMinutes(10);

    private final S3Client source;
    private final ManagedBlobStorage managedStorage;
    private final Path stagingRoot;
    private final long maxSourceBytes;
    private final long maxStageNanos;

    S3VersionedCopyStager(S3Client source, ManagedBlobStorage managedStorage,
                          Path stagingRoot, long maxSourceBytes) {
        this(source, managedStorage, stagingRoot, maxSourceBytes, MAX_STAGE_WALL_TIME);
    }

    S3VersionedCopyStager(S3Client source, ManagedBlobStorage managedStorage,
                          Path stagingRoot, long maxSourceBytes, Duration maxStageWallTime) {
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.managedStorage = Objects.requireNonNull(managedStorage, "managedStorage must not be null");
        this.stagingRoot = Objects.requireNonNull(stagingRoot, "stagingRoot must not be null")
                .toAbsolutePath().normalize();
        if (maxSourceBytes <= 0) {
            throw new IllegalArgumentException("maxSourceBytes must be positive");
        }
        this.maxSourceBytes = maxSourceBytes;
        if (maxStageWallTime == null || maxStageWallTime.isZero()
                || maxStageWallTime.isNegative()) {
            throw new IllegalArgumentException("COPY stage wall time must be positive");
        }
        this.maxStageNanos = maxStageWallTime.toNanos();
    }

    StagedSource stage(StorageProfile profile, String sourceUrl, String tenantId,
                        CopyPreparation budget) throws IOException {
        long deadlineNanos = System.nanoTime() + maxStageNanos;
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
            budget.checkpoint();
            HeadObjectResponse head = source.headObject(HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            String version = head.versionId();
            if (version == null || version.isBlank() || "null".equals(version)) {
                throw new IOException("S3 COPY source must have a non-null object version");
            }
            Long expectedSize = head.contentLength();
            requireWithinDeadline(deadlineNanos);
            if (expectedSize == null || expectedSize < 0 || expectedSize > maxSourceBytes) {
                throw new IOException("S3 COPY source size is unavailable or exceeds the staging limit");
            }
            budget.requireStageCapacity(expectedSize);
            Files.createDirectories(stagingRoot);
            if (Files.isSymbolicLink(stagingRoot)) {
                throw new AccessDeniedException("S3 COPY staging root must not be a symbolic link");
            }
            Path draft = Files.createTempFile(stagingRoot, "copy-", ".part");
            budget.ownDraft(draft);
            try {
                String checksum = downloadExactVersion(bucket, key, version, expectedSize,
                        draft, deadlineNanos, budget);
                requireWithinDeadline(deadlineNanos);
                budget.checkpoint();
                managedStorage.publish(new BlobObjectId(scope, checksum, expectedSize), draft, budget::reserveWrite);
                requireWithinDeadline(deadlineNanos);
                verifyManagedObject(scope, checksum, expectedSize, deadlineNanos, budget);
                return new StagedSource(profile.name(), canonicalSourceUrl, version,
                        managedStorage.objectUri(new BlobObjectId(scope, checksum, expectedSize)),
                        scope, checksum, expectedSize);
            } finally {
                budget.cleanupDraft();
            }
        } catch (SdkException failure) {
            throw new IOException("Failed to stage exact S3 COPY source version", failure);
        }
    }

    /** A durable proof retries from managed storage without reading the original S3 source. */
    StagedSource resume(StorageProfile profile, String sourceUrl, String tenantId,
                        StagedSource staged, CopyPreparation budget) throws IOException {
        long deadlineNanos = System.nanoTime() + maxStageNanos;
        Objects.requireNonNull(staged, "staged source must not be null");
        requireRetrySource(profile, sourceUrl, tenantId);
        String scope = HexFormat.of().formatHex(
                sha256().digest(("copy\0" + tenantId).getBytes(StandardCharsets.UTF_8)));
        requireRetryProof(profile, sourceUrl, staged, scope);
        verifyManagedObject(scope, staged.contentSha256(), staged.contentSize(), deadlineNanos, budget);
        return staged;
    }

    /** Materializes only verified managed bytes into a task-private local scan file. */
    VerifiedLocalCopySource materialize(StorageProfile profile, String sourceUrl, String tenantId,
                                        StagedSource staged) throws IOException {
        return materialize(profile, sourceUrl, tenantId, staged, CopyMaterialization.standalone());
    }

    VerifiedLocalCopySource materialize(StorageProfile profile, String sourceUrl, String tenantId,
                                        StagedSource staged, CopyMaterialization ownership) throws IOException {
        long deadlineNanos = System.nanoTime() + maxStageNanos;
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
        var proof = new org.pangrova.protocol.rest.copy.CopySourceReadHttp.StagedSource(
                staged.sourceVersionId(), staged.managedObjectUri(), staged.contentSha256(),
                staged.contentSize(), staged.tenantScope());
        var owned = new VerifiedLocalCopySource(local, stagingRoot, proof);
        try {
            ownership.publishOwner().accept(owned);
            MessageDigest digest = sha256();
            try (var input = managedStorage.open(
                    new BlobObjectId(scope, staged.contentSha256(), staged.contentSize()),
                    ownership.budget()::reserve)) {
                try (FileChannel channel = FileChannel.open(local, StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
                        OutputStream output = Channels.newOutputStream(channel)) {
                    long bytes = transfer(input, output, digest, staged.contentSize(),
                            deadlineNanos);
                    if (bytes != staged.contentSize()
                            || !staged.contentSha256().equals(HexFormat.of().formatHex(digest.digest()))) {
                        throw new IOException("S3 COPY managed scan bytes failed SHA-256/size verification");
                    }
                    output.flush();
                    channel.force(true);
                } catch (IOException | RuntimeException | Error failure) {
                    abortOnFailure(input, failure);
                    throw failure;
                }
            }
            owned.verified();
            return owned;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                owned.close();
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
                || !managedStorage.objectUri(new BlobObjectId(scope, staged.contentSha256(), staged.contentSize()))
                        .equals(staged.managedObjectUri())) {
            throw new AccessDeniedException("S3 COPY staged proof does not match the retry scope");
        }
    }

    private String downloadExactVersion(String bucket, String key, String version,
                                        long expectedSize, Path draft, long deadlineNanos,
                                        CopyPreparation budget) throws IOException {
        var requestBuilder = GetObjectRequest.builder().bucket(bucket).key(key).versionId(version)
                .overrideConfiguration(config -> config.addPlugin(client -> client.overrideConfiguration(
                        client.overrideConfiguration().toBuilder()
                                .retryPolicy(software.amazon.awssdk.core.retry.RetryPolicy.none()).build())));
        if (expectedSize > 0) requestBuilder.range("bytes=0-" + (expectedSize - 1));
        GetObjectRequest request = requestBuilder.build();
        budget.reserveRead(expectedSize);
        try (ResponseInputStream<GetObjectResponse> input = source.getObject(request)) {
            try {
                if (!version.equals(input.response().versionId())
                        || input.response().contentLength() == null
                        || input.response().contentLength() != expectedSize
                        || expectedSize > 0 && !("bytes 0-" + (expectedSize - 1) + "/" + expectedSize)
                                .equals(input.response().contentRange())) {
                    throw new IOException("S3 COPY GET response differs from the pinned HEAD version");
                }
                MessageDigest digest = sha256();
                try (FileChannel channel = FileChannel.open(draft,
                        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                        var output = Channels.newOutputStream(channel)) {
                    long size = transfer(input, output, digest, expectedSize, deadlineNanos,
                            budget::checkpoint, budget::reserveDraft);
                    if (size != expectedSize) {
                        throw new IOException("S3 COPY source ended before its declared length");
                    }
                    output.flush();
                    channel.force(true);
                }
                return HexFormat.of().formatHex(digest.digest());
            } catch (IOException | RuntimeException | Error failure) {
                abortOnFailure(input, failure);
                throw failure;
            }
        }
    }

    private void verifyManagedObject(String scope, String checksum, long expectedSize,
                                     long deadlineNanos, CopyPreparation budget) throws IOException {
        budget.checkpoint();
        if (expectedSize == 0) {
            managedStorage.verifyEmptyPublication(new BlobObjectId(scope, checksum, 0));
            budget.checkpoint();
            return;
        }
        MessageDigest digest = sha256();
        try (var input = managedStorage.open(new BlobObjectId(scope, checksum, expectedSize), budget::reserveRead)) {
            try {
                long size = transfer(input, OutputStream.nullOutputStream(), digest,
                        expectedSize, deadlineNanos, budget::checkpoint, ignored -> {});
                if (size != expectedSize
                        || !checksum.equals(HexFormat.of().formatHex(digest.digest()))) {
                    throw new IOException("S3 COPY managed staging object failed byte integrity verification");
                }
            } catch (IOException | RuntimeException | Error failure) {
                abortOnFailure(input, failure);
                throw failure;
            }
        }
    }

    private static void abortOnFailure(ResponseInputStream<?> input, Throwable failure) {
        try {
            input.abort();
        } catch (RuntimeException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    private static void abortOnFailure(org.pangrova.filesystem.api.blob.BlobInputStream input, Throwable failure) {
        try {
            input.abort();
        } catch (RuntimeException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    private static long transfer(InputStream input, OutputStream output,
                                 MessageDigest digest, long limit, long deadlineNanos)
            throws IOException {
        return transfer(input, output, digest, limit, deadlineNanos, () -> {}, ignored -> {});
    }

    private static long transfer(InputStream input, OutputStream output, MessageDigest digest,
                                 long limit, long deadlineNanos, Runnable checkpoint,
                                 java.util.function.LongConsumer draftAdmission) throws IOException {
        byte[] buffer = new byte[BUFFER_BYTES];
        long size = 0;
        int read;
        while (true) {
            requireWithinDeadline(deadlineNanos);
            checkpoint.run();
            read = input.read(buffer);
            checkpoint.run();
            requireWithinDeadline(deadlineNanos);
            if (read == -1) break;
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
            draftAdmission.accept(read);
            output.write(buffer, 0, read);
        }
        return size;
    }

    private static void requireWithinDeadline(long deadlineNanos) throws IOException {
        if (System.nanoTime() - deadlineNanos >= 0) {
            throw new IOException("S3 COPY staging wall time exceeded");
        }
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
