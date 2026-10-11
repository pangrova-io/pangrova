package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.worker.ai.context.AiExecutionContext;
import org.pangrova.worker.ai.invocation.AiInvocationService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Immutable, tenant-scoped content-addressed FILE store.
 *
 * <p>Physical paths never leave the Worker. Published references use the governed
 * {@code pangrova-blob} URI scheme and carry an unguessable tenant capability.
 * SQL/table authorization remains responsible for deciding which principals may
 * see a reference; the store prevents cross-tenant use and forged references.
 * Writes are staged locally, forced to stable storage, and published through one
 * durable storage contract; a failed write never publishes a reference.</p>
 */
public final class GovernedBlobStore implements AutoCloseable {

    private static final int BUFFER_BYTES = 64 * 1024;
    private static final int MAX_ARRAY_BYTES = Integer.MAX_VALUE - 8;
    private static final Duration STAGING_CLEANUP_AGE = Duration.ofHours(24);
    private static final int STAGING_CLEANUP_LIMIT = 10_000;
    private static final Pattern MIME = Pattern.compile(
            "[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*");

    private final BlobStorage storage;
    private final Path stagingRoot;
    private final long maxBlobBytes;
    private final Set<String> allowedMimeTypes;
    private final Clock clock;
    private final TenantCapabilitySigner capabilitySigner;

    public GovernedBlobStore(
            BlobStorage storage,
            Path stagingRoot,
            byte[] capabilityKey,
            long maxBlobBytes,
            Set<String> allowedMimeTypes) {
        this(storage, stagingRoot, capabilityKey, maxBlobBytes,
                allowedMimeTypes, Clock.systemUTC());
    }

    GovernedBlobStore(
            BlobStorage storage,
            Path stagingRoot,
            byte[] capabilityKey,
            long maxBlobBytes,
            Set<String> allowedMimeTypes,
            Clock clock) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        this.stagingRoot = requireRoot(stagingRoot);
        if (maxBlobBytes <= 0) {
            throw new IllegalArgumentException("maxBlobBytes must be positive");
        }
        if (allowedMimeTypes == null || allowedMimeTypes.isEmpty()) {
            throw new IllegalArgumentException("allowedMimeTypes must not be empty");
        }
        this.maxBlobBytes = maxBlobBytes;
        this.allowedMimeTypes = allowedMimeTypes.stream()
                .map(GovernedBlobStore::normalizeMimeType)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.capabilitySigner = new TenantCapabilitySigner(capabilityKey);
        createStagingDirectory();
    }

    /** Persists one bounded byte value using the current governed task identity. */
    public BlobReference persist(byte[] content, String mimeType) {
        Objects.requireNonNull(content, "content must not be null");
        return persist(new ByteArrayInputStream(content), mimeType, content.length);
    }

    /** Persists one bounded stream and publishes its immutable content identity. */
    public BlobReference persist(
            InputStream content, String mimeType, long declaredMaximumBytes) {
        Objects.requireNonNull(content, "content must not be null");
        String normalizedMime = requireAllowedMimeType(mimeType);
        AccessScope scope = currentScope();
        long effectiveLimit = effectiveLimit(scope.context(), declaredMaximumBytes);
        Path staging = stagingPath(scope.token());
        try {
            Draft draft = writeDraft(content, staging, effectiveLimit,
                    scope.context().deadlineEpochMs());
            AiInvocationService.chargeGovernedFileBytes(draft.size());
            storage.publish(scope.token(), draft.checksum(), draft.size(), staging);
            String capability = capabilitySigner.sign(
                    scope.token(), normalizedMime, draft.size(), draft.checksum());
            return new BlobReference(
                    governedUri(scope.token(), capability, draft.checksum()),
                    normalizedMime, draft.size(), draft.checksum());
        } catch (IOException failure) {
            throw storageFailure("Failed to publish governed FILE content", failure);
        } finally {
            deleteQuietly(staging);
        }
    }

    /** Opens and verifies one governed object as it is streamed. */
    public InputStream open(BlobReference reference) {
        Objects.requireNonNull(reference, "reference must not be null");
        AccessScope scope = currentScope();
        requireScope(reference, scope);
        long effectiveLimit = effectiveLimit(scope.context(), reference.size());
        try {
            return new VerifyingInputStream(
                    storage.open(scope.token(), reference.checksum(), reference.size()),
                    reference, effectiveLimit);
        } catch (IOException failure) {
            throw readFailure(reference, failure);
        }
    }

    /** Reads a governed object into a bounded byte array for providers requiring inline media. */
    public byte[] read(BlobReference reference) {
        if (reference.size() > MAX_ARRAY_BYTES) {
            throw sizeFailure(reference.size(), MAX_ARRAY_BYTES);
        }
        try (InputStream input = open(reference);
                ByteArrayOutputStream output = new ByteArrayOutputStream(
                        Math.toIntExact(reference.size()))) {
            transfer(input, output, reference.size());
            return output.toByteArray();
        } catch (IOException failure) {
            throw readFailure(reference, failure);
        }
    }

    /** Deletes a bounded number of abandoned staging files older than the grace period. */
    public int cleanupStaging(Duration gracePeriod, int maxEntries) {
        if (gracePeriod == null || gracePeriod.isNegative() || maxEntries <= 0) {
            throw new IllegalArgumentException("staging cleanup bounds are invalid");
        }
        Instant cutoff = clock.instant().minus(gracePeriod);
        int deleted = 0;
        try (Stream<Path> files = Files.walk(stagingRoot)) {
            var iterator = files.filter(path -> Files.isRegularFile(
                    path, LinkOption.NOFOLLOW_LINKS)).iterator();
            while (iterator.hasNext() && deleted < maxEntries) {
                Path candidate = iterator.next();
                if (Files.getLastModifiedTime(
                        candidate, LinkOption.NOFOLLOW_LINKS).toInstant().isBefore(cutoff)) {
                    Files.deleteIfExists(candidate);
                    deleted++;
                }
            }
            return deleted;
        } catch (IOException failure) {
            throw storageFailure("Failed to clean governed FILE staging", failure);
        }
    }

    private Draft writeDraft(InputStream input, Path staging, long limit,
                             long deadlineEpochMs) {
        MessageDigest digest = sha256();
        long size = 0;
        try {
            Path parent = staging.getParent();
            if (parent == null) {
                throw new IOException("FILE staging path requires a parent directory");
            }
            Files.createDirectories(parent);
            try (FileChannel channel = FileChannel.open(
                    staging, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    var output = Channels.newOutputStream(channel)) {
                byte[] buffer = new byte[BUFFER_BYTES];
                int count;
                while ((count = input.read(buffer)) >= 0) {
                    if (clock.millis() >= deadlineEpochMs) {
                        throw new IllegalStateException(
                                "Governed FILE staging deadline has expired");
                    }
                    if (count == 0) {
                        continue;
                    }
                    size = Math.addExact(size, count);
                    if (size > limit) {
                        throw sizeFailure(size, limit);
                    }
                    digest.update(buffer, 0, count);
                    output.write(buffer, 0, count);
                }
                output.flush();
                channel.force(true);
            }
            return new Draft(size, HexFormat.of().formatHex(digest.digest()));
        } catch (IOException failure) {
            throw storageFailure("Failed to stage governed FILE content", failure);
        } catch (ArithmeticException failure) {
            throw sizeFailure(Long.MAX_VALUE, limit);
        }
    }

    private AccessScope currentScope() {
        AiExecutionContext context = AiExecutionContext.current().orElseThrow(() ->
                new IllegalStateException("Governed FILE access requires task identity"));
        context.requireGovernedIdentity();
        context.deadline().ifPresent(deadline -> {
            if (!clock.instant().isBefore(deadline)) {
                throw new IllegalStateException("Governed FILE access deadline has expired");
            }
        });
        String token = scopeToken(context);
        return new AccessScope(context, token);
    }

    private long effectiveLimit(AiExecutionContext context, long requestedLimit) {
        if (requestedLimit < 0 || context.maxMediaBytes() <= 0) {
            throw new IllegalStateException("Governed FILE media-byte grant is unavailable");
        }
        return Math.min(maxBlobBytes, Math.min(requestedLimit, context.maxMediaBytes()));
    }

    private String requireAllowedMimeType(String value) {
        String normalized = normalizeMimeType(value);
        if (!allowedMimeTypes.contains(normalized)) {
            throw new IllegalArgumentException(
                    "MIME type is not allowed by the Worker FILE policy: " + normalized);
        }
        return normalized;
    }

    private static String normalizeMimeType(String value) {
        if (value == null) {
            throw new IllegalArgumentException("mimeType must not be null");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (!MIME.matcher(normalized).matches()) {
            throw new IllegalArgumentException("mimeType must be a canonical media type");
        }
        return normalized;
    }

    private Path stagingPath(String scope) {
        return stagingRoot.resolve(scope)
                .resolve(UUID.randomUUID() + ".part");
    }

    private static String governedUri(
            String scope, String capability, String checksum) {
        return "pangrova-blob://v2/" + scope + "/" + capability + "/" + checksum;
    }

    private void requireScope(BlobReference reference, AccessScope expected) {
        String[] segments = java.net.URI.create(reference.uri())
                .getPath().split("/", -1);
        String actualScope = segments[1];
        if (!MessageDigest.isEqual(
                actualScope.getBytes(StandardCharsets.US_ASCII),
                expected.token().getBytes(StandardCharsets.US_ASCII))) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED,
                    "Governed FILE reference does not belong to the task tenant");
        }
        String expectedCapability = capabilitySigner.sign(
                expected.token(), reference.mimeType(),
                reference.size(), reference.checksum());
        if (!MessageDigest.isEqual(
                segments[2].getBytes(StandardCharsets.US_ASCII),
                expectedCapability.getBytes(StandardCharsets.US_ASCII))) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED,
                    "Governed FILE reference capability is invalid");
        }
    }

    private static String scopeToken(AiExecutionContext context) {
        MessageDigest digest = sha256();
        digest.update(context.tenantId().getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void transfer(
            InputStream input, ByteArrayOutputStream output, long expected) throws IOException {
        byte[] buffer = new byte[BUFFER_BYTES];
        long size = 0;
        int count;
        while ((count = input.read(buffer)) >= 0) {
            if (count > 0) {
                size += count;
                if (size > expected) {
                    throw new IOException("Governed FILE content exceeded declared size");
                }
                output.write(buffer, 0, count);
            }
        }
    }

    private void createStagingDirectory() {
        try {
            Files.createDirectories(stagingRoot);
        } catch (IOException failure) {
            throw storageFailure("Failed to initialize governed FILE staging", failure);
        }
    }

    private static Path requireRoot(Path value) {
        if (value == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        return value.toAbsolutePath().normalize();
    }

    private static PangrovaException sizeFailure(long actual, long maximum) {
        return new PangrovaException(
                PangrovaErrorCode.AI_BLOB_SIZE_EXCEEDED,
                "Governed FILE content size " + actual + " exceeds " + maximum + " bytes");
    }

    private static PangrovaException storageFailure(String message, Exception failure) {
        return new PangrovaException(
                PangrovaErrorCode.CONNECTOR_WRITE_ERROR, message, failure);
    }

    private static PangrovaException readFailure(
            BlobReference reference, Exception failure) {
        return new PangrovaException(
                PangrovaErrorCode.CONNECTOR_READ_ERROR,
                "Failed to read governed FILE content: " + reference.uri(), failure);
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // The bounded staging scavenger removes abandoned files after its grace period.
        }
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            cleanupStaging(STAGING_CLEANUP_AGE, STAGING_CLEANUP_LIMIT);
        } catch (RuntimeException cleanupFailure) {
            failure = cleanupFailure;
        }
        try {
            storage.close();
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = storageFailure("Failed to close governed FILE storage", closeFailure);
            } else {
                failure.addSuppressed(closeFailure);
            }
        } finally {
            capabilitySigner.destroy();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private record AccessScope(AiExecutionContext context, String token) {}

    private record Draft(long size, String checksum) {}

    private static final class VerifyingInputStream extends FilterInputStream {
        private final BlobReference reference;
        private final long limit;
        private final MessageDigest digest = sha256();
        private long count;
        private boolean verified;

        private VerifyingInputStream(
                InputStream input, BlobReference reference, long limit) {
            super(input);
            this.reference = reference;
            this.limit = limit;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value < 0) {
                verifyEnd();
            } else {
                digest.update((byte) value);
                add(1);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int value = super.read(buffer, offset, length);
            if (value < 0) {
                verifyEnd();
            } else if (value > 0) {
                digest.update(buffer, offset, value);
                add(value);
            }
            return value;
        }

        @Override
        public void close() throws IOException {
            try {
                byte[] buffer = new byte[BUFFER_BYTES];
                while (!verified && read(buffer) >= 0) {
                    // Drain within the configured limit so integrity is never optional.
                }
            } finally {
                super.close();
            }
        }

        private void add(int amount) {
            count = Math.addExact(count, amount);
            if (count > limit) {
                throw sizeFailure(count, limit);
            }
        }

        private void verifyEnd() throws IOException {
            if (verified) {
                return;
            }
            verified = true;
            String actual = HexFormat.of().formatHex(digest.digest());
            if (count != reference.size() || !actual.equals(reference.checksum())) {
                throw new IOException("Governed FILE size or checksum validation failed");
            }
        }
    }
}
