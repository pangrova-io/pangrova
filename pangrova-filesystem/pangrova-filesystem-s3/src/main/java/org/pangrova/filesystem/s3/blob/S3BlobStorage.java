package org.pangrova.filesystem.s3.blob;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.filesystem.api.blob.BlobObjectId;

import org.pangrova.filesystem.api.blob.BlobInputStream;
import org.pangrova.filesystem.api.blob.ManagedBlobStorage;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** S3-compatible durable backend for governed FILE objects. */
public final class S3BlobStorage implements ManagedBlobStorage {

    private static final Pattern BUCKET = Pattern.compile(
            "(?=.{3,63}$)[a-z0-9][a-z0-9.-]*[a-z0-9]");
    private static final Pattern REGION = Pattern.compile("[A-Za-z0-9][A-Za-z0-9-]{0,62}");
    private static final Pattern PREFIX = Pattern.compile("[A-Za-z0-9!_.*'()/=-]*");
    private static final int PRECONDITION_FAILED = 412;
    private static final int HTTPS_PORT = 443;
    private static final int HTTP_PORT = 80;

    private final S3Client client;
    private final Settings settings;
    private final boolean ownsClient;

    /** Creates a production backend using the AWS SDK default credential chain. */
    public S3BlobStorage(Settings settings) {
        this(buildClient(settings), settings, true);
    }

    /** Dedicated managed backend for COPY staging with bounded S3 network I/O. */
    public static S3BlobStorage forCopyStaging(Settings settings) {
        return new S3BlobStorage(buildCopyClient(settings), settings, true);
    }

    /** Uses a caller-configured S3 client while retaining all publication and read checks. */
    public S3BlobStorage(S3Client client, Settings settings) {
        this(client, settings, false);
    }

    private S3BlobStorage(S3Client client, Settings settings, boolean ownsClient) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.ownsClient = ownsClient;
    }

    @Override
    public void publish(
            BlobObjectId object, Path stagedContent)
            throws IOException {
        publishInternal(object, stagedContent, null);
    }

    /** One admitted publication; internal SDK retries cannot reuse its byte allowance. */
    public void publish(BlobObjectId object, Path stagedContent,
                        java.util.function.LongConsumer writeAdmission) throws IOException {
        publishInternal(object, stagedContent,
                Objects.requireNonNull(writeAdmission, "writeAdmission"));
    }

    private void publishInternal(BlobObjectId object, Path stagedContent,
                                 java.util.function.LongConsumer writeAdmission) throws IOException {
        Objects.requireNonNull(object, "object");
        String checksum = object.checksum();
        long size = object.size();
        String key = objectKey(object.tenantScope(), checksum);
        PutObjectRequest request = putRequest(key, checksum, size);
        if (writeAdmission != null) {
            request = request.toBuilder().overrideConfiguration(request.overrideConfiguration().orElseThrow()
                    .toBuilder().addPlugin(S3BlobStorage::withoutHiddenPayloadRetries).build()).build();
            writeAdmission.accept(size);
        }
        try {
            client.putObject(request, RequestBody.fromFile(stagedContent));
        } catch (S3Exception failure) {
            if (failure.statusCode() != PRECONDITION_FAILED) {
                throw ioFailure("Failed to publish governed FILE object", failure);
            }
            // Another Worker atomically published the same content identity.
        } catch (SdkException failure) {
            throw ioFailure("Failed to publish governed FILE object", failure);
        }
        verifyHead(key, checksum, size);
    }

    @Override
    public InputStream open(BlobObjectId object) throws IOException {
        return openInternal(object, null);
    }

    /** Exact admitted payload range for a task-owned managed-object download. */
    public BlobInputStream open(BlobObjectId object,
                            java.util.function.LongConsumer readAdmission) throws IOException {
        var stream = openInternal(object, Objects.requireNonNull(readAdmission, "readAdmission"));
        return new BlobInputStream(stream, stream::abort);
    }

    private ResponseInputStream<GetObjectResponse> openInternal(BlobObjectId object,
                                    java.util.function.LongConsumer readAdmission) throws IOException {
        Objects.requireNonNull(object, "object");
        String checksum = object.checksum();
        long size = object.size();
        String key = objectKey(object.tenantScope(), checksum);
        try {
            var request = getRequest(key);
            if (readAdmission != null) {
                if (size <= 0) {
                    throw new IllegalArgumentException("Admitted object read requires a positive byte range");
                }
                request = request.toBuilder().range("bytes=0-" + (size - 1))
                        .overrideConfiguration(config -> config.addPlugin(
                                S3BlobStorage::withoutHiddenPayloadRetries)).build();
                readAdmission.accept(size);
            }
            ResponseInputStream<GetObjectResponse> stream = client.getObject(
                    request);
            try {
                if (readAdmission != null
                        && !("bytes 0-" + (size - 1) + "/" + size).equals(stream.response().contentRange())) {
                    throw new IOException("Managed object does not match its admitted complete byte range");
                }
                verifyMetadata(
                        stream.response().contentLength(), stream.response().metadata(),
                        checksum, size);
                verifyEncryption(
                        stream.response().serverSideEncryption(),
                        stream.response().ssekmsKeyId());
                return stream;
            } catch (IOException failure) {
                // Apache close drains unread bodies; a rejected object must not
                // hold the staging thread while the remote endpoint trickles data.
                try {
                    stream.abort();
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                throw failure;
            }
        } catch (SdkException failure) {
            throw ioFailure("Failed to open governed FILE object", failure);
        }
    }

    // A task retry gets a new admission; an SDK retry must not reuse one byte reservation.
    private static void withoutHiddenPayloadRetries(
            software.amazon.awssdk.core.SdkServiceClientConfiguration.Builder config) {
        config.overrideConfiguration(config.overrideConfiguration().toBuilder()
                .retryPolicy(software.amazon.awssdk.core.retry.RetryPolicy.none()).build());
    }

    private PutObjectRequest putRequest(String key, String checksum, long size) {
        PutObjectRequest.Builder request = PutObjectRequest.builder()
                .bucket(settings.bucket())
                .key(key)
                .contentLength(size)
                .contentType("application/octet-stream")
                .checksumSHA256(Base64.getEncoder().encodeToString(
                        HexFormat.of().parseHex(checksum)))
                .metadata(Map.of("sha256", checksum, "content-length", Long.toString(size)))
                .serverSideEncryption(settings.encryption().mode())
                .overrideConfiguration(override -> override.putHeader("If-None-Match", "*"));
        applyOwner(request);
        if (settings.encryption().mode() == ServerSideEncryption.AWS_KMS) {
            request.ssekmsKeyId(settings.encryption().kmsKeyId());
        }
        return request.build();
    }

    private HeadObjectRequest headRequest(String key) {
        HeadObjectRequest.Builder request = HeadObjectRequest.builder()
                .bucket(settings.bucket()).key(key).checksumMode("ENABLED");
        if (!settings.expectedBucketOwner().isEmpty()) {
            request.expectedBucketOwner(settings.expectedBucketOwner());
        }
        return request.build();
    }

    private GetObjectRequest getRequest(String key) {
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(settings.bucket()).key(key).checksumMode("ENABLED");
        if (!settings.expectedBucketOwner().isEmpty()) {
            request.expectedBucketOwner(settings.expectedBucketOwner());
        }
        return request.build();
    }

    private void applyOwner(PutObjectRequest.Builder request) {
        if (!settings.expectedBucketOwner().isEmpty()) {
            request.expectedBucketOwner(settings.expectedBucketOwner());
        }
    }

    /** Empty prepared sources have no payload range; verify their exact publication metadata. */
    public void verifyEmptyPublication(BlobObjectId object) throws IOException {
        if (object.size() != 0) throw new IllegalArgumentException("Expected an empty object identity");
        verifyHead(objectKey(object.tenantScope(), object.checksum()), object.checksum(), 0);
    }

    private void verifyHead(String key, String checksum, long size) throws IOException {
        try {
            HeadObjectResponse response = client.headObject(headRequest(key));
            verifyMetadata(response.contentLength(), response.metadata(), checksum, size);
            verifyEncryption(response.serverSideEncryption(), response.ssekmsKeyId());
        } catch (SdkException failure) {
            throw ioFailure("Failed to verify governed FILE publication", failure);
        }
    }

    private void verifyEncryption(
            ServerSideEncryption actualEncryption, String actualKmsKey) throws IOException {
        ServerSideEncryption expected = settings.encryption().mode();
        if (actualEncryption != expected) {
            throw new IOException("S3 FILE object does not satisfy encryption policy");
        }
        if (expected == ServerSideEncryption.AWS_KMS
                && !kmsKeyMatches(settings.encryption().kmsKeyId(), actualKmsKey)) {
            throw new IOException("S3 FILE object does not use the configured KMS key");
        }
    }

    private static boolean kmsKeyMatches(String configured, String actual) {
        return configured.equals(actual)
                || (actual != null && !configured.startsWith("alias/")
                && actual.endsWith("/" + configured));
    }

    private static void verifyMetadata(
            Long actualSize, Map<String, String> metadata,
            String expectedChecksum, long expectedSize) throws IOException {
        String storedChecksum = metadata.get("sha256");
        String storedSize = metadata.get("content-length");
        if (actualSize == null || actualSize != expectedSize
                || !expectedChecksum.equals(storedChecksum)
                || !Long.toString(expectedSize).equals(storedSize)) {
            throw new IOException("S3 content-addressed object failed integrity validation");
        }
    }

    private String objectKey(String tenantScope, String checksum) {
        String base = "tenants/" + tenantScope + "/sha256/"
                + checksum.substring(0, 2) + "/" + checksum;
        return settings.prefix().isEmpty() ? base : settings.prefix() + "/" + base;
    }

    /** Stable physical identity of an object published through this managed backend. */
    public String objectUri(BlobObjectId object) {
        return "s3://" + settings.bucket() + "/" + objectKey(object.tenantScope(), object.checksum());
    }

    /** Canonical bucket/prefix identity bound into a COPY staging admission. */
    public String rootUri() {
        return "s3://" + settings.bucket()
                + (settings.prefix().isEmpty() ? "" : "/" + settings.prefix());
    }

    /**
     * Non-secret identity of the configured physical backend, distinct from the
     * logical s3:// bucket/prefix. Production clients are built from these same
     * settings; a task admission must bind this digest before any managed I/O.
     */
    public String backendIdentitySha256() {
        if (ownsClient && settings.endpoint() == null) {
            throw new IllegalStateException(
                    "COPY managed backend requires an explicit HTTPS endpoint override");
        }
        URI configuredEndpoint = client.serviceClientConfiguration().endpointOverride()
                .orElseThrow(() -> new IllegalStateException(
                        "COPY managed backend has no explicit client endpoint override"));
        URI physicalEndpoint = canonicalPhysicalEndpoint(configuredEndpoint);
        if (settings.endpoint() != null && !settings.endpoint().equals(physicalEndpoint)) {
            throw new IllegalStateException(
                    "COPY managed backend client endpoint differs from its storage settings");
        }
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            writeIdentityField(out, physicalEndpoint.toString());
            writeIdentityField(out, settings.region());
            writeIdentityField(out, settings.bucket());
            writeIdentityField(out, settings.prefix());
            writeIdentityField(out, settings.expectedBucketOwner());
            writeIdentityField(out, settings.encryption().mode().toString());
            writeIdentityField(out, settings.encryption().kmsKeyId());
            out.writeBoolean(settings.pathStyleAccess());
            out.flush();
            return Sha256Digests.sha256Hex(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("S3 backend identity cannot be encoded", failure);
        }
    }

    private static URI canonicalPhysicalEndpoint(URI endpoint) {
        if (endpoint.getHost() == null || endpoint.getRawUserInfo() != null
                || endpoint.getRawQuery() != null || endpoint.getRawFragment() != null
                || !("http".equalsIgnoreCase(endpoint.getScheme())
                    || "https".equalsIgnoreCase(endpoint.getScheme()))
                || (endpoint.getRawPath() != null && !endpoint.getRawPath().isEmpty()
                    && !"/".equals(endpoint.getRawPath()))) {
            throw new IllegalStateException("COPY managed backend endpoint is not an origin");
        }
        String scheme = endpoint.getScheme().toLowerCase(java.util.Locale.ROOT);
        int port = endpoint.getPort();
        if (("https".equals(scheme) && port == HTTPS_PORT)
                || ("http".equals(scheme) && port == HTTP_PORT)) {
            port = -1;
        }
        try {
            return new URI(scheme, null, endpoint.getHost().toLowerCase(
                    java.util.Locale.ROOT), port, null, null, null);
        } catch (URISyntaxException failure) {
            throw new IllegalStateException("COPY managed backend endpoint is invalid", failure);
        }
    }

    private static void writeIdentityField(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static S3Client buildClient(Settings settings) {
        Objects.requireNonNull(settings, "settings must not be null");
        var builder = S3Client.builder()
                .region(Region.of(settings.region()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(settings.pathStyleAccess()).build());
        if (settings.endpoint() != null) {
            builder.endpointOverride(settings.endpoint());
        }
        return builder.build();
    }

    private static S3Client buildCopyClient(Settings settings) {
        Objects.requireNonNull(settings, "settings must not be null");
        var builder = S3Client.builder()
                .region(Region.of(settings.region()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(settings.pathStyleAccess()).build());
        if (settings.endpoint() != null) {
            builder.endpointOverride(settings.endpoint());
        }
        CopyS3ClientPolicy.configure(builder);
        return builder.build();
    }

    private static IOException ioFailure(String message, Exception failure) {
        return new IOException(message, failure);
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }

    /** Required server-side encryption policy. */
    public record Encryption(ServerSideEncryption mode, String kmsKeyId) {
        public Encryption {
            if (!supportedEncryption(mode)) {
                throw new IllegalArgumentException(
                        "S3 FILE encryption must be AES256 or aws:kms");
            }
            kmsKeyId = kmsKeyId == null ? "" : kmsKeyId.trim();
            if (requiresKmsKey(mode) && kmsKeyId.isEmpty()) {
                throw new IllegalArgumentException("S3 FILE KMS key id is required");
            }
            if (kmsKeyId.startsWith("alias/")) {
                throw new IllegalArgumentException(
                        "S3 FILE KMS configuration must use a key id or ARN, not an alias");
            }
            if (!requiresKmsKey(mode) && !kmsKeyId.isEmpty()) {
                throw new IllegalArgumentException(
                        "S3 FILE KMS key id is only valid with aws:kms");
            }
        }

        private static boolean supportedEncryption(ServerSideEncryption value) {
            return value == ServerSideEncryption.AES256
                    || value == ServerSideEncryption.AWS_KMS;
        }

        private static boolean requiresKmsKey(ServerSideEncryption value) {
            return value == ServerSideEncryption.AWS_KMS;
        }
    }

    /** Immutable, validated S3-compatible backend configuration. */
    public record Settings(
            String bucket,
            String region,
            URI endpoint,
            String prefix,
            Encryption encryption,
            String expectedBucketOwner,
            boolean pathStyleAccess) {
        public Settings {
            bucket = requireMatch(bucket, BUCKET, "S3 FILE bucket");
            region = requireMatch(region, REGION, "S3 FILE region");
            prefix = normalizePrefix(prefix);
            encryption = Objects.requireNonNull(encryption, "encryption must not be null");
            expectedBucketOwner = expectedBucketOwner == null
                    ? "" : expectedBucketOwner.trim();
            if (!expectedBucketOwner.isEmpty()
                    && !expectedBucketOwner.matches("[0-9]{12}")) {
                throw new IllegalArgumentException(
                        "S3 FILE expected bucket owner must be a 12-digit account id");
            }
            validateEndpoint(endpoint);
            endpoint = canonicalEndpoint(endpoint);
        }

        private static URI canonicalEndpoint(URI value) {
            if (value == null) {
                return null;
            }
            int port = value.getPort() == HTTPS_PORT ? -1 : value.getPort();
            try {
                return new URI("https", null, value.getHost().toLowerCase(
                        java.util.Locale.ROOT), port, null, null, null);
            } catch (URISyntaxException failure) {
                throw new IllegalArgumentException("S3 FILE endpoint is invalid", failure);
            }
        }

        private static String normalizePrefix(String value) {
            String normalized = value == null ? "" : value.trim();
            while (normalized.endsWith("/")) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            if (!PREFIX.matcher(normalized).matches()
                    || normalized.startsWith("/")
                    || normalized.contains("..")) {
                throw new IllegalArgumentException("S3 FILE prefix is invalid");
            }
            return normalized;
        }

        private static void validateEndpoint(URI value) {
            if (value == null) {
                return;
            }
            if (!"https".equalsIgnoreCase(value.getScheme())) {
                invalidEndpoint();
            }
            if (value.getHost() == null) {
                invalidEndpoint();
            }
            requireRootPath(value.getPath());
            if (hasEndpointDecorations(value)) {
                invalidEndpoint();
            }
        }

        private static void requireRootPath(String path) {
            if (path != null && !path.isEmpty() && !"/".equals(path)) {
                invalidEndpoint();
            }
        }

        private static boolean hasEndpointDecorations(URI value) {
            return value.getUserInfo() != null
                    || value.getQuery() != null
                    || value.getFragment() != null;
        }

        private static void invalidEndpoint() {
            throw new IllegalArgumentException(
                    "S3 FILE endpoint must be an absolute HTTPS origin");
        }

        private static String requireMatch(String value, Pattern pattern, String label) {
            String normalized = value == null ? "" : value.trim();
            if (!pattern.matcher(normalized).matches()) {
                throw new IllegalArgumentException(label + " is invalid");
            }
            return normalized;
        }
    }
}
