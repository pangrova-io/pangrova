package org.pangrova.worker.ai.blob;

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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** S3-compatible durable backend for governed FILE objects. */
public final class S3BlobStorage implements BlobStorage {

    private static final Pattern BUCKET = Pattern.compile(
            "(?=.{3,63}$)[a-z0-9][a-z0-9.-]*[a-z0-9]");
    private static final Pattern REGION = Pattern.compile("[A-Za-z0-9][A-Za-z0-9-]{0,62}");
    private static final Pattern PREFIX = Pattern.compile("[A-Za-z0-9!_.*'()/=-]*");
    private static final int PRECONDITION_FAILED = 412;

    private final S3Client client;
    private final Settings settings;
    private final boolean ownsClient;

    /** Creates a production backend using the AWS SDK default credential chain. */
    public S3BlobStorage(Settings settings) {
        this(buildClient(settings), settings, true);
    }

    S3BlobStorage(S3Client client, Settings settings) {
        this(client, settings, false);
    }

    private S3BlobStorage(S3Client client, Settings settings, boolean ownsClient) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.ownsClient = ownsClient;
    }

    @Override
    public void publish(
            String tenantScope, String checksum, long size, Path stagedContent)
            throws IOException {
        String key = objectKey(tenantScope, checksum);
        PutObjectRequest request = putRequest(key, checksum, size);
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
    public InputStream open(String tenantScope, String checksum, long size) throws IOException {
        String key = objectKey(tenantScope, checksum);
        try {
            ResponseInputStream<GetObjectResponse> stream = client.getObject(
                    getRequest(key));
            try {
                verifyMetadata(
                        stream.response().contentLength(), stream.response().metadata(),
                        checksum, size);
                verifyEncryption(
                        stream.response().serverSideEncryption(),
                        stream.response().ssekmsKeyId());
                return stream;
            } catch (IOException failure) {
                stream.close();
                throw failure;
            }
        } catch (SdkException failure) {
            throw ioFailure("Failed to open governed FILE object", failure);
        }
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
