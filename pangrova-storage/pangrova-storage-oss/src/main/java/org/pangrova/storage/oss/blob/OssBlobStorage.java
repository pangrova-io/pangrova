package org.pangrova.storage.oss.blob;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OperationOptions;
import com.aliyun.sdk.service.oss2.credentials.EnvironmentVariableCredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.GetBucketVersioningRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import org.pangrova.storage.api.blob.BlobInputStream;
import org.pangrova.storage.api.blob.BlobObjectId;
import org.pangrova.storage.api.blob.BlobStorage;

/** Alibaba OSS publication in a dedicated, never-versioned bucket; not a COPY managed/source adapter. */
public final class OssBlobStorage implements BlobStorage {
    private static final long MAX_PUT_BYTES = 5L * 1024 * 1024 * 1024;
    private static final int BUFFER_SIZE = 8192;
    private static final int CONFLICT_STATUS = 409;
    private static final OperationOptions ONCE = OperationOptions.newBuilder().retryMaxAttempts(1).build();
    private final OSSClient client;
    private final Settings settings;

    /** Uses official OSS environment credentials and verified HTTPS with bounded socket/pool waits. */
    public OssBlobStorage(Settings settings) {
        this(settings, new EnvironmentVariableCredentialsProvider(), null);
    }

    /** Uses caller-supplied credentials/trust while owning the exact bounded, cancellable SDK transport. */
    public OssBlobStorage(Settings settings, CredentialsProvider credentials, SSLContext tlsContext) {
        this.settings = Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(credentials, "credentials");
        var initial = credentials.getCredentials();
        if (initial == null || !initial.hasKeys()) throw new IllegalArgumentException("OSS credentials are required");
        this.client = buildClient(settings, credentials, tlsContext);
    }

    @Override
    public void publish(BlobObjectId object, Path stagedContent) throws IOException {
        Objects.requireNonNull(object, "object");
        if (object.size() > MAX_PUT_BYTES) throw new IOException("OSS single-object publication exceeds 5 GiB");
        try (var draft = OssPublicationDraft.copy(object, stagedContent)) {
            requireNeverVersionedBucket();
            boolean existing = false;
            try (var input = Files.newInputStream(draft.path())) {
                var request = PutObjectRequest.newBuilder().bucket(settings.bucket()).key(key(object))
                        .forbidOverwrite(true).contentType("application/octet-stream")
                        .contentMd5(draft.contentMd5()).metadata(metadata(object))
                        .serverSideEncryption(settings.encryption().algorithm())
                        .body(BinaryData.fromStream(input, object.size()));
                if (!settings.encryption().kmsKeyId().isEmpty()) {
                    request.serverSideEncryptionKeyId(settings.encryption().kmsKeyId());
                }
                try {
                    var result = client.putObject(request.build(), ONCE);
                    requireUnversionedObject(result.versionId());
                } catch (RuntimeException failure) {
                    ServiceException service = ServiceException.asCause(failure);
                    if (service == null || service.statusCode() != CONFLICT_STATUS
                            || !"FileAlreadyExists".equals(service.errorCode())) throw failure;
                    existing = true;
                }
            }
            var head = client.headObject(HeadObjectRequest.newBuilder()
                    .bucket(settings.bucket()).key(key(object)).build(), ONCE);
            verify(object, head.contentLength(), head.metadata(), head.serverSideEncryption(),
                    head.serverSideEncryptionKeyId(), head.versionId());
            if (existing) {
                try (var input = open(object)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    while (input.read(buffer) != -1) { /* Validate the actual concurrent winner. */ }
                }
            }
        } catch (RuntimeException failure) {
            throw new IOException("OSS blob publication failed", failure);
        }
    }

    @Override
    public InputStream open(BlobObjectId object) throws IOException {
        Objects.requireNonNull(object, "object");
        try {
            var result = client.getObject(GetObjectRequest.newBuilder()
                    .bucket(settings.bucket()).key(key(object)).build(), ONCE);
            var body = new BlobInputStream(Objects.requireNonNull(result.body(), "OSS response body"), result::abort);
            try {
                verify(object, result.contentLength(), result.metadata(), result.serverSideEncryption(),
                        result.serverSideEncryptionKeyId(), result.versionId());
                return new OssContentStream(body, object);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    body.abort();
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } catch (RuntimeException failure) {
            throw new IOException("OSS blob read failed", failure);
        }
    }

    private void requireNeverVersionedBucket() throws IOException {
        var result = client.getBucketVersioning(GetBucketVersioningRequest.newBuilder()
                .bucket(settings.bucket()).build(), ONCE);
        // OSS ignores forbid-overwrite when versioning is enabled OR suspended.
        if (result.versioningConfiguration() == null || result.versioningConfiguration().status() != null) {
            throw new IOException("OSS publication requires a never-versioned bucket");
        }
    }

    private void verify(BlobObjectId object, Long length, Map<String, String> metadata,
                        String encryption, String kmsKey, String version) throws IOException {
        requireUnversionedObject(version);
        if (!Objects.equals(length, object.size())
                || !object.checksum().equals(metadata.get("sha256"))
                || !Long.toString(object.size()).equals(metadata.get("size"))) {
            throw new IOException("OSS object metadata does not match its size/SHA-256");
        }
        if (!settings.encryption().algorithm().equals(encryption)
                || !settings.encryption().kmsKeyId().equals(kmsKey == null ? "" : kmsKey)) {
            throw new IOException("OSS object does not match the required server-side encryption");
        }
    }

    private static void requireUnversionedObject(String version) throws IOException {
        if (version != null) throw new IOException("OSS object unexpectedly has a version id");
    }

    private static Map<String, String> metadata(BlobObjectId object) {
        return Map.of("sha256", object.checksum(), "size", Long.toString(object.size()));
    }

    private String key(BlobObjectId object) {
        return (settings.prefix().isEmpty() ? "" : settings.prefix() + "/")
                + object.tenantScope() + "/" + object.checksum() + ".blob";
    }

    private static OSSClient buildClient(Settings settings, CredentialsProvider credentials, SSLContext tlsContext) {
        Objects.requireNonNull(settings, "settings");
        var transport = new OssHttpTransport(settings, tlsContext);
        try {
            return OSSClient.newBuilder().region(settings.region()).endpoint(settings.endpoint().toString())
                    .usePathStyle(settings.pathStyleAccess()).retryMaxAttempts(1)
                    .credentialsProvider(credentials).httpClient(transport).build();
        } catch (RuntimeException | Error failure) {
            try {
                transport.close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
        } catch (Exception failure) {
            throw new IOException("OSS client close failed", failure);
        }
    }

    /** Required immutable server-side encryption policy; KMS uses an exact key id. */
    public record Encryption(String algorithm, String kmsKeyId) {
        public Encryption {
            if (!"AES256".equals(algorithm) && !"KMS".equals(algorithm)) {
                throw new IllegalArgumentException("OSS encryption must be AES256 or KMS");
            }
            kmsKeyId = kmsKeyId == null ? "" : kmsKeyId.trim();
            if (("KMS".equals(algorithm) && kmsKeyId.isEmpty())
                    || ("AES256".equals(algorithm) && !kmsKeyId.isEmpty())) {
                throw new IllegalArgumentException("OSS KMS requires a key id; AES256 must not specify one");
            }
        }
    }

    /** Explicit backend origin; administrators must keep the dedicated bucket never-versioned. */
    public record Settings(String bucket, String region, URI endpoint, String prefix, Encryption encryption,
                           boolean pathStyleAccess, Duration connectTimeout, Duration readTimeout) {
        private static final Pattern BUCKET = Pattern.compile("(?=.{3,63}$)[a-z0-9][a-z0-9-]*[a-z0-9]");
        private static final Pattern REGION = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");
        private static final Pattern PREFIX = Pattern.compile("[A-Za-z0-9!_.*'()/=-]*");
        private static final int OBJECT_KEY_SUFFIX_LENGTH = 135;
        private static final int MAX_OBJECT_KEY_LENGTH = 1023;
        private static final int MAX_PORT = 65535;

        public Settings {
            requireMatch("bucket", bucket, BUCKET);
            requireMatch("region", region, REGION);
            requireOrigin(endpoint);
            requirePrefix(prefix);
            Objects.requireNonNull(encryption, "encryption");
            requireTimeout(connectTimeout);
            requireTimeout(readTimeout);
        }

        private static void requireMatch(String name, String value, Pattern pattern) {
            if (value == null || !pattern.matcher(value).matches()) {
                throw new IllegalArgumentException("Invalid OSS " + name);
            }
        }

        private static void requireOrigin(URI endpoint) {
            if (endpoint == null || !"https".equalsIgnoreCase(endpoint.getScheme()) || endpoint.getHost() == null
                    || endpoint.getRawUserInfo() != null || endpoint.getRawQuery() != null
                    || endpoint.getRawFragment() != null
                    || (endpoint.getRawPath() != null && !endpoint.getRawPath().isEmpty())
                    || endpoint.getPort() == 0 || endpoint.getPort() > MAX_PORT) {
                throw new IllegalArgumentException("OSS endpoint must be an HTTPS origin without path or credentials");
            }
        }

        private static void requirePrefix(String prefix) {
            if (prefix == null || !PREFIX.matcher(prefix).matches()
                    || prefix.length() + OBJECT_KEY_SUFFIX_LENGTH > MAX_OBJECT_KEY_LENGTH
                    || prefix.startsWith("/") || prefix.endsWith("/") || prefix.contains("//")) {
                throw new IllegalArgumentException("Invalid OSS object prefix");
            }
            for (String segment : prefix.split("/")) {
                if (".".equals(segment) || "..".equals(segment)) {
                    throw new IllegalArgumentException("Invalid OSS prefix segment");
                }
            }
        }

        private static void requireTimeout(Duration value) {
            if (value == null || value.isNegative() || value.isZero() || value.toMillis() < 1
                    || value.toMillis() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid OSS socket timeout");
            }
        }
    }
}
