package org.pangrova.shuffle.storage.oss;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageContext;

/**
 * Aliyun OSS immutable batch storage using forbid-overwrite publication.
 *
 * <p>OSS has no {@code If-None-Match} pre-condition; immutability is enforced with the native
 * {@code x-oss-forbid-overwrite} header, which makes the server reject overwrites with
 * {@code 409 FileAlreadyExists}. The SHA-256 is stored in user metadata and verified by
 * {@link #verifyStored} via a metadata-only HEAD.
 *
 * <p>Configuration properties: {@code bucket} (required), {@code endpoint} (required — OSS
 * endpoints embed the region), {@code prefix} (default {@code pangrova/shuffle}),
 * {@code access-key-id}/{@code access-key-secret} (optional — fall back to the
 * {@code ALIBABA_CLOUD_ACCESS_KEY_ID}/{@code ALIBABA_CLOUD_ACCESS_KEY_SECRET} environment
 * variables), {@code api-call-timeout-seconds} (default 120),
 * {@code api-call-attempt-timeout-seconds} (default 45).
 */
public final class OssShuffleObjectStorage implements ShuffleObjectStorage {

    private static final int MAX_DELETE_OBJECTS = 1000;
    private static final String DEFAULT_PREFIX = "pangrova/shuffle";
    private static final int DEFAULT_CALL_TIMEOUT_SECONDS = 120;
    private static final int DEFAULT_ATTEMPT_TIMEOUT_SECONDS = 45;
    private static final String META_SHA256 = "sha256";

    private final OSS client;
    private final String bucket;
    private final String prefix;

    private OssShuffleObjectStorage(OSS client, String bucket, String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public static OssShuffleObjectStorage create(ShuffleObjectStorageContext context) {
        String bucket = context.get("bucket")
                .orElseThrow(() -> new IllegalArgumentException(
                        "oss object storage requires 'bucket'"));
        String endpoint = context.get("endpoint")
                .orElseThrow(() -> new IllegalArgumentException(
                        "oss object storage requires 'endpoint'"));
        String prefix = context.getOrDefault("prefix", DEFAULT_PREFIX);
        int callTimeoutSeconds = Integer.parseInt(context.getOrDefault(
                "api-call-timeout-seconds", String.valueOf(DEFAULT_CALL_TIMEOUT_SECONDS)));
        int attemptTimeoutSeconds = Integer.parseInt(context.getOrDefault(
                "api-call-attempt-timeout-seconds", String.valueOf(DEFAULT_ATTEMPT_TIMEOUT_SECONDS)));
        ClientBuilderConfiguration configuration = new ClientBuilderConfiguration();
        configuration.setConnectionTimeout(attemptTimeoutSeconds * 1000);
        configuration.setSocketTimeout(callTimeoutSeconds * 1000);
        OSSClientBuilder builder = new OSSClientBuilder();
        OSS client;
        if (context.get("access-key-id").isPresent() && context.get("access-key-secret").isPresent()) {
            client = builder.build(endpoint,
                    new DefaultCredentialProvider(
                            context.get("access-key-id").orElseThrow(),
                            context.get("access-key-secret").orElseThrow()),
                    configuration);
        } else {
            client = builder.build(endpoint,
                    new EnvironmentVariableCredentialsProvider(), configuration);
        }
        return new OssShuffleObjectStorage(client, bucket, prefix);
    }

    public static OssShuffleObjectStorage testing(OSS client, String bucket, String prefix) {
        return new OssShuffleObjectStorage(client, bucket, prefix);
    }

    @Override
    public void putImmutable(String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(sizeBytes);
        metadata.setHeader("x-oss-forbid-overwrite", "true");
        metadata.addUserMetadata(META_SHA256, Sha256.base64(sha256));
        try {
            client.putObject(new PutObjectRequest(bucket, objectKey(key), data.get(), metadata));
        } catch (com.aliyun.oss.OSSException conflict) {
            // OSSException in SDK 3.17.x carries no HTTP status; the code comes from the
            // error XML body the server sends alongside the 409.
            if (!"FileAlreadyExists".equals(conflict.getErrorCode())) {
                throw conflict;
            }
            byte[] existing = get(key);
            if (!MessageDigest.isEqual(sha256, Sha256.digest(existing))) {
                throw new IllegalStateException(
                        "immutable shuffle object conflicts with existing content", conflict);
            }
        }
    }

    @Override
    public byte[] get(String key) {
        try (OSSObject object = client.getObject(bucket, objectKey(key))) {
            try (InputStream in = object.getObjectContent()) {
                return in.readAllBytes();
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("Failed to read shuffle object " + key, e);
        }
    }

    @Override
    public void verifyStored(String key, long sizeBytes, byte[] sha256) {
        ObjectMetadata metadata = client.getObjectMetadata(bucket, objectKey(key));
        String expected = Sha256.base64(sha256);
        String actual = metadata.getUserMetadata().get(META_SHA256);
        if (metadata.getContentLength() != sizeBytes
                || actual == null
                || !actual.equals(expected)) {
            throw new IllegalStateException(
                    "durable shuffle object metadata does not match accepted batch");
        }
    }

    @Override
    public List<String> listPrefix(String keyPrefix) {
        List<String> keys = new ArrayList<>();
        ListObjectsV2Request request = new ListObjectsV2Request(bucket)
                .withPrefix(objectKey(keyPrefix));
        ListObjectsV2Result result = client.listObjectsV2(request);
        while (true) {
            for (OSSObjectSummary summary : result.getObjectSummaries()) {
                keys.add(stripPrefix(summary.getKey()));
            }
            if (!result.isTruncated()) {
                return keys;
            }
            result = client.listObjectsV2(request.withContinuationToken(
                    result.getNextContinuationToken()));
        }
    }

    @Override
    public void delete(List<String> keys) {
        for (int offset = 0; offset < keys.size(); offset += MAX_DELETE_OBJECTS) {
            int end = Math.min(offset + MAX_DELETE_OBJECTS, keys.size());
            List<String> objectKeys = keys.subList(offset, end).stream()
                    .map(this::objectKey).toList();
            // Verbose mode: OSS still answers 200 when individual deletes fail, and the
            // SDK 3.17.x parser drops the <Error> entries, so failures are detected by
            // absence from the <Deleted> list.
            DeleteObjectsResult result = client.deleteObjects(
                    new DeleteObjectsRequest(bucket).withKeys(objectKeys));
            Set<String> deleted = new HashSet<>(result.getDeletedObjects());
            List<String> failed = objectKeys.stream()
                    .filter(key -> !deleted.contains(key)).toList();
            if (!failed.isEmpty()) {
                throw new IllegalStateException("shuffle object cleanup failed for "
                        + failed.size() + " object(s): key=" + failed.get(0));
            }
        }
    }

    @Override
    public String deletePrefixPage(String keyPrefix, String after, int limit) {
        if (limit <= 0 || limit > MAX_DELETE_OBJECTS) {
            throw new IllegalArgumentException("object cleanup page limit is invalid");
        }
        ListObjectsV2Request request = new ListObjectsV2Request(bucket)
                .withPrefix(objectKey(keyPrefix)).withMaxKeys(limit);
        if (after != null) {
            request.setStartAfter(objectKey(after));
        }
        ListObjectsV2Result result = client.listObjectsV2(request);
        List<String> keys = result.getObjectSummaries().stream()
                .map(summary -> stripPrefix(summary.getKey())).toList();
        delete(keys);
        return result.isTruncated() && !keys.isEmpty() ? keys.get(keys.size() - 1) : null;
    }

    @Override
    public void close() {
        client.shutdown();
    }

    private String objectKey(String key) {
        return prefix + "/" + key;
    }

    private String stripPrefix(String key) {
        return key.substring(prefix.length() + 1);
    }
}
