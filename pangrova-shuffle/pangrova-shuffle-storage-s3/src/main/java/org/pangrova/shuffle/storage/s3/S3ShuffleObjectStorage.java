package org.pangrova.shuffle.storage.s3;

import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageContext;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * S3-compatible immutable batch storage using conditional object creation.
 *
 * <p>Configuration properties (keys shared with the service config section):
 * {@code bucket} (required), {@code prefix} (default {@code pangrova/shuffle}),
 * {@code region} (default {@code us-east-1}), {@code endpoint} (optional override),
 * {@code path-style-access} (default {@code false}), {@code api-call-timeout-seconds}
 * (default 120), {@code api-call-attempt-timeout-seconds} (default 45).
 */
public final class S3ShuffleObjectStorage implements ShuffleObjectStorage {

    private static final int MAX_DELETE_OBJECTS = 1000;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_PRECONDITION_FAILED = 412;
    private static final String DEFAULT_PREFIX = "pangrova/shuffle";
    private static final String DEFAULT_REGION = "us-east-1";
    private static final int DEFAULT_CALL_TIMEOUT_SECONDS = 120;
    private static final int DEFAULT_ATTEMPT_TIMEOUT_SECONDS = 45;

    private final S3Client client;
    private final String bucket;
    private final String prefix;

    private S3ShuffleObjectStorage(
            S3Client client,
            String bucket,
            String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public static S3ShuffleObjectStorage create(ShuffleObjectStorageContext context) {
        String bucket = context.get("bucket")
                .orElseThrow(() -> new IllegalArgumentException(
                        "s3 object storage requires 'bucket'"));
        String prefix = context.getOrDefault("prefix", DEFAULT_PREFIX);
        String region = context.getOrDefault("region", DEFAULT_REGION);
        Duration callTimeout = Duration.ofSeconds(Integer.parseInt(
                context.getOrDefault("api-call-timeout-seconds",
                        String.valueOf(DEFAULT_CALL_TIMEOUT_SECONDS))));
        Duration attemptTimeout = Duration.ofSeconds(Integer.parseInt(
                context.getOrDefault("api-call-attempt-timeout-seconds",
                        String.valueOf(DEFAULT_ATTEMPT_TIMEOUT_SECONDS))));
        boolean pathStyle = Boolean.parseBoolean(
                context.getOrDefault("path-style-access", "false"));
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(region))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(callTimeout)
                        .apiCallAttemptTimeout(attemptTimeout)
                        .build())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(pathStyle)
                        .build());
        context.get("endpoint")
                .map(java.net.URI::create)
                .ifPresent(builder::endpointOverride);
        return new S3ShuffleObjectStorage(builder.build(), bucket, prefix);
    }

    public static S3ShuffleObjectStorage testing(
            S3Client client,
            String bucket,
            String prefix) {
        return new S3ShuffleObjectStorage(client, bucket, prefix);
    }

    @Override
    public void putImmutable(
            String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256) {
        String objectKey = objectKey(key);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .checksumSHA256(Base64.getEncoder().encodeToString(sha256))
                .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                        .putHeader("If-None-Match", "*")
                        .build())
                .build();
        try {
            client.putObject(request, RequestBody.fromContentProvider(
                    data::get, sizeBytes, "application/octet-stream"));
        } catch (S3Exception conflict) {
            if (conflict.statusCode() != HTTP_CONFLICT
                    && conflict.statusCode() != HTTP_PRECONDITION_FAILED) {
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
        ResponseBytes<GetObjectResponse> response = client.getObjectAsBytes(
                GetObjectRequest.builder().bucket(bucket).key(objectKey(key)).build());
        return response.asByteArray();
    }

    @Override
    public void verifyStored(String key, long sizeBytes, byte[] sha256) {
        var response = client.headObject(HeadObjectRequest.builder()
                .bucket(bucket).key(objectKey(key)).checksumMode(ChecksumMode.ENABLED).build());
        String expected = Base64.getEncoder().encodeToString(sha256);
        if (response.contentLength() != sizeBytes
                || response.checksumSHA256() == null
                || !response.checksumSHA256().equals(expected)) {
            throw new IllegalStateException(
                    "durable shuffle object metadata does not match accepted batch");
        }
    }

    @Override
    public List<String> listPrefix(String keyPrefix) {
        List<String> keys = new ArrayList<>();
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket).prefix(objectKey(keyPrefix)).build();
        var response = client.listObjectsV2(request);
        while (true) {
            response.contents().stream()
                    .map(object -> stripPrefix(object.key()))
                    .forEach(keys::add);
            if (!response.isTruncated()) {
                return keys;
            }
            response = client.listObjectsV2(request.toBuilder()
                    .continuationToken(response.nextContinuationToken())
                    .build());
        }
    }

    @Override
    public void delete(List<String> keys) {
        for (int offset = 0; offset < keys.size(); offset += MAX_DELETE_OBJECTS) {
            int end = Math.min(offset + MAX_DELETE_OBJECTS, keys.size());
            List<ObjectIdentifier> objects = new ArrayList<>(end - offset);
            for (String key : keys.subList(offset, end)) {
                objects.add(ObjectIdentifier.builder().key(objectKey(key)).build());
            }
            var response = client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(Delete.builder().objects(objects).quiet(true).build())
                    .build());
            if (!response.errors().isEmpty()) {
                var first = response.errors().get(0);
                throw new IllegalStateException("shuffle object cleanup failed for "
                        + response.errors().size() + " object(s): key=" + first.key()
                        + " code=" + first.code());
            }
        }
    }

    @Override
    public String deletePrefixPage(String keyPrefix, String after, int limit) {
        if (limit <= 0 || limit > MAX_DELETE_OBJECTS) {
            throw new IllegalArgumentException("object cleanup page limit is invalid");
        }
        ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
                .bucket(bucket).prefix(objectKey(keyPrefix)).maxKeys(limit);
        if (after != null) {
            request.startAfter(objectKey(after));
        }
        var response = client.listObjectsV2(request.build());
        List<String> keys = response.contents().stream()
                .map(object -> stripPrefix(object.key())).toList();
        delete(keys);
        return response.isTruncated() && !keys.isEmpty() ? keys.get(keys.size() - 1) : null;
    }

    @Override
    public void close() {
        client.close();
    }

    private String objectKey(String key) {
        return prefix + "/" + key;
    }

    private String stripPrefix(String key) {
        return key.substring(prefix.length() + 1);
    }
}
