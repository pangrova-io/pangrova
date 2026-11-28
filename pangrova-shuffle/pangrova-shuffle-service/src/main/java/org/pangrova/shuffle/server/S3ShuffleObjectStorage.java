package org.pangrova.shuffle.server;

import org.pangrova.shuffle.checksum.ShuffleChecksums;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
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

/** S3-compatible immutable batch storage using conditional object creation. */
final class S3ShuffleObjectStorage implements ShuffleObjectStorage {

    private static final int MAX_DELETE_OBJECTS = 1000;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_PRECONDITION_FAILED = 412;

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

    static S3ShuffleObjectStorage create(
            ShuffleServiceConfig.ObjectStorage config) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(config.region()))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(config.apiCallTimeout())
                        .apiCallAttemptTimeout(config.apiCallAttemptTimeout())
                        .retryPolicy(requestSafeRetries())
                        .build())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(config.pathStyleAccess())
                        .build());
        if (config.endpoint() != null) {
            builder.endpointOverride(config.endpoint());
        }
        return new S3ShuffleObjectStorage(
                builder.build(), config.bucket(), config.prefix());
    }

    static RetryPolicy requestSafeRetries() {
        RetryPolicy defaults = RetryPolicy.defaultRetryPolicy();
        return defaults.toBuilder()
                .additionalRetryConditionsAllowed(false)
                // A timed-out PUT can finish after a replay succeeds. Keep the single
                // submission's uncertain outcome attached to its durable reservation.
                .retryCondition(context -> !(context.originalRequest() instanceof PutObjectRequest)
                        && defaults.retryCondition().shouldRetry(context))
                .build();
    }

    static S3ShuffleObjectStorage testing(
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
            byte[] existing = get(key, sizeBytes);
            if (!MessageDigest.isEqual(sha256, ShuffleChecksums.sha256(existing))) {
                throw new IllegalStateException(
                        "immutable shuffle object conflicts with existing content", conflict);
            }
        }
    }

    @Override
    public byte[] get(String key, long sizeBytes) {
        int size = Math.toIntExact(sizeBytes);
        if (size <= 0) {
            throw new IllegalArgumentException("shuffle object size must be positive");
        }
        byte[] data = client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(objectKey(key)).build(),
                (response, input) -> readExact(response, input, size));
        if (data.length != size) {
            throw new ShuffleServiceException(FailureKind.CORRUPT, "stored shuffle object has an unexpected length");
        }
        return data;
    }

    private static byte[] readExact(GetObjectResponse response, AbortableInputStream input, int size)
            throws IOException {
        if (response.contentLength() == null || response.contentLength() != size) {
            input.abort();
            return new byte[0];
        }
        byte[] bytes = new byte[size];
        if (input.readNBytes(bytes, 0, size) != size || input.read() != -1) {
            input.abort();
            return new byte[0];
        }
        return bytes;
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
    public ObjectPage listPrefixPage(String keyPrefix, String after, int limit) {
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
        Boolean truncated = response.isTruncated();
        if (truncated == null || (truncated && keys.isEmpty())
                || keys.stream().anyMatch(key -> !key.startsWith(keyPrefix))) {
            throw new IllegalStateException("Object storage returned an invalid shuffle cleanup page");
        }
        return new ObjectPage(keys, truncated ? keys.get(keys.size() - 1) : null);
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
