package org.pangrova.filesystem.s3.object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.filesystem.api.object.ObjectLocation;
import org.pangrova.filesystem.api.object.UploadedPart;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real S3-compatible behavior for the file-plane transfer layer. */
class S3ObjectStoreAccessorMinioTest extends ContainerTestBase {

    private static final String ACCESS_KEY = "file-plane-test";
    private static final String SECRET_KEY = "file-plane-test-secret";
    private static final int FIVE_MIB = 5 * 1024 * 1024;

    static final GenericContainer<?> MINIO = new GenericContainer<>(
            "minio/minio:RELEASE.2024-12-18T13-15-44Z")
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server", "/data");

    @BeforeAll
    static void startMinio() {
        isDockerReady();
        MINIO.start();
    }

    @AfterAll
    static void stopMinio() {
        MINIO.stop();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void replacementBetweenHeadAndDeleteSurvivesForMutableVersions(boolean suspended) throws IOException {
        try (S3Client writer = client()) {
            String bucket = bucket(writer, suspended);
            if (suspended) {
                writer.putBucketVersioning(request -> request.bucket(bucket).versioningConfiguration(
                        VersioningConfiguration.builder().status(BucketVersioningStatus.SUSPENDED).build()));
            }
            var location = location(bucket, "delete-race");
            String key = location.key("item");
            String etag = writer.putObject(request -> request.bucket(bucket).key(key),
                    software.amazon.awssdk.core.sync.RequestBody.fromString("before")).eTag();
            var replaced = new java.util.concurrent.atomic.AtomicBoolean();
            var deletes = new java.util.concurrent.atomic.AtomicInteger();
            var replaceAfterHead = new ExecutionInterceptor() {
                @Override
                public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attributes) {
                    if (context.request() instanceof software.amazon.awssdk.services.s3.model.DeleteObjectRequest) {
                        deletes.incrementAndGet();
                    }
                }
                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
                    if (context.response() instanceof software.amazon.awssdk.services.s3.model.HeadObjectResponse
                            && replaced.compareAndSet(false, true)) {
                        writer.putObject(request -> request.bucket(bucket).key(key),
                                software.amazon.awssdk.core.sync.RequestBody.fromString("replacement"));
                    }
                }
            };
            try (S3Client deleter = client(replaceAfterHead)) {
                assertThatThrownBy(() -> new S3ObjectStoreAccessor(deleter).delete(location, "item", etag))
                        .isInstanceOfSatisfying(PangrovaException.class, failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
                assertThat(replaced).isTrue();
                assertThat(deletes).hasValue(0);
                assertThat(writer.getObjectAsBytes(request -> request.bucket(bucket).key(key)).asUtf8String())
                        .isEqualTo("replacement");
            }
        }
    }

    @Test
    void putHeadRangeAndDeleteRoundTrip() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, true);
            var accessor = new S3ObjectStoreAccessor(client);
            ObjectLocation location = location(bucket, "landing");

            FileStat written = accessor.put(location, "images/a.bin",
                    stream("0123456789"), 10, "text/plain");

            assertThat(written.size()).isEqualTo(10);
            assertThat(written.providerVersion()).isNotBlank();
            // This fixture's ETag happens to equal MD5; the generic S3 port cannot
            // establish that guarantee for encrypted or compatible-provider objects.
            assertThat(written.etag().replace("\"", "")).isEqualTo(md5Of("0123456789"));
            assertThat(written.md5()).isNull();
            assertThat(read(accessor.openRange(location, "images/a.bin", 0, -1, null)))
                    .isEqualTo("0123456789");
            assertThat(accessor.list(location, "images", null, 10).files()).singleElement()
                    .satisfies(file -> {
                        assertThat(file.etag()).isEqualTo(written.etag());
                        assertThat(file.md5()).isNull();
                    });
            assertThat(read(accessor.openRange(location, "images/a.bin", 2, 3, null)))
                    .isEqualTo("234");
            assertThat(read(accessor.openRange(location, "images/a.bin", 8, -1, null)))
                    .isEqualTo("89");

            accessor.delete(location, "images/a.bin", written.providerVersion());
            assertThatThrownBy(() -> accessor.head(location, "images/a.bin"))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_OBJECT_NOT_FOUND));
        }
    }

    @ParameterizedTest
    @CsvSource({"false,true", "true,true", "false,false", "true,false"})
    void committedReceiptDoesNotDescribeAConcurrentWritersNewerVersion(boolean multipart, boolean versioned)
            throws IOException {
        try (S3Client otherWriter = client()) {
            String bucket = bucket(otherWriter, versioned);
            var location = location(bucket, "commit-receipt");
            var committed = new java.util.concurrent.atomic.AtomicReference<String>();
            var committedEtag = new java.util.concurrent.atomic.AtomicReference<String>();
            var writes = new java.util.concurrent.atomic.AtomicInteger();
            var replaceAfterCommit = new ExecutionInterceptor() {
                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
                    String version = null;
                    String etag = null;
                    if (!multipart && context.response()
                            instanceof software.amazon.awssdk.services.s3.model.PutObjectResponse response) {
                        version = response.versionId();
                        etag = response.eTag();
                    } else if (multipart && context.response()
                            instanceof software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse response) {
                        version = response.versionId();
                        etag = response.eTag();
                    }
                    if (etag != null) {
                        writes.incrementAndGet();
                        committed.set(version);
                        committedEtag.set(etag);
                        otherWriter.putObject(request -> request.bucket(bucket).key(location.key("item.bin")),
                                software.amazon.awssdk.core.sync.RequestBody.fromString("other-writer"));
                    }
                }
            };
            try (S3Client originalWriter = client(replaceAfterCommit)) {
                var accessor = new S3ObjectStoreAccessor(originalWriter);
                FileStat result = null;
                if (versioned) {
                    result = writeOwnedObject(accessor, location, multipart);
                } else {
                    assertThatThrownBy(() -> writeOwnedObject(accessor, location, multipart))
                            .isInstanceOfSatisfying(PangrovaException.class, failure -> {
                                assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE);
                                assertThat(failure).hasMessageContaining("committed");
                            });
                }
                assertThat(writes.get()).isEqualTo(1);
                assertThat(committedEtag.get()).isNotBlank();
                var latest = otherWriter.headObject(request -> request.bucket(bucket)
                        .key(location.key("item.bin")));
                assertThat(latest.eTag()).isNotEqualTo(committedEtag.get());
                if (versioned) {
                    assertThat(latest.versionId()).isNotEqualTo(committed.get());
                    assertThat(result.providerVersion()).isEqualTo(committed.get());
                    assertThat(result.size()).isEqualTo(5);
                    String ownedVersion = result.providerVersion();
                    assertThat(otherWriter.getObjectAsBytes(request -> request.bucket(bucket)
                            .key(location.key("item.bin")).versionId(ownedVersion)).asUtf8String())
                            .isEqualTo("owned");
                }
            }
        }
    }

    private static FileStat writeOwnedObject(S3ObjectStoreAccessor accessor, ObjectLocation location,
            boolean multipart) throws IOException {
        if (!multipart) return accessor.put(location, "item.bin", stream("owned"), 5, "text/plain");
        var upload = accessor.beginMultipart(location, "item.bin", "text/plain");
        var part = accessor.uploadPart(upload, 1, stream("owned"), 5);
        return accessor.completeMultipart(upload, List.of(part));
    }

    @Test
    void partialCloseAbortsItsBodyAndLeavesTheClientUsableForTheNextRange() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, false);
            var accessor = new S3ObjectStoreAccessor(client);
            var location = location(bucket, "partial-close");
            byte[] bytes = new byte[32 * 1024];
            for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) (i % 251);
            accessor.put(location, "body.bin", new ByteArrayInputStream(bytes), bytes.length,
                    "application/octet-stream");
            try (var input = accessor.openRange(location, "body.bin", 0, -1, null)) {
                assertThat(input.readNBytes(7)).isEqualTo(java.util.Arrays.copyOf(bytes, 7));
            }
            try (var next = accessor.openRange(location, "body.bin", 19, 11, null)) {
                assertThat(next.readAllBytes()).isEqualTo(java.util.Arrays.copyOfRange(bytes, 19, 30));
            }
        }
    }

    @Test
    void unversionedReadUsesProviderConditionAcrossConcurrentReplacement() throws IOException {
        try (S3Client writer = client()) {
            String bucket = bucket(writer, false);
            ObjectLocation location = location(bucket, "landing");
            var accessor = new S3ObjectStoreAccessor(writer);
            FileStat original = accessor.put(location, "race.bin", stream("original"), 8, null);
            ExecutionInterceptor replaceBeforeGet = new ExecutionInterceptor() {
                @Override
                public void beforeExecution(Context.BeforeExecution context,
                        ExecutionAttributes attributes) {
                    if (context.request() instanceof GetObjectRequest get) {
                        assertThat(get.ifMatch()).isEqualTo(original.etag());
                        writer.putObject(request -> request.bucket(bucket)
                                        .key(location.key("race.bin")),
                                software.amazon.awssdk.core.sync.RequestBody.fromString("replaced"));
                    }
                }
            };
            try (S3Client reader = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .endpointOverride(URI.create(location.profile().endpoint()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true).build())
                    .overrideConfiguration(config -> config.addExecutionInterceptor(replaceBeforeGet))
                    .build()) {
                var conditional = new S3ObjectStoreAccessor(reader);
                assertThatThrownBy(() -> conditional.openRange(location, "race.bin", 0, 4,
                        original.providerVersion()))
                        .isInstanceOfSatisfying(PangrovaException.class, failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));
            }
        }
    }

    @Test
    void versionedBucketRejectsStaleRangePreconditions() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, true);
            var accessor = new S3ObjectStoreAccessor(client);
            ObjectLocation location = location(bucket, "landing");
            FileStat first = accessor.put(location, "data/part.parquet",
                    stream("first-version"), 13, null);

            accessor.put(location, "data/part.parquet", stream("second-version"), 14, null);

            assertThatThrownBy(() -> accessor.openRange(location, "data/part.parquet",
                    0, 5, first.providerVersion()))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));

            FileStat current = accessor.head(location, "data/part.parquet");
            assertThat(read(accessor.openRange(location, "data/part.parquet", 0, -1,
                    current.providerVersion()))).isEqualTo("second-version");
        }
    }

    @Test
    void versionedBucketDeletesFromListedEtag() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, true);
            var accessor = new S3ObjectStoreAccessor(client);
            ObjectLocation location = location(bucket, "landing");
            accessor.put(location, "images/a.bin", stream("0123456789"), 10, null);

            FileStat listed = accessor.list(location, "images", null, 100).files().get(0);
            assertThat(listed.providerVersion()).isEqualTo(listed.etag());

            accessor.delete(location, listed.path(), listed.providerVersion());

            assertThatThrownBy(() -> accessor.head(location, listed.path()))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_OBJECT_NOT_FOUND));
        }
    }

    @Test
    void multipartUploadStitchesPartsAndDoesNotReportMd5() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, false);
            var accessor = new S3ObjectStoreAccessor(client);
            ObjectLocation location = location(bucket, "landing");
            byte[] head = new byte[FIVE_MIB];
            java.util.Arrays.fill(head, (byte) 'a');

            var upload = accessor.beginMultipart(location, "video/clip.mov", "video/quicktime");
            UploadedPart first = accessor.uploadPart(upload, 1,
                    new ByteArrayInputStream(head), head.length);
            UploadedPart second = accessor.uploadPart(upload, 2, stream("tail"), 4);
            FileStat completed = accessor.completeMultipart(upload, List.of(first, second));

            assertThat(completed.size()).isEqualTo(FIVE_MIB + 4L);
            assertThat(completed.md5()).isNull();
            try (InputStream input = accessor.openRange(location, "video/clip.mov", 0, -1, null)) {
                assertThat(input.readAllBytes()).hasSize(FIVE_MIB + 4)
                        .startsWith(head).endsWith("tail".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void listingPaginatesAcrossContinuationTokens() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, false);
            var accessor = new S3ObjectStoreAccessor(client);
            ObjectLocation location = location(bucket, "landing");
            for (int index = 0; index < 5; index++) {
                accessor.put(location, "parts/part-" + index + ".bin", stream("x"), 1, null);
            }

            List<String> seen = new ArrayList<>();
            String token = null;
            do {
                FileListing page = accessor.list(location, "parts", token, 2);
                page.files().forEach(file -> seen.add(file.path()));
                token = page.nextPageToken();
            } while (token != null);

            assertThat(seen).containsExactly(
                    "parts/part-0.bin", "parts/part-1.bin", "parts/part-2.bin",
                    "parts/part-3.bin", "parts/part-4.bin");
        }
    }

    @Test
    void presignedReadUrlServesTheObjectForAFixedLifetime() throws Exception {
        try (S3Client client = client(); S3Presigner presigner = presigner()) {
            String bucket = bucket(client, false);
            var accessor = new S3ObjectStoreAccessor(client, presigner);
            ObjectLocation location = location(bucket, "landing");
            accessor.put(location, "images/presigned.txt", stream("signed"), 6, "text/plain");

            long before = System.currentTimeMillis();
            var signed = accessor.presign(location, "images/presigned.txt",
                    java.time.Duration.ofMinutes(5), true);
            String url = signed.url();
            assertThat(signed.expiresAtEpochMillis()).isGreaterThan(before)
                    .isLessThanOrEqualTo(System.currentTimeMillis() + 300_000);

            assertThat(url).contains(bucket).contains("X-Amz-Signature");
            var response = java.net.http.HttpClient.newHttpClient().send(
                    java.net.http.HttpRequest.newBuilder(URI.create(url)).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("signed");
        }
    }

    private static String bucket(S3Client client, boolean versioned) {
        String name = "file-plane-" + UUID.randomUUID();
        client.createBucket(request -> request.bucket(name));
        if (versioned) {
            client.putBucketVersioning(request -> request.bucket(name)
                    .versioningConfiguration(VersioningConfiguration.builder()
                            .status(BucketVersioningStatus.ENABLED).build()));
        }
        return name;
    }

    private static ObjectLocation location(String bucket, String prefix) {
        StorageProfile profile = new StorageProfile("minio", StorageType.S3, bucket, prefix,
                "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000),
                "us-east-1",
                Map.of("access-key-id", ACCESS_KEY, "secret-access-key", SECRET_KEY),
                Map.of("path-style-access", "true"));
        return new ObjectLocation(profile, prefix + "/volumes/media/raw/landing");
    }

    private static S3Client client() {
        return client(null);
    }

    private static S3Client client(ExecutionInterceptor interceptor) {
        var builder = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build());
        if (interceptor != null) {
            builder.overrideConfiguration(config -> config.addExecutionInterceptor(interceptor));
        }
        return builder.build();
    }

    private static S3Presigner presigner() {
        return S3Presigner.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build())
                .build();
    }

    private static String md5Of(String value) throws IOException {
        try (InputStream input = stream(value)) {
            java.security.MessageDigest digest;
            try {
                digest = java.security.MessageDigest.getInstance("MD5");
            } catch (java.security.NoSuchAlgorithmException failure) {
                throw new IllegalStateException(failure);
            }
            return java.util.HexFormat.of().formatHex(
                    digest.digest(input.readAllBytes()));
        }
    }

    private static InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(InputStream input) throws IOException {
        try (input) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
