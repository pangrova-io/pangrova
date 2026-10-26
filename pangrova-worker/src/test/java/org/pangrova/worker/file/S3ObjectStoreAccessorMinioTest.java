package org.pangrova.worker.file;

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
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileStat;
import org.pangrova.spi.storage.file.ObjectLocation;
import org.pangrova.spi.storage.file.UploadedPart;
import org.pangrova.testing.ContainerTestBase;
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

    @Test
    void putHeadRangeAndDeleteRoundTrip() throws IOException {
        try (S3Client client = client()) {
            String bucket = bucket(client, false);
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
    void renewedMultipartGrantSurvivesOriginCleanupOnRealS3() throws IOException {
        var now = new java.util.concurrent.atomic.AtomicLong(1_000L);
        var mapper = org.pangrova.common.json.JsonMapperFactory.create();
        var credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));
        try (S3Client client = client();
                var originFactory = new ObjectStoreAccessorFactory(credentials);
                var nextFactory = new ObjectStoreAccessorFactory(credentials)) {
            String bucket = bucket(client, false);
            var origin = new FileSpaceFileResource(originFactory, SECRET_KEY, mapper, now::get);
            var next = new FileSpaceFileResource(nextFactory, SECRET_KEY, mapper, now::get);
            String endpoint = location(bucket, "landing").profile().endpoint();
            var initial = new FileAccessDescriptor("minio", StorageType.S3, bucket, "", endpoint,
                    "us-east-1", true, "landing", "video/resume.mov", "write", 2_000L, "00000000-0000-0000-0000-000000000001", "alice", "tenant-a", "00000000-0000-0000-0000-000000000002", 1L);
            String encoded = java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(mapper.writeValueAsBytes(initial));
            var session = origin.beginMultipart(new FileSpaceFileResource.BeginMultipartRequest(
                    encoded, org.pangrova.common.file.FileAccessSignature.sign(SECRET_KEY, encoded), null));
            byte[] head = new byte[FIVE_MIB];
            java.util.Arrays.fill(head, (byte) 'a');
            UploadedPart first = origin.uploadPart(initial, session.uploadId(), 1, head.length,
                    new ByteArrayInputStream(head));
            now.set(3_000L);
            var renewed = new FileAccessDescriptor("minio", StorageType.S3, bucket, "", endpoint,
                    "us-east-1", true, "landing", "video/resume.mov", "write", 9_000L, "00000000-0000-0000-0000-000000000001", "alice", "tenant-a", "00000000-0000-0000-0000-000000000002", 1L);
            var additional = origin.beginMultipart(renewed, null);
            UploadedPart second = next.uploadPart(renewed, session.uploadId(), 2, 4, stream("tail"));
            FileStat completed = next.completeMultipart(renewed, session.uploadId(), List.of(first, second));
            assertThat(completed.size()).isEqualTo(FIVE_MIB + 4L);
            try (InputStream input = new S3ObjectStoreAccessor(client).openRange(
                    new ObjectLocation(renewed.profile(), renewed.baseKey()), "video/resume.mov", 0, -1, null)) {
                assertThat(input.readAllBytes()).hasSize(FIVE_MIB + 4)
                        .startsWith(head).endsWith("tail".getBytes(StandardCharsets.UTF_8));
            }
            origin.abortMultipart(renewed, additional.uploadId());
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
        return S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(
                        "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build())
                .build();
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
