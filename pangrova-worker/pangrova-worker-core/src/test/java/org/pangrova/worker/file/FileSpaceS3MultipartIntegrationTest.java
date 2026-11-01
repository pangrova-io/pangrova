package org.pangrova.worker.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.UploadedPart;
import org.pangrova.filesystem.api.object.ObjectLocation;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.filesystem.s3.object.S3ObjectStoreAccessor;
import org.pangrova.filesystem.s3.object.S3ObjectStoreAccessorProvider;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;

/** Worker multipart grant renewal across independent resource/provider owners, using real MinIO. */
class FileSpaceS3MultipartIntegrationTest extends ContainerTestBase {
    private static final String ACCESS_KEY = "file-plane-test";
    private static final String SECRET_KEY = "file-plane-test-secret";
    private static final int FIVE_MIB = 5 * 1024 * 1024;
    private static final GenericContainer<?> MINIO = new GenericContainer<>(
            "minio/minio:RELEASE.2024-12-18T13-15-44Z")
            .withExposedPorts(9000).withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY).withCommand("server", "/data");

    @BeforeAll static void startMinio() { MINIO.start(); }
    @AfterAll static void stopMinio() { MINIO.stop(); }

    @Test
    void renewedMultipartGrantSurvivesOriginCleanupOnRealS3() throws IOException {
        var now = new java.util.concurrent.atomic.AtomicLong(1_000L);
        var mapper = org.pangrova.common.json.jackson.JsonMapperFactory.create();
        var credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));
        try (S3Client client = client();
                var originFactory = new ObjectStoreAccessorFactory(List.of(new S3ObjectStoreAccessorProvider(credentials)));
                var nextFactory = new ObjectStoreAccessorFactory(List.of(new S3ObjectStoreAccessorProvider(credentials)))) {
            String bucket = bucket(client);
            try (var origin = new FileSpaceFileResource(originFactory, SECRET_KEY, mapper, now::get);
                    var next = new FileSpaceFileResource(nextFactory, SECRET_KEY, mapper, now::get)) {
                String endpoint = endpoint();
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
    }

    private static InputStream stream(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String endpoint() {
        return "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000);
    }

    private static S3Client client() {
        return S3Client.builder().endpointOverride(URI.create(endpoint())).region(Region.US_EAST_1)
                .forcePathStyle(true).credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY))).build();
    }

    private static String bucket(S3Client client) {
        String bucket = "multipart-renewal-" + UUID.randomUUID();
        client.createBucket(request -> request.bucket(bucket));
        return bucket;
    }
}
