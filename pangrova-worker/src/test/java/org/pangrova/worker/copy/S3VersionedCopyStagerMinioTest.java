package org.pangrova.worker.copy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.worker.ai.blob.S3BlobStorage;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real S3-compatible version semantics; no SQL or production COPY wiring is implied. */
class S3VersionedCopyStagerMinioTest extends ContainerTestBase {

    private static final String ACCESS_KEY = "copy-version-test";
    private static final String SECRET_KEY = "copy-version-test-secret";

    static final GenericContainer<?> MINIO = new GenericContainer<>(
            "minio/minio:RELEASE.2024-12-18T13-15-44Z")
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withEnv("MINIO_KMS_SECRET_KEY",
                    "copy-test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
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

    @TempDir
    Path temporaryDirectory;

    @Test
    void retrievesThePinnedVersionAfterRealConcurrentOverwrite() throws Exception {
        String bucket = bucketName();
        String managedBucket = bucketName();
        byte[] oldContent = "first-parquet-version".getBytes(StandardCharsets.UTF_8);
        byte[] newContent = "second-parquet-version".getBytes(StandardCharsets.UTF_8);
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            String firstVersion = objects.putObject(request -> request.bucket(bucket)
                    .key("data/part.parquet"), RequestBody.fromBytes(oldContent)).versionId();
            assertThat(firstVersion).isNotBlank().isNotEqualTo("null");
            AtomicBoolean replaced = new AtomicBoolean();
            try (S3Client overwriteAfterHead = client(new ExecutionInterceptor() {
                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
                    if (context.request() instanceof HeadObjectRequest head
                            && head.bucket().equals(bucket) && replaced.compareAndSet(false, true)) {
                        objects.putObject(put -> put.bucket(bucket).key(head.key()),
                                RequestBody.fromBytes(newContent));
                    }
                }
            })) {
                var managed = managed(objects, managedBucket);
                var stager = new S3VersionedCopyStager(overwriteAfterHead, managed,
                        temporaryDirectory.resolve("drafts"), 1024);

                var result = stager.stage(profile(bucket),
                        "s3://" + bucket + "/data/part.parquet", "tenant-one");

                assertThat(replaced.get()).isTrue();
                assertThat(result.sourceVersionId()).isEqualTo(firstVersion);
                try (var input = managed.open(result.tenantScope(),
                        result.contentSha256(), result.contentSize())) {
                    assertThat(input.readAllBytes()).isEqualTo(oldContent);
                }
                assertThat(objects.getObjectAsBytes(request -> request.bucket(bucket)
                        .key("data/part.parquet")).asByteArray()).isEqualTo(newContent);
            }
        }
    }

    @Test
    void rejectsRealUnversionedBucket() {
        String bucket = bucketName();
        try (S3Client objects = client()) {
            objects.createBucket(request -> request.bucket(bucket));
            objects.putObject(request -> request.bucket(bucket).key("data/part.parquet"),
                    RequestBody.fromBytes(new byte[] {1, 2, 3}));
            var stager = new S3VersionedCopyStager(objects,
                    managed(objects, bucket),
                    temporaryDirectory.resolve("drafts"), 1024);

            assertThatThrownBy(() -> stager.stage(profile(bucket),
                    "s3://" + bucket + "/data/part.parquet", "tenant-one"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("non-null object version");
        }
    }

    @Test
    void duplicateExactVersionKeepsOneImmutableManagedObject() throws Exception {
        String bucket = bucketName();
        String managedBucket = bucketName();
        byte[] bytes = "same-source-version".getBytes(StandardCharsets.UTF_8);
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            String version = objects.putObject(request -> request.bucket(bucket)
                    .key("data/part.parquet"), RequestBody.fromBytes(bytes)).versionId();
            var managed = managed(objects, managedBucket);
            var stager = new S3VersionedCopyStager(objects, managed,
                    temporaryDirectory.resolve("drafts"), 1024);
            String url = "s3://" + bucket + "/data/part.parquet";

            var first = stager.stage(profile(bucket), url, "tenant-one");
            var second = stager.stage(profile(bucket), url, "tenant-one");

            assertThat(first).isEqualTo(second);
            assertThat(first.sourceVersionId()).isEqualTo(version);
            try (var input = managed.open(first.tenantScope(), first.contentSha256(), first.contentSize())) {
                assertThat(input.readAllBytes()).isEqualTo(bytes);
            }
        }
    }

    @Test
    void durableRetryReadsManagedObjectAfterOriginalVersionIsDeleted() throws Exception {
        String bucket = bucketName();
        String managedBucket = bucketName();
        byte[] bytes = "durable-retry-parquet-bytes".getBytes(StandardCharsets.UTF_8);
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            String version = objects.putObject(request -> request.bucket(bucket)
                    .key("data/part.parquet"), RequestBody.fromBytes(bytes)).versionId();
            var managed = managed(objects, managedBucket);
            String url = "s3://" + bucket + "/data/part.parquet";
            var first = new S3VersionedCopyStager(objects, managed,
                    temporaryDirectory.resolve("first-draft"), 1024)
                    .stage(profile(bucket), url, "tenant-one");
            assertThat(first.sourceVersionId()).isEqualTo(version);
            assertThat(first.managedObjectUri()).isEqualTo("s3://" + managedBucket + "/" + managedKey(first));

            objects.deleteObject(request -> request.bucket(bucket).key("data/part.parquet")
                    .versionId(version));
            assertThatThrownBy(() -> objects.headObject(request -> request.bucket(bucket)
                    .key("data/part.parquet"))).isInstanceOf(RuntimeException.class);

            var restarted = new S3VersionedCopyStager(objects, managed,
                    temporaryDirectory.resolve("retry-draft"), 1024);
            assertThat(restarted.resume(profile(bucket), url, "tenant-one", first)).isEqualTo(first);
            Path verifiedPath;
            try (var verified = restarted.materialize(profile(bucket), url, "tenant-one", first)) {
                verifiedPath = verified.path();
                assertThat(java.nio.file.Files.readAllBytes(verifiedPath)).isEqualTo(bytes);
            }
            assertThat(verifiedPath).doesNotExist();
            assertThatThrownBy(() -> restarted.resume(profile(bucket), url, "tenant-two", first))
                    .isInstanceOf(AccessDeniedException.class);
            var redirected = new S3VersionedCopyStager.StagedSource(first.profileName(),
                    first.sourceUrl(), first.sourceVersionId(),
                    "s3://" + bucket + "/" + managedKey(first), first.tenantScope(),
                    first.contentSha256(), first.contentSize());
            assertThatThrownBy(() -> restarted.resume(profile(bucket), url, "tenant-one", redirected))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    @Test
    void existingManagedObjectWithForgedMetadataCannotPassByteVerification() throws Exception {
        String bucket = bucketName();
        String managedBucket = bucketName();
        byte[] original = "valid-parquet-bytes".getBytes(StandardCharsets.UTF_8);
        byte[] corrupted = "wrong-parquet-bytes".getBytes(StandardCharsets.UTF_8);
        assertThat(corrupted.length).isEqualTo(original.length);
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            objects.putObject(request -> request.bucket(bucket).key("data/part.parquet"),
                    RequestBody.fromBytes(original));
            var managed = managed(objects, managedBucket);
            var stager = new S3VersionedCopyStager(objects, managed,
                    temporaryDirectory.resolve("drafts"), 1024);
            String url = "s3://" + bucket + "/data/part.parquet";
            var staged = stager.stage(profile(bucket), url, "tenant-one");
            String key = managedKey(staged);
            objects.putObject(PutObjectRequest.builder().bucket(managedBucket).key(key)
                    .serverSideEncryption(ServerSideEncryption.AES256)
                    .metadata(Map.of("sha256", staged.contentSha256(),
                            "content-length", Long.toString(staged.contentSize())))
                    .build(), RequestBody.fromBytes(corrupted));

            assertThatThrownBy(() -> stager.stage(profile(bucket), url, "tenant-one"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("byte integrity verification");
            assertThatThrownBy(() -> stager.resume(profile(bucket), url, "tenant-one", staged))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("byte integrity verification");
            assertThatThrownBy(() -> stager.materialize(profile(bucket), url, "tenant-one", staged))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("SHA-256/size verification");
        }
    }

    @Test
    void oversizedVersionIsRejectedBeforeManagedPublication() {
        String bucket = bucketName();
        String managedBucket = bucketName();
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            objects.putObject(request -> request.bucket(bucket).key("data/part.parquet"),
                    RequestBody.fromBytes(new byte[5]));
            var stager = new S3VersionedCopyStager(objects, managed(objects, managedBucket),
                    temporaryDirectory.resolve("drafts"), 4);

            assertThatThrownBy(() -> stager.stage(profile(bucket),
                    "s3://" + bucket + "/data/part.parquet", "tenant-one"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("exceeds the staging limit");
        }
    }

    @Test
    void differentBucketOrPrefixIsRejectedBeforeS3Access() {
        String bucket = bucketName();
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.putObject(request -> request.bucket(bucket).key("private/part.parquet"),
                    RequestBody.fromBytes(new byte[] {1}));
            var stager = new S3VersionedCopyStager(objects, managed(objects, bucket),
                    temporaryDirectory.resolve("drafts"), 1024);

            assertThatThrownBy(() -> stager.stage(profile(bucket),
                    "s3://" + bucket + "/private/part.parquet", "tenant-one"))
                    .isInstanceOf(AccessDeniedException.class);
            assertThatThrownBy(() -> stager.stage(profile(bucket),
                    "s3://" + bucketName() + "/data/part.parquet", "tenant-one"))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    @Test
    void taskReadAllowancePrecedesRealGetAndPrivateFileOwnershipPrecedesIo() throws Exception {
        String bucket = bucketName();
        String managedBucket = bucketName();
        byte[] bytes = "admitted-task-input".getBytes(StandardCharsets.UTF_8);
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            objects.putObject(request -> request.bucket(bucket).key("data/part.parquet"),
                    RequestBody.fromBytes(bytes));
            String url = "s3://" + bucket + "/data/part.parquet";
            var staged = new S3VersionedCopyStager(objects, managed(objects, managedBucket),
                    temporaryDirectory.resolve("stage"), 1024).stage(profile(bucket), url, "tenant-one");
            var gets = new java.util.concurrent.atomic.AtomicInteger();
            var owner = new java.util.concurrent.atomic.AtomicReference<VerifiedLocalCopySource>();
            try (S3Client counted = client(new ExecutionInterceptor() {
                @Override
                public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes attributes) {
                    if (context.request() instanceof software.amazon.awssdk.services.s3.model.GetObjectRequest get
                            && get.bucket().equals(managedBucket)) {
                        assertThat(owner.get()).isNotNull();
                        assertThatThrownBy(() -> owner.get().path()).isInstanceOf(IllegalStateException.class);
                        gets.incrementAndGet();
                    }
                }
            })) {
                var stager = new S3VersionedCopyStager(objects, managed(counted, managedBucket),
                        temporaryDirectory.resolve("native"), 1024);
                var denied = new CopyTaskReadBudget(0);
                assertThatThrownBy(() -> stager.materialize(profile(bucket), url, "tenant-one", staged,
                        new CopyMaterialization(denied, owner::set)))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("grant exhausted");
                assertThat(gets.get()).isZero();
                assertThat(denied.requestedBytes()).isZero();
                assertThatThrownBy(() -> managed(counted, managedBucket).open(staged.tenantScope(),
                        staged.contentSha256(), 0, denied::reserve))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive byte range");
                assertThat(gets.get()).isZero();
                var accepted = new CopyTaskReadBudget(bytes.length);
                Path path;
                try (var file = stager.materialize(profile(bucket), url, "tenant-one", staged,
                        new CopyMaterialization(accepted, owner::set))) {
                    path = file.path();
                    assertThat(java.nio.file.Files.readAllBytes(path)).isEqualTo(bytes);
                    assertThat(file).isSameAs(owner.get());
                    assertThat(accepted.requestedBytes()).isEqualTo(bytes.length);
                    assertThat(gets.get()).isEqualTo(1);
                }
                assertThat(path).doesNotExist();
                try (var files = java.nio.file.Files.list(temporaryDirectory.resolve("native"))) {
                    assertThat(files.toList()).isEmpty();
                }
            }
        }
    }

    @Test
    void admittedRangeRejectsAnAppendedManagedObjectEvenWithForgedOriginalMetadata() throws Exception {
        String bucket = bucketName();
        String managedBucket = bucketName();
        byte[] bytes = "frozen-task-bytes".getBytes(StandardCharsets.UTF_8);
        try (S3Client objects = client()) {
            createVersionedBucket(objects, bucket);
            objects.createBucket(request -> request.bucket(managedBucket));
            objects.putObject(request -> request.bucket(bucket).key("data/part.parquet"), RequestBody.fromBytes(bytes));
            var stager = new S3VersionedCopyStager(objects, managed(objects, managedBucket),
                    temporaryDirectory.resolve("private-inputs"), 1024);
            String url = "s3://" + bucket + "/data/part.parquet";
            var proof = stager.stage(profile(bucket), url, "tenant-one");
            objects.putObject(PutObjectRequest.builder().bucket(managedBucket).key(managedKey(proof))
                    .serverSideEncryption(ServerSideEncryption.AES256)
                    .metadata(Map.of("sha256", proof.contentSha256(), "content-length", Long.toString(bytes.length)))
                    .build(), RequestBody.fromBytes("frozen-task-bytes-appended".getBytes(StandardCharsets.UTF_8)));
            var budget = new CopyTaskReadBudget(bytes.length);
            assertThatThrownBy(() -> stager.materialize(profile(bucket), url, "tenant-one", proof,
                    new CopyMaterialization(budget, ignored -> {})))
                    .isInstanceOf(IOException.class).hasMessageContaining("complete byte range");
            assertThat(budget.requestedBytes()).isEqualTo(bytes.length);
            try (var files = java.nio.file.Files.list(temporaryDirectory.resolve("private-inputs"))) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }

    private static String managedKey(S3VersionedCopyStager.StagedSource staged) {
        return "copy-managed/tenants/" + staged.tenantScope() + "/sha256/"
                + staged.contentSha256().substring(0, 2) + "/" + staged.contentSha256();
    }

    private static void createVersionedBucket(S3Client objects, String bucket) {
        objects.createBucket(request -> request.bucket(bucket));
        objects.putBucketVersioning(request -> request.bucket(bucket)
                .versioningConfiguration(VersioningConfiguration.builder()
                        .status(BucketVersioningStatus.ENABLED).build()));
    }

    private static S3BlobStorage managed(S3Client objects, String bucket) {
        return new S3BlobStorage(objects, new S3BlobStorage.Settings(
                bucket, "us-east-1", null, "copy-managed",
                new S3BlobStorage.Encryption(ServerSideEncryption.AES256, ""), "", true));
    }

    private static StorageProfile profile(String bucket) {
        return new StorageProfile("versioned-source", StorageType.S3, bucket,
                "data/", "", "us-east-1", Map.of(), Map.of());
    }

    private static S3Client client() {
        return client(null);
    }

    private static S3Client client(ExecutionInterceptor interceptor) {
        var builder = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000)))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        if (interceptor != null) {
            builder.overrideConfiguration(ClientOverrideConfiguration.builder()
                    .addExecutionInterceptor(interceptor).build());
        }
        return builder.build();
    }

    private static String bucketName() {
        return "copy-version-" + UUID.randomUUID();
    }
}
