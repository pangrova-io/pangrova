package org.pangrova.worker.ai.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3BlobStorageTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void conditionalContentAddressedPublicationIsReadableAcrossWorkers() throws Exception {
        FakeS3Client client = new FakeS3Client();
        S3BlobStorage first = new S3BlobStorage(client, settings());
        S3BlobStorage second = new S3BlobStorage(client, settings());
        Path staged = Files.write(temporaryDirectory.resolve("content.part"),
                "distributed-content".getBytes(StandardCharsets.UTF_8));
        String checksum = sha256(staged);
        long size = Files.size(staged);

        first.publish("a".repeat(64), checksum, size, staged);
        second.publish("a".repeat(64), checksum, size, staged);

        try (var input = second.open("a".repeat(64), checksum, size)) {
            assertThat(input.readAllBytes())
                    .isEqualTo("distributed-content".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(client.objects).hasSize(1);
        assertThat(client.putRequests).hasSize(2);
        PutObjectRequest request = client.putRequests.get(0);
        assertThat(request.key()).startsWith("pangrova/ai-files/tenants/")
                .endsWith(checksum);
        assertThat(request.serverSideEncryption()).isEqualTo(ServerSideEncryption.AES256);
        assertThat(request.metadata()).containsEntry("sha256", checksum);
        assertThat(request.overrideConfiguration().orElseThrow().headers().entrySet())
                .anySatisfy(entry -> {
                    assertThat(entry.getKey()).isEqualToIgnoringCase("If-None-Match");
                    assertThat(entry.getValue()).containsExactly("*");
                });
    }

    @Test
    void publicationFailsClosedWhenBackendDoesNotHonorEncryption() throws Exception {
        FakeS3Client client = new FakeS3Client();
        client.reportedEncryption = null;
        S3BlobStorage storage = new S3BlobStorage(client, settings());
        Path staged = Files.write(temporaryDirectory.resolve("content.part"), new byte[] {1});

        assertThatThrownBy(() -> storage.publish(
                "a".repeat(64), sha256(staged), 1, staged))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("encryption policy");
    }

    @Test
    void publicationFailsClosedWhenBackendUsesDifferentKmsKey() throws Exception {
        FakeS3Client client = new FakeS3Client();
        client.reportedEncryption = ServerSideEncryption.AWS_KMS;
        client.reportedKmsKey = "arn:aws:kms:us-east-1:123456789012:key/other";
        S3BlobStorage.Settings kms = new S3BlobStorage.Settings(
                "governed-files", "us-east-1", null, "pangrova/ai-files",
                new S3BlobStorage.Encryption(ServerSideEncryption.AWS_KMS,
                        "arn:aws:kms:us-east-1:123456789012:key/expected"),
                "123456789012", false);
        S3BlobStorage storage = new S3BlobStorage(client, kms);
        Path staged = Files.write(temporaryDirectory.resolve("kms.part"), new byte[] {1});

        assertThatThrownBy(() -> storage.publish(
                "a".repeat(64), sha256(staged), 1, staged))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("configured KMS key");
    }

    @Test
    void settingsRejectInsecureEndpointPathAndIncompleteKmsPolicy() {
        assertThatThrownBy(() -> new S3BlobStorage.Settings(
                "governed-files", "us-east-1", URI.create("http://store.example"),
                "prefix", new S3BlobStorage.Encryption(
                        ServerSideEncryption.AES256, ""), "", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS origin");
        assertThatThrownBy(() -> new S3BlobStorage.Settings(
                "governed-files", "us-east-1", URI.create("https://store.example/api"),
                "prefix", new S3BlobStorage.Encryption(
                        ServerSideEncryption.AES256, ""), "", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS origin");
        assertThatThrownBy(() -> new S3BlobStorage.Encryption(
                ServerSideEncryption.AWS_KMS, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key id");
    }

    @Test
    void backendIdentityBindsPhysicalOriginEvenWhenLogicalRootMatches() {
        var encryption = new S3BlobStorage.Encryption(ServerSideEncryption.AES256, "");
        var firstSettings = new S3BlobStorage.Settings("governed-files", "us-east-1",
                URI.create("https://STORE-A.example:443/"), "pangrova/ai-files",
                encryption, "", true);
        var sameOrigin = new S3BlobStorage.Settings("governed-files", "us-east-1",
                URI.create("https://store-a.example"), "pangrova/ai-files",
                encryption, "", true);
        var otherOrigin = new S3BlobStorage.Settings("governed-files", "us-east-1",
                URI.create("https://store-b.example"), "pangrova/ai-files",
                encryption, "", true);
        try (var first = new S3BlobStorage(firstSettings);
                var canonical = new S3BlobStorage(sameOrigin);
                var other = new S3BlobStorage(otherOrigin)) {
            assertThat(first.rootUri()).isEqualTo(other.rootUri());
            assertThat(first.backendIdentitySha256()).isEqualTo(canonical.backendIdentitySha256());
            assertThat(first.backendIdentitySha256()).isNotEqualTo(other.backendIdentitySha256());
        }
    }

    @Test
    void copyBackendIdentityRejectsImplicitSdkEndpointOverrides() {
        String previous = System.getProperty("aws.endpointUrl.s3");
        System.setProperty("aws.endpointUrl.s3", "https://unexpected-store.example");
        try (var storage = new S3BlobStorage(settings())) {
            assertThatThrownBy(storage::backendIdentitySha256)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("explicit HTTPS endpoint override");
        } finally {
            if (previous == null) {
                System.clearProperty("aws.endpointUrl.s3");
            } else {
                System.setProperty("aws.endpointUrl.s3", previous);
            }
        }
    }

    private static S3BlobStorage.Settings settings() {
        return new S3BlobStorage.Settings(
                "governed-files", "us-east-1", null, "pangrova/ai-files",
                new S3BlobStorage.Encryption(ServerSideEncryption.AES256, ""),
                "123456789012", false);
    }

    private static String sha256(Path path) throws Exception {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    private static final class FakeS3Client implements S3Client {
        private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();
        private final List<PutObjectRequest> putRequests =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private ServerSideEncryption reportedEncryption = ServerSideEncryption.AES256;
        private String reportedKmsKey;

        @Override
        public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
            putRequests.add(request);
            StoredObject object = new StoredObject(read(body), request.metadata(),
                    request.serverSideEncryption(), request.ssekmsKeyId());
            if (objects.putIfAbsent(request.key(), object) != null) {
                throw S3Exception.builder().statusCode(412)
                        .message("precondition failed").build();
            }
            return PutObjectResponse.builder().build();
        }

        @Override
        public HeadObjectResponse headObject(HeadObjectRequest request) {
            StoredObject object = requireObject(request.key());
            return HeadObjectResponse.builder()
                    .contentLength((long) object.content().length)
                    .metadata(object.metadata())
                    .serverSideEncryption(reportedEncryption)
                    .ssekmsKeyId(reportedKmsKey == null
                            ? object.kmsKeyId() : reportedKmsKey)
                    .build();
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
            StoredObject object = requireObject(request.key());
            GetObjectResponse response = GetObjectResponse.builder()
                    .contentLength((long) object.content().length)
                    .metadata(object.metadata())
                    .serverSideEncryption(reportedEncryption)
                    .ssekmsKeyId(reportedKmsKey == null
                            ? object.kmsKeyId() : reportedKmsKey)
                    .build();
            return new ResponseInputStream<>(
                    response, new ByteArrayInputStream(object.content()));
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {}

        private StoredObject requireObject(String key) {
            StoredObject object = objects.get(key);
            if (object == null) {
                throw S3Exception.builder().statusCode(404).message("missing").build();
            }
            return object;
        }

        private static byte[] read(RequestBody body) {
            try (var input = body.contentStreamProvider().newStream()) {
                return input.readAllBytes();
            } catch (IOException failure) {
                throw new IllegalStateException("Failed to read request body", failure);
            }
        }
    }

    private record StoredObject(
            byte[] content,
            Map<String, String> metadata,
            ServerSideEncryption encryption,
            String kmsKeyId) {}
}
