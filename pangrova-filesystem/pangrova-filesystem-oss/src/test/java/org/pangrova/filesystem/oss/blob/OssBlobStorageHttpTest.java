package org.pangrova.filesystem.oss.blob;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.filesystem.api.blob.BlobObjectId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** Tests the actual official SDK over verified HTTPS; does not claim a real OSS service deployment. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class OssBlobStorageHttpTest {
    private static final OssBlobStorage.Encryption AES = new OssBlobStorage.Encryption("AES256", "");
    @TempDir static Path directory;
    private OssHttpFixture http;

    @BeforeAll void start() throws Exception { http = new OssHttpFixture(directory); }
    @AfterAll void stop() throws Exception { if (http != null) http.close(); }
    @BeforeEach void reset() { http.reset(); }

    @Test
    void signedConditionalPublicationUsesPrivateBytesAndChecksTheActualExistingWinner() throws Exception {
        byte[] bytes = {1, 2, 3, 4};
        BlobObjectId object = object(bytes);
        Path draft = Files.write(directory.resolve("draft"), bytes);
        http.onVersioning = () -> {
            try { Files.write(draft, new byte[]{9, 9, 9, 9}); }
            catch (IOException failure) { throw new UncheckedIOException(failure); }
        };
        var storage = storage(AES);
        try (storage) {
            storage.publish(object, draft);
            var stored = http.objects.get(key(object));
            assertThat(stored.bytes()).containsExactly(bytes);
            assertThat(stored.encryption()).isEqualTo("AES256");
            assertThat(http.lastPut.authorization()).startsWith("OSS4-HMAC-SHA256 Credential=test-id/");
            assertThat(http.lastPut.forbidOverwrite()).isEqualTo("true");
            assertThat(http.lastPut.length()).isEqualTo("4");
            assertThat(http.objects).hasSize(1);
            http.onVersioning = null;
            Files.write(draft, bytes);
            storage.publish(object, draft);
            assertThat(http.objects.get(key(object))).isSameAs(stored);
            assertThat(http.gets).hasValue(1);
            try (var input = storage.open(object)) { assertThat(input.readAllBytes()).containsExactly(bytes); }
            assertThat(http.puts).hasValue(2);
        }
        assertThatThrownBy(() -> storage.open(object)).isInstanceOf(IOException.class);
        try (var independent = storage(AES); var input = independent.open(object)) {
            assertThat(input.readAllBytes()).containsExactly(bytes);
        }
    }

    @Test
    void emptyContentHasOneImmutableIdentityAndItsStreamObeysEofAndClose() throws Exception {
        BlobObjectId empty = object(new byte[0]);
        try (var storage = storage(AES)) {
            Path draft = Files.write(directory.resolve("empty"), new byte[0]);
            storage.publish(empty, draft);
            storage.publish(empty, draft);
            try (var input = storage.open(empty)) {
                assertThat(input.read(new byte[0])).isZero();
                assertThat(input.read()).isEqualTo(-1);
                assertThat(input.readAllBytes()).isEmpty();
            }
            assertThat(http.objects).hasSize(1);
        }
    }

    @Test
    void sdkUploadChecksumFailureRemainsAFailureAndANewAttemptVerifiesThePersistedWinner() throws Exception {
        byte[] bytes = {1, 2, 3};
        BlobObjectId object = object(bytes);
        Path draft = Files.write(directory.resolve("crc"), bytes);
        http.badUploadCrc = true;
        try (var storage = storage(AES)) {
            assertThatThrownBy(() -> storage.publish(object, draft)).isInstanceOf(IOException.class);
            assertThat(http.puts).hasValue(1);
            assertThat(http.objects.get(key(object)).bytes()).containsExactly(bytes);
            http.badUploadCrc = false;
            storage.publish(object, draft);
            assertThat(http.puts).hasValue(2);
            assertThat(http.gets).hasValue(1);
            assertThat(http.objects).hasSize(1);
        }
    }

    @Test
    void invalidCallerDraftAndOversizedIdentityIssueNoRemoteRequest() throws Exception {
        BlobObjectId expected = object(new byte[]{1, 2, 3});
        Path wrong = Files.write(directory.resolve("wrong"), new byte[]{1, 2, 4});
        try (var storage = storage(AES)) {
            assertThatThrownBy(() -> storage.publish(expected, wrong)).isInstanceOf(IOException.class);
            Files.write(wrong, new byte[]{1, 2});
            assertThatThrownBy(() -> storage.publish(expected, wrong)).isInstanceOf(IOException.class);
            Files.write(wrong, new byte[]{1, 2, 3, 4});
            assertThatThrownBy(() -> storage.publish(expected, wrong)).isInstanceOf(IOException.class);
            BlobObjectId huge = new BlobObjectId(expected.tenantScope(), expected.checksum(), Long.MAX_VALUE);
            assertThatThrownBy(() -> storage.publish(huge, wrong)).isInstanceOf(IOException.class);
        }
        assertThat(http.versions).hasValue(0);
        assertThat(http.puts).hasValue(0);
        assertThat(http.objects).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"Enabled", "Suspended", "Unknown"})
    void versionedSuspendedAndUnknownBucketStatesFailBeforePut(String state) throws Exception {
        http.versioning = state;
        byte[] bytes = {1, 2, 3};
        try (var storage = storage(AES)) {
            assertThatThrownBy(() -> storage.publish(object(bytes), Files.write(directory.resolve("versioned"), bytes)))
                    .isInstanceOf(IOException.class).hasMessageContaining("never-versioned");
        }
        assertThat(http.versions).hasValue(1);
        assertThat(http.puts).hasValue(0);
        assertThat(http.objects).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"500,InternalError", "403,AccessDenied", "409,OperationAborted"})
    void failedPutIsNotRetriedOrTreatedAsAnExistingWinner(int status, String code) throws Exception {
        http.errorStatus = status;
        http.errorCode = code;
        byte[] bytes = {1, 2, 3};
        try (var storage = storage(AES)) {
            assertThatThrownBy(() -> storage.publish(object(bytes), Files.write(directory.resolve("failed"), bytes)))
                    .isInstanceOf(IOException.class);
        }
        assertThat(http.puts).hasValue(1);
        assertThat(http.objects).isEmpty();
        assertThat(http.gets).hasValue(0);
    }

    @Test
    void aVersionReturnedAfterThePreflightFailsPublication() throws Exception {
        http.putVersion = "null"; // Suspended buckets return a literal null version id.
        byte[] bytes = {1, 2, 3};
        try (var storage = storage(AES)) {
            assertThatThrownBy(() -> storage.publish(object(bytes), Files.write(directory.resolve("raced"), bytes)))
                    .isInstanceOf(IOException.class).hasMessageContaining("version id");
        }
    }

    @Test
    void matchingMetadataCannotHideCorruptedExistingBytes() throws Exception {
        byte[] expected = {1, 2, 3};
        BlobObjectId object = object(expected);
        http.objects.put(key(object), stored(new byte[]{9, 9, 9}, object, "AES256", null, null));
        try (var storage = storage(AES)) {
            assertThatThrownBy(() -> storage.publish(object, Files.write(directory.resolve("collision"), expected)))
                    .isInstanceOf(IOException.class).hasMessageContaining("SHA-256");
            try (var input = storage.open(object)) {
                assertThatThrownBy(input::readAllBytes).isInstanceOf(IOException.class).hasMessageContaining("SHA-256");
            }
        }
        assertThat(http.objects.get(key(object)).bytes()).containsExactly(9, 9, 9);
    }

    @Test
    void readsRejectWrongLengthChecksumEncryptionKeyAndObjectVersion() throws Exception {
        byte[] bytes = {1, 2, 3};
        BlobObjectId object = object(bytes);
        try (var storage = storage(new OssBlobStorage.Encryption("KMS", "exact-key-id"))) {
            for (OssHttpFixture.Stored bad : java.util.List.of(
                    stored(bytes, object, "AES256", null, null),
                    stored(bytes, object, "KMS", "other-key", null),
                    stored(bytes, object, "KMS", "exact-key-id", "version-1"),
                    new OssHttpFixture.Stored(bytes, bytes.length + 1, Map.of("sha256", object.checksum(), "size", "3"),
                            "KMS", "exact-key-id", null),
                    new OssHttpFixture.Stored(bytes, bytes.length, Map.of("sha256", "0".repeat(64), "size", "3"),
                            "KMS", "exact-key-id", null),
                    new OssHttpFixture.Stored(bytes, bytes.length, Map.of("sha256", object.checksum(), "size", "4"),
                            "KMS", "exact-key-id", null))) {
                http.objects.put(key(object), bad);
                assertThatThrownBy(() -> storage.open(object)).isInstanceOf(IOException.class);
            }
            http.objects.put(key(object), stored(bytes, object, "KMS", "exact-key-id", null));
            try (var input = storage.open(object)) { assertThat(input.readAllBytes()).containsExactly(bytes); }
        }
    }

    @Test
    void metadataRejectionAndPartialCloseDoNotWaitForAnUnreadRemoteBody() throws Exception {
        try (var storage = storage(AES)) {
            for (int size : java.util.List.of(1, 64 * 1024)) {
                byte[] bytes = new byte[size];
                BlobObjectId object = object(bytes);
                for (String encryption : java.util.List.of("KMS", "AES256")) {
                    http.objects.put(key(object), stored(bytes, object, encryption, null, null));
                    try (var block = new OssHttpFixture.BodyBlock()) {
                        http.bodyBlock = block;
                        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                            if ("KMS".equals(encryption)) {
                                assertThatThrownBy(() -> storage.open(object)).isInstanceOf(IOException.class);
                            } else {
                                try (var input = storage.open(object)) {
                                    assertThat(input).isNotNull(); // Close before any body bytes are released.
                                }
                            }
                            assertThat(block.entered.await(1, TimeUnit.SECONDS)).isTrue();
                        });
                    }
                }
                http.bodyBlock = null;
                try (var input = storage.open(object)) { assertThat(input.readAllBytes()).containsExactly(bytes); }
            }
        }
    }

    @Test
    void aControlResponseReadTimeoutCancelsBeforeClosingTheUnreadEntity() throws Exception {
        var base = http.settings(AES);
        var settings = new OssBlobStorage.Settings(base.bucket(), base.region(), base.endpoint(), base.prefix(),
                base.encryption(), base.pathStyleAccess(), base.connectTimeout(), Duration.ofSeconds(2));
        byte[] bytes = {1, 2, 3};
        Path draft = Files.write(directory.resolve("control-timeout"), bytes);
        try (var storage = new OssBlobStorage(settings, new StaticCredentialsProvider("test-id", "test-secret"),
                http.trustContext()); var block = new OssHttpFixture.BodyBlock()) {
            http.controlBlock = block;
            BlobObjectId object = object(bytes);
            assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
                    assertThatThrownBy(() -> storage.publish(object, draft)).isInstanceOf(IOException.class));
            assertThat(block.entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(http.puts).hasValue(0);
        }
    }

    @Test
    void defaultTrustRejectsAnUntrustedEndpointAndMissingCredentialsFailBeforeIo() throws Exception {
        byte[] bytes = {1, 2, 3};
        try (var storage = new OssBlobStorage(http.settings(AES),
                new StaticCredentialsProvider("test-id", "test-secret"), null)) {
            assertThatThrownBy(() -> storage.publish(object(bytes), Files.write(directory.resolve("untrusted"), bytes)))
                    .isInstanceOf(IOException.class);
        }
        assertThatThrownBy(() -> new OssBlobStorage(http.settings(AES),
                new StaticCredentialsProvider("", ""), http.trustContext()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("credentials");
        assertThat(http.versions).hasValue(0);
        assertThat(http.puts).hasValue(0);
    }

    private OssBlobStorage storage(OssBlobStorage.Encryption encryption) throws Exception {
        return new OssBlobStorage(http.settings(encryption), new StaticCredentialsProvider("test-id", "test-secret"),
                http.trustContext());
    }

    private static BlobObjectId object(byte[] bytes) throws Exception {
        return new BlobObjectId("a".repeat(64),
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), bytes.length);
    }

    private static String key(BlobObjectId object) {
        return "/test-bucket/files/" + object.tenantScope() + "/" + object.checksum() + ".blob";
    }

    private static OssHttpFixture.Stored stored(byte[] bytes, BlobObjectId object,
                                                String encryption, String key, String version) {
        return new OssHttpFixture.Stored(bytes, bytes.length,
                Map.of("sha256", object.checksum(), "size", Long.toString(object.size())), encryption, key, version);
    }
}
