package org.pangrova.security.adapter.kms;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KMS;
import org.pangrova.security.core.crypto.EncryptedData;
import org.pangrova.security.core.crypto.EncryptionManager;

/**
 * Integration test for {@link AwsKmsKeyManager} using LocalStack KMS.
 * Verifies envelope encryption round-trip: generate DEK, encrypt DEK, decrypt DEK.
 */
@Tag("integration")
@Testcontainers
class AwsKmsKeyManagerIntegrationTest {

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.4"))
            .withServices(KMS);

    private static KmsClient kmsClient;
    private static String masterKeyArn;
    private static AwsKmsKeyManager keyManager;

    @BeforeAll
    static void setUp() {
        kmsClient = KmsClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(KMS))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(),
                                LOCALSTACK.getSecretKey())))
                .build();

        CreateKeyResponse createKeyResponse = kmsClient.createKey();
        masterKeyArn = createKeyResponse.keyMetadata().arn();

        keyManager = new AwsKmsKeyManager(kmsClient, masterKeyArn);
    }

    @Test
    void generateDataKeyReturnsValidAes256Key() {
        SecretKey dataKey = keyManager.getDataEncryptionKey(masterKeyArn);

        assertThat(dataKey).isNotNull();
        assertThat(dataKey.getAlgorithm()).isEqualTo("AES");
        assertThat(dataKey.getEncoded()).hasSize(32);
    }

    @Test
    void encryptThenDecryptDataKeyRoundTrip() {
        SecretKey originalKey = keyManager.getDataEncryptionKey(masterKeyArn);

        byte[] encryptedKey = keyManager.encryptDataKey(originalKey, masterKeyArn);
        assertThat(encryptedKey).isNotNull();
        assertThat(encryptedKey).isNotEmpty();
        assertThat(encryptedKey).isNotEqualTo(originalKey.getEncoded());

        SecretKey decryptedKey = keyManager.decryptDataKey(encryptedKey, masterKeyArn);
        assertThat(decryptedKey.getEncoded()).isEqualTo(originalKey.getEncoded());
        assertThat(decryptedKey.getAlgorithm()).isEqualTo("AES");
    }

    @Test
    void envelopeEncryptionEndToEnd() {
        EncryptionManager encryptionManager = new EncryptionManager(keyManager);
        byte[] plaintext = "sensitive data for envelope encryption test".getBytes();

        byte[] aad = "test-context".getBytes();
        EncryptedData encrypted = encryptionManager.encrypt(plaintext, masterKeyArn, aad);

        assertThat(encrypted.ciphertext()).isNotEqualTo(plaintext);
        assertThat(encrypted.iv()).hasSize(12);
        assertThat(encrypted.encryptedKey()).isNotEmpty();
        assertThat(encrypted.masterKeyId()).isEqualTo(masterKeyArn);

        byte[] decrypted = encryptionManager.decrypt(encrypted, aad);
        assertThat(decrypted).isEqualTo(plaintext);
    }

    @Test
    void createKeyReturnsValidArn() {
        String arn = keyManager.createKey("test-envelope-key");

        assertThat(arn).isNotNull();
        assertThat(arn).contains("arn:");
    }

    @Test
    void rotateKeyEnablesRotation() {
        CreateKeyResponse newKey = kmsClient.createKey();
        String keyId = newKey.keyMetadata().keyId();

        String result = keyManager.rotateKey(keyId);

        assertThat(result).isEqualTo(keyId);
    }

    @Test
    void defaultKeyArnUsedWhenKeyIdIsBlank() {
        SecretKey dataKey = keyManager.getDataEncryptionKey("");

        assertThat(dataKey).isNotNull();
        assertThat(dataKey.getAlgorithm()).isEqualTo("AES");
        assertThat(dataKey.getEncoded()).hasSize(32);
    }

    @Test
    void multipleDataKeysAreUnique() {
        SecretKey key1 = keyManager.getDataEncryptionKey(masterKeyArn);
        SecretKey key2 = keyManager.getDataEncryptionKey(masterKeyArn);

        assertThat(key1.getEncoded()).isNotEqualTo(key2.getEncoded());
    }

    @Test
    void encryptedDataKeyDiffersFromPlaintext() {
        GenerateDataKeyResponse response = kmsClient.generateDataKey(
                GenerateDataKeyRequest.builder()
                        .keyId(masterKeyArn)
                        .keySpec(DataKeySpec.AES_256)
                        .build());

        byte[] plaintextKeyBytes = response.plaintext().asByteArray();
        SecretKey plaintextKey = new SecretKeySpec(plaintextKeyBytes, "AES");

        byte[] encryptedKey = keyManager.encryptDataKey(plaintextKey, masterKeyArn);

        assertThat(encryptedKey).isNotEqualTo(plaintextKeyBytes);
    }
}
