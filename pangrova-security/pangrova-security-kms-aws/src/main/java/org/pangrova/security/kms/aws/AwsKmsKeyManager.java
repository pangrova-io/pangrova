package org.pangrova.security.kms.aws;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.security.api.crypto.KeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.awssdk.services.kms.model.UpdateKeyDescriptionRequest;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AWS KMS key management using envelope encryption.
 *
 * <p>Generates data encryption keys (DEK) via KMS GenerateDataKey, encrypts data locally
 * with the plaintext DEK, and stores the KMS-encrypted DEK alongside the ciphertext.
 * Decryption calls KMS Decrypt to recover the plaintext DEK, then decrypts locally.</p>
 */
public class AwsKmsKeyManager implements KeyManager {

    private static final Logger LOG = LoggerFactory.getLogger(AwsKmsKeyManager.class);
    private static final String AES_ALGORITHM = "AES";
    private static final int MINIMUM_MASKED_ARN_LENGTH = 12;

    private final KmsClient kmsClient;
    private final String defaultKeyArn;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a key manager backed by AWS KMS.
     *
     * @param configuration KMS client configuration
     */
    public AwsKmsKeyManager(KmsConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration must not be null");
        this.defaultKeyArn = configuration.keyArn();
        this.kmsClient = buildClient(configuration);
        LOG.info("Initialized AWS KMS key manager, region={}, keyArn={}",
                configuration.region(), maskArn(defaultKeyArn));
    }

    /**
     * Takes ownership of a pre-built KMS client.
     */
    AwsKmsKeyManager(KmsClient kmsClient, String defaultKeyArn) {
        this.kmsClient = Objects.requireNonNull(kmsClient, "kmsClient must not be null");
        this.defaultKeyArn = Objects.requireNonNull(defaultKeyArn, "defaultKeyArn must not be null");
    }

    @Override
    public SecretKey getDataEncryptionKey(String keyId) {
        Objects.requireNonNull(keyId, "keyId must not be null");
        String resolvedKeyId = keyId.isBlank() ? defaultKeyArn : keyId;
        GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
                .keyId(resolvedKeyId)
                .keySpec(DataKeySpec.AES_256)
                .build();
        try {
            GenerateDataKeyResponse response = kmsClient.generateDataKey(request);
            byte[] plaintextKey = response.plaintext().asByteArray();
            return new SecretKeySpec(plaintextKey, AES_ALGORITHM);
        } catch (KmsException e) {
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED,
                    "Failed to generate data key via KMS, keyId=" + maskArn(resolvedKeyId), e);
        }
    }

    @Override
    public String createKey(String alias) {
        Objects.requireNonNull(alias, "alias must not be null");
        try {
            CreateKeyRequest createRequest = CreateKeyRequest.builder()
                    .description("Pangrova encryption key: " + alias)
                    .build();
            CreateKeyResponse response = kmsClient.createKey(createRequest);
            String keyId = response.keyMetadata().keyId();

            CreateAliasRequest aliasRequest = CreateAliasRequest.builder()
                    .aliasName("alias/" + alias)
                    .targetKeyId(keyId)
                    .build();
            kmsClient.createAlias(aliasRequest);

            LOG.info("Created KMS key, alias={}, keyId={}", alias, keyId);
            return response.keyMetadata().arn();
        } catch (KmsException e) {
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED,
                    "Failed to create KMS key, alias=" + alias, e);
        }
    }

    @Override
    public String rotateKey(String keyId) {
        Objects.requireNonNull(keyId, "keyId must not be null");
        try {
            kmsClient.enableKeyRotation(r -> r.keyId(keyId));
            kmsClient.updateKeyDescription(UpdateKeyDescriptionRequest.builder()
                    .keyId(keyId)
                    .description("Pangrova key (rotation enabled)")
                    .build());
            LOG.info("Enabled automatic rotation for KMS key, keyId={}", maskArn(keyId));
            return keyId;
        } catch (KmsException e) {
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED,
                    "Failed to enable key rotation, keyId=" + maskArn(keyId), e);
        }
    }

    @Override
    public byte[] encryptDataKey(SecretKey dataKey, String masterKeyId) {
        Objects.requireNonNull(dataKey, "dataKey must not be null");
        Objects.requireNonNull(masterKeyId, "masterKeyId must not be null");
        String resolvedKeyId = masterKeyId.isBlank() ? defaultKeyArn : masterKeyId;
        EncryptRequest request = EncryptRequest.builder()
                .keyId(resolvedKeyId)
                .plaintext(SdkBytes.fromByteArray(dataKey.getEncoded()))
                .build();
        try {
            EncryptResponse response = kmsClient.encrypt(request);
            return response.ciphertextBlob().asByteArray();
        } catch (KmsException e) {
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED,
                    "Failed to encrypt data key via KMS, masterKeyId=" + maskArn(resolvedKeyId), e);
        }
    }

    @Override
    public SecretKey decryptDataKey(byte[] encryptedKey, String masterKeyId) {
        Objects.requireNonNull(encryptedKey, "encryptedKey must not be null");
        Objects.requireNonNull(masterKeyId, "masterKeyId must not be null");
        String resolvedKeyId = masterKeyId.isBlank() ? defaultKeyArn : masterKeyId;
        DecryptRequest request = DecryptRequest.builder()
                .keyId(resolvedKeyId)
                .ciphertextBlob(SdkBytes.fromByteArray(encryptedKey))
                .build();
        try {
            DecryptResponse response = kmsClient.decrypt(request);
            byte[] plaintextKey = response.plaintext().asByteArray();
            return new SecretKeySpec(plaintextKey, AES_ALGORITHM);
        } catch (KmsException e) {
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED,
                    "Failed to decrypt data key via KMS, masterKeyId=" + maskArn(resolvedKeyId), e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            kmsClient.close();
        }
    }

    private static KmsClient buildClient(KmsConfiguration configuration) {
        KmsClientBuilder builder = KmsClient.builder()
                .region(Region.of(configuration.region()));
        if (configuration.endpointOverride() != null && !configuration.endpointOverride().isBlank()) {
            builder.endpointOverride(URI.create(configuration.endpointOverride()));
        }
        return builder.build();
    }

    private static String maskArn(String arn) {
        if (arn == null || arn.length() <= MINIMUM_MASKED_ARN_LENGTH) {
            return "***";
        }
        return arn.substring(0, 8) + "..." + arn.substring(arn.length() - 4);
    }
}
