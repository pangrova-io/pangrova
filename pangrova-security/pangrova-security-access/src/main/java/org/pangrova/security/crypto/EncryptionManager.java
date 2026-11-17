package org.pangrova.security.crypto;

import org.pangrova.security.api.crypto.EncryptedData;
import org.pangrova.security.api.crypto.KeyManager;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Objects;

/**
 * Envelope encryption using AES-256-GCM: generates data key via KeyManager,
 * encrypts payload, and wraps the data key with the master key.
 */
public class EncryptionManager {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int IV_LENGTH = 12;
    private static final int FORMAT_VERSION = 1;

    private final KeyManager keyManager;
    private final SecureRandom random = new SecureRandom();

    /**
     * Creates an encryption manager.
     *
     * @param keyManager borrowed data-key provider, closed by its creator
     */
    public EncryptionManager(KeyManager keyManager) {
        this.keyManager = Objects.requireNonNull(keyManager, "keyManager");
    }

    /**
     * Encrypts a payload and wraps its generated data key.
     *
     * @param plaintext payload to encrypt
     * @param masterKeyId master key identifier
     * @param aad additional authenticated data
     * @return encrypted payload and key metadata
     */
    public EncryptedData encrypt(byte[] plaintext, String masterKeyId, byte[] aad) {
        SecretKey dataKey = keyManager.getDataEncryptionKey(masterKeyId);
        byte[] iv = generateIv();
        byte[] ciphertext = doEncrypt(plaintext, dataKey, iv, aad);
        byte[] encryptedKey = keyManager.encryptDataKey(dataKey, masterKeyId);
        return new EncryptedData(ciphertext, iv, encryptedKey, masterKeyId, FORMAT_VERSION);
    }

    /** Decrypts an envelope of the supported format version using its associated data. */
    public byte[] decrypt(EncryptedData data, byte[] aad) {
        if (data.formatVersion() != FORMAT_VERSION) {
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED,
                    "Unsupported envelope format version: " + data.formatVersion());
        }
        SecretKey dataKey = keyManager.decryptDataKey(data.encryptedKey(), data.masterKeyId());
        return doDecrypt(data.ciphertext(), dataKey, data.iv(), aad);
    }

    /**
     * Builds AAD (Additional Authenticated Data) from tenant and table identifiers.
     *
     * @param tenantId the tenant identifier
     * @param tableId the table identifier
     * @return AAD as byte array
     */
    public static byte[] buildAad(String tenantId, String tableId) {
        if (tenantId == null || tableId == null) {
            throw new IllegalArgumentException("tenantId and tableId must not be null");
        }
        byte[] tenant = tenantId.getBytes(StandardCharsets.UTF_8);
        byte[] table = tableId.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(Integer.BYTES + tenant.length + table.length)
                .putInt(tenant.length).put(tenant).put(table).array();
    }

    private byte[] generateIv() {
        byte[] iv = new byte[IV_LENGTH];
        random.nextBytes(iv);
        return iv;
    }

    private byte[] doEncrypt(byte[] plaintext, SecretKey key, byte[] iv, byte[] aad) {
        return doCipher(Cipher.ENCRYPT_MODE, plaintext, key, iv, aad);
    }

    private byte[] doDecrypt(byte[] ciphertext, SecretKey key, byte[] iv, byte[] aad) {
        return doCipher(Cipher.DECRYPT_MODE, ciphertext, key, iv, aad);
    }

    private byte[] doCipher(int mode, byte[] input, SecretKey key, byte[] iv, byte[] aad) {
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(mode, key, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
            if (aad != null && aad.length > 0) {
                cipher.updateAAD(aad);
            }
            return cipher.doFinal(input);
        } catch (Exception e) {
            String op = mode == Cipher.ENCRYPT_MODE ? "Encryption" : "Decryption";
            throw new PangrovaException(PangrovaErrorCode.ENCRYPTION_FAILED, op + " failed", e);
        }
    }
}
