package org.pangrova.security.spi.crypto;

import javax.crypto.SecretKey;
import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for key management operations (data encryption keys, master key rotation, envelope encryption).
 */
@Experimental
public interface KeyManager {

    SecretKey getDataEncryptionKey(String keyId);

    String createKey(String alias);

    String rotateKey(String keyId);

    byte[] encryptDataKey(SecretKey dataKey, String masterKeyId);

    SecretKey decryptDataKey(byte[] encryptedKey, String masterKeyId);

    /**
     * Returns the current version of the specified master key.
     */
    int getKeyVersion(String keyId);
}
