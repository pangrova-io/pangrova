package org.pangrova.security.api.crypto;

import javax.crypto.SecretKey;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Key-management provider used by envelope encryption. Its creator owns its lifecycle
 * and closes it after active operations have finished.
 */
@Experimental
public interface KeyManager extends AutoCloseable {

    SecretKey getDataEncryptionKey(String keyId);

    String createKey(String alias);

    String rotateKey(String keyId);

    byte[] encryptDataKey(SecretKey dataKey, String masterKeyId);

    SecretKey decryptDataKey(byte[] encryptedKey, String masterKeyId);

    /** Releases resources owned by this provider. Repeated calls are safe. */
    @Override
    void close();
}
