package org.pangrova.security.api.crypto;

/**
 * Envelope-encrypted payload: ciphertext, IV, encrypted data key, master key reference, and envelope format version.
 */
public record EncryptedData(byte[] ciphertext, byte[] iv, byte[] encryptedKey, String masterKeyId, int formatVersion) {

    /**
     * Defensive copies of byte arrays to ensure immutability.
     */
    public EncryptedData {
        ciphertext = ciphertext != null ? ciphertext.clone() : null;
        iv = iv != null ? iv.clone() : null;
        encryptedKey = encryptedKey != null ? encryptedKey.clone() : null;
    }

    @Override
    public byte[] ciphertext() {
        return ciphertext != null ? ciphertext.clone() : null;
    }

    @Override
    public byte[] iv() {
        return iv != null ? iv.clone() : null;
    }

    @Override
    public byte[] encryptedKey() {
        return encryptedKey != null ? encryptedKey.clone() : null;
    }
}
