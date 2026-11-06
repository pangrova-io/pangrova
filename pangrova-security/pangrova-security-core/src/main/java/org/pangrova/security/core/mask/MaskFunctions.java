package org.pangrova.security.core.mask;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Built-in masking functions for common PII patterns (email, phone, card, name, hash, range).
 */
public final class MaskFunctions {

    private static final String HMAC_SHA256 = "HmacSHA256";

    private MaskFunctions() {}

    /** Returns an email address with its local part masked. */
    public static String maskEmail(String email) {
        if (email == null || !email.contains("@")) {
            return email;
        }
        int atIdx = email.indexOf('@');
        return email.charAt(0) + "***" + email.substring(atIdx);
    }

    /** Returns a phone number with only its final four characters visible. */
    public static String maskPhone(String phone) {
        if (phone == null || phone.length() < 4) {
            return phone;
        }
        return "****" + phone.substring(phone.length() - 4);
    }

    /** Returns a card number with only its final four characters visible. */
    public static String maskCard(String card) {
        if (card == null || card.length() < 4) {
            return card;
        }
        return "****-****-****-" + card.substring(card.length() - 4);
    }

    /** Returns a name with only its first character visible. */
    public static String maskName(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        return name.charAt(0) + "*".repeat(name.length() - 1);
    }

    public static String maskNull(String input) {
        return null;
    }

    /**
     * Computes a deterministic, per-table-isolated hash of the input value using HMAC-SHA256.
     *
     * <p>The per-table salt ensures that the same column value produces different hashes
     * across different tables, preventing cross-table correlation attacks and rainbow table lookups.
     *
     * @param input the column value to mask; null returns null
     * @param perTableSalt the per-table salt used as the HMAC key; must not be null or empty
     * @return 64-character lowercase hex-encoded HMAC-SHA256 digest, or null if input is null
     * @throws IllegalArgumentException if perTableSalt is null or empty
     */
    public static String maskHash(String input, String perTableSalt) {
        if (input == null) {
            return null;
        }
        if (perTableSalt == null || perTableSalt.isEmpty()) {
            throw new IllegalArgumentException("perTableSalt must not be null or empty");
        }
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec keySpec = new SecretKeySpec(
                    perTableSalt.getBytes(StandardCharsets.UTF_8), HMAC_SHA256);
            mac.init(keySpec);
            byte[] hmacBytes = mac.doFinal(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hmacBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("HmacSHA256 not available", e);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("Invalid HMAC key", e);
        }
    }

    public static String maskRange(long value, long precision) {
        long lower = (value / precision) * precision;
        long upper = lower + precision;
        return lower + "-" + upper;
    }
}
