package org.pangrova.security.auth.oauth2;

import org.pangrova.security.api.authentication.TokenValidationException;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies JWT RSA signatures against JWKS-resolved public keys.
 */
final class JwtSignatureVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(JwtSignatureVerifier.class);

    private JwtSignatureVerifier() {}

    /**
     * Verifies the signature of a JWT token.
     *
     * @param token  compact JWS token
     * @param header decoded JWT header
     * @param requireSignature whether unsigned tokens are rejected
     * @param keyResolver resolver for JWKS public keys
     */
    static void verify(String token, JwtHeader header,
                       boolean requireSignature, JwksKeyResolver keyResolver) {
        String alg = header.alg();

        if ("none".equals(alg)) {
            rejectIfRequired(requireSignature, "Unsigned tokens not accepted");
            return;
        }

        String kid = header.kid();
        String javaAlg = mapJwtAlgorithm(alg);
        if (javaAlg == null) {
            throw new TokenValidationException("Unsupported JWT algorithm: " + alg);
        }

        PublicKey key = resolveKeyOrReject(kid, requireSignature, keyResolver);
        if (key == null) {
            return;
        }

        verifySignature(token, key, javaAlg);
    }

    private static void rejectIfRequired(boolean requireSignature, String message) {
        if (requireSignature) {
            throw new TokenValidationException(message);
        }
    }

    private static PublicKey resolveKeyOrReject(String kid, boolean requireSignature,
                                                JwksKeyResolver keyResolver) {
        PublicKey key = keyResolver.resolveSigningKey(kid);
        if (key == null) {
            if (requireSignature) {
                throw new TokenValidationException(
                        "No signing key available for verification");
            }
            LOG.warn("event=oauth2_signature_skip reason=no_key_available kid={}", kid);
        }
        return key;
    }

    private static void verifySignature(String token, PublicKey key, String javaAlg) {
        byte[][] tokenParts = extractSignedParts(token);
        executeSignatureCheck(tokenParts[0], tokenParts[1], key, javaAlg);
    }

    private static byte[][] extractSignedParts(String token) {
        String[] parts = token.split("\\.");
        if (parts.length < 3 || parts[2].isEmpty()) {
            throw new TokenValidationException("Token missing signature");
        }
        byte[] signedContent = (parts[0] + "." + parts[1]).getBytes(StandardCharsets.UTF_8);
        byte[] signatureBytes = Base64.getUrlDecoder().decode(parts[2]);
        return new byte[][] {signedContent, signatureBytes};
    }

    private static void executeSignatureCheck(byte[] signedContent, byte[] signatureBytes,
                                              PublicKey key, String javaAlg) {
        try {
            Signature sig = Signature.getInstance(javaAlg);
            sig.initVerify(key);
            sig.update(signedContent);
            if (!sig.verify(signatureBytes)) {
                throw new TokenValidationException(
                        "Token signature verification failed");
            }
        } catch (TokenValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new TokenValidationException(
                    "Token signature verification error");
        }
    }

    private static String mapJwtAlgorithm(String jwtAlg) {
        if (jwtAlg == null) {
            return null;
        }
        return switch (jwtAlg) {
            case "RS256" -> "SHA256withRSA";
            case "RS384" -> "SHA384withRSA";
            case "RS512" -> "SHA512withRSA";
            default -> null;
        };
    }

}
